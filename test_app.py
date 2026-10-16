import os
os.environ["SYNC_DB_NAME"] = "test_sync_data.db"

import asyncio
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path
from fastapi.testclient import TestClient

from app import database
from app.config import DB_PATH, RCLONE_CONF_PATH
from app.rclone_manager import RcloneManager
from app.sync_engine import sync_engine
from main import app

class TestDataSyncApp(unittest.TestCase):
    def setUp(self):
        database.init_db()
        sync_engine.stop()
        for t_id in list(sync_engine.runners.keys()):
            sync_engine.remove_runner(t_id)
        with database.get_db() as conn:
            conn.execute("DELETE FROM files")
            conn.execute("DELETE FROM system_logs")
            conn.execute("DELETE FROM sync_history")
            conn.execute("DELETE FROM sync_tasks")
        self.client = TestClient(app)

    def test_database_init_and_stats(self):
        """Verify DB initialization and initial stats."""
        stats = database.get_stats()
        self.assertIn("total_files", stats)
        self.assertIn("completed", stats)
        self.assertIn("transferring", stats)
        self.assertIn("error", stats)
        self.assertIn("pending", stats)

    def test_reboot_contingency_recovery(self):
        """
        Verify contingency when machine reboots:
        Files left in 'transferring' must be automatically detected and reverted to 'pending'.
        """
        # 1. Insert a mock file as 'transferring' simulating an interrupted transfer during power outage
        with database.get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM files WHERE file_path = 'test/interrupted_file.txt'")
            cursor.execute("""
                INSERT INTO files (file_path, file_name, file_size, status)
                VALUES ('test/interrupted_file.txt', 'interrupted_file.txt', 1024, 'transferring')
            """)

        # 2. Trigger crash recovery
        recovery_result = database.recover_from_crash()
        self.assertGreaterEqual(recovery_result["interrupted_files_recovered"], 1)

        # 3. Verify the file is now 'pending' with contingency message
        files = database.get_files_paginated(search="interrupted_file.txt")
        self.assertEqual(len(files["items"]), 1)
        interrupted_file = files["items"][0]
        self.assertEqual(interrupted_file["status"], "pending")
        self.assertIn("reinicio", interrupted_file["error_message"].lower())

    def test_mapping_files(self):
        """Verify mapping new files and updating existing ones."""
        mock_discovered = [
            {"Path": "docs/report1.pdf", "Name": "report1.pdf", "Size": 5000, "ModTime": "2026-10-15T10:00:00Z"},
            {"Path": "docs/report2.pdf", "Name": "report2.pdf", "Size": 8000, "ModTime": "2026-10-15T11:00:00Z"}
        ]
        new_c, upd_c = database.map_discovered_files(mock_discovered)
        self.assertGreaterEqual(new_c, 2)

        # Query files
        files = database.get_files_paginated(search="report1.pdf")
        self.assertEqual(len(files["items"]), 1)
        self.assertEqual(files["items"][0]["status"], "pending")
        self.assertEqual(files["items"][0]["file_size"], 5000)

        # Update file size on source
        mock_updated = [
            {"Path": "docs/report1.pdf", "Name": "report1.pdf", "Size": 9999, "ModTime": "2026-10-15T12:00:00Z"}
        ]
        # First mark as completed to test modification re-queue
        database.set_file_completed(files["items"][0]["id"])
        new_c2, upd_c2 = database.map_discovered_files(mock_updated)
        self.assertEqual(upd_c2, 1)

        updated_file = database.get_files_paginated(search="report1.pdf")["items"][0]
        self.assertEqual(updated_file["status"], "pending")
        self.assertEqual(updated_file["file_size"], 9999)

    def test_file_status_transitions_and_errors(self):
        """Verify transitions: pending -> transferring -> error (with error msg saved) -> retry."""
        mock_file = [{"Path": "docs/transition_test.pdf", "Name": "transition_test.pdf", "Size": 1234, "ModTime": "2026-10-15T12:00:00Z"}]
        database.map_discovered_files(mock_file)
        files = database.get_files_paginated(search="transition_test.pdf")
        self.assertTrue(len(files["items"]) > 0)
        f_id = files["items"][0]["id"]

        # Transferring
        database.set_file_transferring(f_id)
        f_check = database.get_files_paginated(search="transition_test.pdf")["items"][0]
        self.assertEqual(f_check["status"], "transferring")

        # Error with message
        test_err = "403 AccessDenied: Bucket policy rejects upload"
        database.set_file_error(f_id, test_err)
        f_check2 = database.get_files_paginated(search="transition_test.pdf")["items"][0]
        self.assertEqual(f_check2["status"], "error")
        self.assertEqual(f_check2["error_message"], test_err)

        # Retry
        database.retry_file(f_id)
        f_check3 = database.get_files_paginated(search="transition_test.pdf")["items"][0]
        self.assertEqual(f_check3["status"], "pending")
        self.assertIsNone(f_check3["error_message"])

    def test_rclone_manager_config_generation(self):
        """Verify dynamic generation of rclone.conf."""
        settings = {
            "source_host": "sftp.testserver.com",
            "source_user": "tester",
            "source_port": "2222",
            "source_pass": "plain_password",
            "source_path": "incoming",
            "dest_type": "s3",
            "s3_provider": "AWS",
            "s3_bucket": "my-backup-bucket",
            "s3_access_key": "TESTACCESSKEY",
            "s3_secret_key": "TESTSECRETKEY",
            "s3_region": "us-west-2",
            "s3_path": "transferred"
        }
        conf_path = RcloneManager.generate_config_file(settings)
        self.assertTrue(conf_path.exists())
        content = conf_path.read_text()
        self.assertIn("[sftp_source]", content)
        self.assertIn("host = sftp.testserver.com", content)
        self.assertIn("user = tester", content)
        self.assertIn("[dest_remote]", content)
        self.assertIn("access_key_id = TESTACCESSKEY", content)

        # Verify target destinations
        dst_target = RcloneManager.get_destination_target(settings, "folder/file.csv")
        self.assertEqual(dst_target, "dest_remote:my-backup-bucket/transferred/folder/file.csv")

    def test_sync_engine_controls(self):
        """Verify start, pause, resume, stop controls."""
        sync_engine.start()
        self.assertEqual(sync_engine.state, "running")

        sync_engine.pause()
        self.assertEqual(sync_engine.state, "paused")

        sync_engine.resume()
        self.assertEqual(sync_engine.state, "running")

        sync_engine.stop()
        self.assertEqual(sync_engine.state, "stopped")

    def test_api_endpoints(self):
        """Verify API endpoints response codes and payloads."""
        # 1. UI Root
        res = self.client.get("/")
        self.assertEqual(res.status_code, 200)
        self.assertIn("DataSync Manager", res.text)

        # 2. Status
        res = self.client.get("/api/status")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIn("total_files", data)

        # 3. Control API
        res = self.client.post("/api/control/start")
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["current_state"], "running")

        res = self.client.post("/api/control/pause")
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["current_state"], "paused")

        res = self.client.post("/api/control/resume")
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["current_state"], "running")

        res = self.client.post("/api/control/stop")
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["current_state"], "stopped")

        # 4. Config API
        res = self.client.get("/api/config")
        self.assertEqual(res.status_code, 200)
        self.assertIn("source_host", res.json())

        # 5. Files API
        res = self.client.get("/api/files?page=1&per_page=10")
        self.assertEqual(res.status_code, 200)
        self.assertIn("items", res.json())

        # 6. Service Status API
        res = self.client.get("/api/service/status")
        self.assertEqual(res.status_code, 200)
        self.assertIn("os", res.json())

    def test_multi_task_crud_and_controls(self):
        """Verify multi-task creation, listing, controls, updating, and deletion."""
        # 1. Create Strain Task
        strain_payload = {
            "alias": "STRAIN",
            "name": "Sincronización NAS - Strain",
            "source_type": "sftp",
            "source_host": "192.168.1.50",
            "source_port": "22",
            "source_user": "nasuser",
            "source_pass": "secret123",
            "source_path": "datos/strain",
            "dest_type": "s3",
            "s3_provider": "AWS",
            "s3_bucket": "das-data-bucket",
            "s3_access_key": "AKIAEXAMPLE",
            "s3_secret_key": "SECEXAMPLE",
            "s3_region": "us-east-1",
            "s3_path": "respaldos/strain",
            "sync_interval": 30,
            "auto_start": 0,
            "recursive_scan": 1
        }
        res = self.client.post("/api/tasks", json=strain_payload)
        self.assertEqual(res.status_code, 200)
        strain_id = res.json()["task_id"]
        self.assertIsNotNone(strain_id)

        # 2. Duplicate alias check
        res_dup = self.client.post("/api/tasks", json=strain_payload)
        self.assertEqual(res_dup.status_code, 400)
        self.assertIn("Ya existe", res_dup.json()["detail"])

        # 3. Create FBE Task
        fbe_payload = {
            "alias": "FBE",
            "name": "Sincronización NAS - FBE",
            "source_type": "sftp",
            "source_host": "192.168.1.50",
            "source_port": "22",
            "source_user": "nasuser",
            "source_pass": "secret123",
            "source_path": "datos/fbe",
            "dest_type": "ftp",
            "ftp_host": "192.168.1.60",
            "ftp_port": "21",
            "ftp_user": "ftpuser",
            "ftp_pass": "ftppass",
            "ftp_path": "/respaldos/fbe",
            "sync_interval": 45,
            "auto_start": 0,
            "recursive_scan": 1
        }
        res2 = self.client.post("/api/tasks", json=fbe_payload)
        self.assertEqual(res2.status_code, 200)
        fbe_id = res2.json()["task_id"]

        # 4. List Tasks
        res_list = self.client.get("/api/tasks")
        self.assertEqual(res_list.status_code, 200)
        tasks = res_list.json()["tasks"]
        aliases = [t["alias"] for t in tasks]
        self.assertIn("STRAIN", aliases)
        self.assertIn("FBE", aliases)

        # 5. Task Individual Runtime Controls
        with mock.patch.object(RcloneManager, "list_source_files", return_value=(True, [], "")):
            res_start = self.client.post(f"/api/tasks/{strain_id}/control/start")
            self.assertEqual(res_start.status_code, 200)
            self.assertEqual(res_start.json()["state"], "running")

            res_pause = self.client.post(f"/api/tasks/{strain_id}/control/pause")
            self.assertEqual(res_pause.status_code, 200)
            self.assertEqual(res_pause.json()["state"], "paused")

            res_resume = self.client.post(f"/api/tasks/{strain_id}/control/resume")
            self.assertEqual(res_resume.status_code, 200)
            self.assertEqual(res_resume.json()["state"], "running")

            res_stop = self.client.post(f"/api/tasks/{strain_id}/control/stop")
            self.assertEqual(res_stop.status_code, 200)
            self.assertEqual(res_stop.json()["state"], "stopped")

        # 6. Update Task
        strain_payload["name"] = "Sincronización NAS - Strain Actualizada"
        res_upd = self.client.put(f"/api/tasks/{strain_id}", json=strain_payload)
        self.assertEqual(res_upd.status_code, 200)

        # Verify update
        res_get = self.client.get(f"/api/tasks/{strain_id}")
        self.assertEqual(res_get.status_code, 200)
        self.assertEqual(res_get.json()["name"], "Sincronización NAS - Strain Actualizada")

        # 7. Delete Task
        res_del = self.client.delete(f"/api/tasks/{fbe_id}")
        self.assertEqual(res_del.status_code, 200)

    def test_task_scoped_file_mapping_and_alias_filter(self):
        """Verify files mapped under distinct tasks carry their alias and can be filtered."""
        # 1. Map files for STRAIN
        strain_files = [
            {"Path": "strain/sensor_01.dat", "Name": "sensor_01.dat", "Size": 2048, "ModTime": "2026-10-15T12:00:00Z"},
            {"Path": "strain/sensor_02.dat", "Name": "sensor_02.dat", "Size": 4096, "ModTime": "2026-10-15T12:00:00Z"}
        ]
        database.map_task_files(task_id=101, task_alias="STRAIN", discovered_files=strain_files)

        # 2. Map files for FBE
        fbe_files = [
            {"Path": "fbe/readings_01.csv", "Name": "readings_01.csv", "Size": 1024, "ModTime": "2026-10-15T12:00:00Z"}
        ]
        database.map_task_files(task_id=102, task_alias="FBE", discovered_files=fbe_files)

        # 3. Check Aliases API
        res_aliases = self.client.get("/api/task-aliases")
        self.assertEqual(res_aliases.status_code, 200)
        aliases = [a["alias"] if isinstance(a, dict) else a for a in res_aliases.json()["aliases"]]
        self.assertIn("STRAIN", aliases)
        self.assertIn("FBE", aliases)

        # 4. Filter by STRAIN
        res_strain = self.client.get("/api/files?task_alias=STRAIN")
        self.assertEqual(res_strain.status_code, 200)
        items_strain = res_strain.json()["items"]
        self.assertEqual(len(items_strain), 2)
        for it in items_strain:
            self.assertEqual(it["task_alias"], "STRAIN")

        # 5. Filter by FBE
        res_fbe = self.client.get("/api/files?task_alias=FBE")
        self.assertEqual(res_fbe.status_code, 200)
        items_fbe = res_fbe.json()["items"]
        self.assertEqual(len(items_fbe), 1)
        self.assertEqual(items_fbe[0]["task_alias"], "FBE")

    def test_selective_file_transfer(self):
        """Verify selecting specific files by ID triggers immediate transfer."""
        import unittest.mock
        sample_files = [
            {"Path": "batch/f1.bin", "Name": "f1.bin", "Size": 100, "ModTime": "2026-10-15T12:00:00Z"},
            {"Path": "batch/f2.bin", "Name": "f2.bin", "Size": 200, "ModTime": "2026-10-15T12:00:00Z"},
            {"Path": "batch/f3.bin", "Name": "f3.bin", "Size": 300, "ModTime": "2026-10-15T12:00:00Z"},
        ]
        database.map_task_files(task_id=1, task_alias="TEST", files_data=sample_files)
        all_files = database.get_files_paginated(task_alias="TEST")["items"]
        self.assertEqual(len(all_files), 3)

        with unittest.mock.patch.object(RcloneManager, "execute_file_transfer", return_value=(True, "")):
            selected_ids = [all_files[0]["id"], all_files[1]["id"]]
            res = self.client.post("/api/files/transfer-selected", json={"file_ids": selected_ids})
            self.assertEqual(res.status_code, 200)
            data = res.json()
            self.assertTrue(data["success"])
            self.assertEqual(data["transferred"], 2)

    def test_scan_preview_endpoint(self):
        """Verify /api/tasks/scan-preview endpoint returns proper structure."""
        import unittest.mock
        payload = {
            "settings": {
                "source_host": "mock.sftp.host",
                "source_port": "22",
                "source_user": "test",
                "source_pass": "pass",
                "source_path": "datos"
            }
        }
        mock_files = [{"Path": "datos/f1.txt", "Name": "f1.txt", "Size": 500, "ModTime": "2026-10-15T12:00:00Z"}]
        with unittest.mock.patch.object(RcloneManager, "scan_source_files", return_value=(True, mock_files, "")):
            res = self.client.post("/api/tasks/scan-preview", json=payload)
            self.assertEqual(res.status_code, 200)
            data = res.json()
            self.assertTrue(data["success"])
            self.assertEqual(data["count"], 1)
            self.assertEqual(data["total_size"], 500)

    def test_scan_all_tasks_only_endpoint(self):
        """Verify /api/tasks/scan-all discovers and maps files WITHOUT starting tasks."""
        import unittest.mock
        t_id = database.create_task({
            "alias": "SCANONLY",
            "name": "Scan Only Task",
            "source_type": "sftp",
            "source_host": "127.0.0.1",
            "source_port": 22,
            "source_path": "/data",
            "dest_type": "s3",
            "s3_bucket": "test-bucket"
        })

        mock_files = [
            {"Path": "sample1.dat", "Name": "sample1.dat", "Size": 1024, "ModTime": "2026-10-15T12:00:00Z"},
            {"Path": "sample2.dat", "Name": "sample2.dat", "Size": 2048, "ModTime": "2026-10-15T12:00:00Z"}
        ]

        with unittest.mock.patch.object(RcloneManager, "list_source_files", return_value=(True, mock_files, "")):
            res = self.client.post("/api/tasks/scan-all")
            self.assertEqual(res.status_code, 200)
            data = res.json()
            self.assertTrue(data["success"])
            self.assertEqual(data["tasks_scanned"], 1)
            self.assertEqual(data["total_discovered"], 2)

            # Files mapped
            files = database.get_files_paginated(task_alias="SCANONLY")["items"]
            self.assertEqual(len(files), 2)

            # Task state must remain stopped
            task = database.get_task_by_id(t_id)
            self.assertEqual(task["state"], "stopped")
            self.assertEqual(sync_engine.state, "stopped")

    def test_scan_all_and_start_endpoint(self):
        """Verify /api/tasks/scan-and-start discovers files and starts all tasks."""
        import unittest.mock
        # 1. Create a task
        t_id = database.create_task({
            "alias": "SCANTEST",
            "name": "Scan Test Task",
            "source_type": "sftp",
            "source_host": "127.0.0.1",
            "source_port": 22,
            "source_path": "/data",
            "dest_type": "s3",
            "s3_bucket": "test-bucket"
        })

        mock_files = [
            {"Path": "file_alpha.dat", "Name": "file_alpha.dat", "Size": 1024, "ModTime": "2026-10-15T12:00:00Z"},
            {"Path": "file_beta.dat", "Name": "file_beta.dat", "Size": 2048, "ModTime": "2026-10-15T12:00:00Z"}
        ]

        with unittest.mock.patch.object(RcloneManager, "list_source_files", return_value=(True, mock_files, "")):
            res = self.client.post("/api/tasks/scan-and-start")
            self.assertEqual(res.status_code, 200)
            data = res.json()
            self.assertTrue(data["success"])
            self.assertEqual(data["tasks_started"], 1)
            self.assertEqual(data["total_discovered"], 2)

            # Check that files were stored with alias SCANTEST
            files = database.get_files_paginated(task_alias="SCANTEST")["items"]
            self.assertEqual(len(files), 2)

    def test_network_ping_endpoint(self):
        """Verify /api/network/ping tests connectivity to source, destination, or both."""
        import unittest.mock
        payload = {
            "settings": {
                "source_host": "nas.local",
                "source_port": 22,
                "dest_type": "s3",
                "s3_endpoint": "https://s3.amazonaws.com",
                "s3_provider": "AWS"
            }
        }
        mock_ping_res = {
            "host": "nas.local",
            "port": 22,
            "resolved_ip": "192.168.1.100",
            "reachable": True,
            "latency_ms": 14.5,
            "error": None,
            "details": "Puerto 22 abierto."
        }
        with unittest.mock.patch.object(RcloneManager, "ping_host", return_value=mock_ping_res):
            # 1. Both endpoints (default)
            res_all = self.client.post("/api/network/ping", json=payload)
            self.assertEqual(res_all.status_code, 200)
            data_all = res_all.json()
            self.assertTrue(data_all["success"])
            self.assertIn("source", data_all)
            self.assertIn("destination", data_all)

            # 2. Source only
            res_src = self.client.post("/api/network/ping?target=source", json=payload)
            self.assertEqual(res_src.status_code, 200)
            data_src = res_src.json()
            self.assertTrue(data_src["success"])
            self.assertIn("source", data_src)
            self.assertNotIn("destination", data_src)
            self.assertEqual(data_src["source"]["latency_ms"], 14.5)

            # 3. Destination only
            res_dst = self.client.post("/api/network/ping?target=destination", json=payload)
            self.assertEqual(res_dst.status_code, 200)
            data_dst = res_dst.json()
            self.assertTrue(data_dst["success"])
            self.assertIn("destination", data_dst)
            self.assertNotIn("source", data_dst)

    def test_task_ping_endpoint(self):
        """Verify /api/tasks/{task_id}/ping tests connectivity for a specific task."""
        import unittest.mock
        t_id = database.create_task({
            "alias": "PINGTASK",
            "name": "Ping Test Task",
            "source_type": "sftp",
            "source_host": "192.168.1.50",
            "source_port": 22,
            "source_path": "/sensor_data",
            "dest_type": "s3",
            "s3_bucket": "sensor-archive"
        })

        mock_ping_res = {
            "host": "192.168.1.50",
            "port": 22,
            "resolved_ip": "192.168.1.50",
            "reachable": True,
            "latency_ms": 8.2,
            "error": None,
            "details": "Puerto 22 abierto."
        }
        with unittest.mock.patch.object(RcloneManager, "ping_host", return_value=mock_ping_res):
            # Both
            res = self.client.post(f"/api/tasks/{t_id}/ping")
            self.assertEqual(res.status_code, 200)
            data = res.json()
            self.assertTrue(data["success"])
            self.assertEqual(data["task_alias"], "PINGTASK")
            self.assertEqual(data["task_name"], "Ping Test Task")
            self.assertTrue(data["source"]["reachable"])
            self.assertIn("destination", data)

            # Source only
            res_src = self.client.post(f"/api/tasks/{t_id}/ping?target=source")
            self.assertEqual(res_src.status_code, 200)
            data_src = res_src.json()
            self.assertIn("source", data_src)
            self.assertNotIn("destination", data_src)

            # Destination only
            res_dst = self.client.post(f"/api/tasks/{t_id}/ping?target=destination")
            self.assertEqual(res_dst.status_code, 200)
            data_dst = res_dst.json()
            self.assertIn("destination", data_dst)
            self.assertNotIn("source", data_dst)

        # Nonexistent task returns 404
        res_404 = self.client.post("/api/tasks/99999/ping")
        self.assertEqual(res_404.status_code, 404)

    def test_websocket_connection_and_initial_state(self):
        """Verify WebSocket client connection and initial_state payload."""
        with self.client.websocket_connect("/ws") as websocket:
            data = websocket.receive_json()
            self.assertEqual(data["type"], "initial_state")
            self.assertIn("stats", data["data"])
            self.assertIn("server_time", data["data"])
            self.assertIn("total_files", data["data"]["stats"])

    def test_websocket_ping_pong(self):
        """Verify WebSocket responds to client ping action with pong."""
        with self.client.websocket_connect("/ws") as websocket:
            # First frame is always initial_state
            init_msg = websocket.receive_json()
            self.assertEqual(init_msg["type"], "initial_state")

            # Send ping
            websocket.send_json({"action": "ping"})
            resp = websocket.receive_json()
            self.assertEqual(resp["type"], "pong")
            self.assertIn("timestamp", resp["data"])

    def test_websocket_broadcast_events(self):
        """Verify server broadcasts push events in real time to connected WebSocket clients."""
        from app.websocket_manager import ws_manager
        with self.client.websocket_connect("/ws") as websocket:
            init_msg = websocket.receive_json()
            self.assertEqual(init_msg["type"], "initial_state")

            # Trigger a broadcast synchronously
            ws_manager.broadcast_sync("file_completed", {"file_id": 42, "file_name": "strain_test.dat"})
            event = websocket.receive_json()
            self.assertEqual(event["type"], "file_completed")
            self.assertEqual(event["data"]["file_id"], 42)
            self.assertEqual(event["data"]["file_name"], "strain_test.dat")

    def test_multi_sync_engine_active_file_property(self):
        """Verify MultiSyncEngine has active_file attribute and broadcast_status executes without error."""
        self.assertIsNone(sync_engine.active_file)
        self.assertIsNone(sync_engine.current_file_path)
        # Should not raise AttributeError: 'MultiSyncEngine' object has no attribute 'active_file'
        sync_engine.broadcast_status()

    def test_map_task_files_min_age_and_no_overwrite(self):
        """
        Verify:
        1. Files created/modified within last 2 minutes (< 120s) are ignored so incomplete interrogator files are not read.
        2. Files older than 2 minutes are registered as pending.
        3. Existing files in database are never overwritten or reset.
        """
        from datetime import datetime, timezone, timedelta

        now_utc = datetime.now(timezone.utc)
        recent_mod_time = (now_utc - timedelta(seconds=30)).isoformat()    # 30 seconds old (< 2m)
        mature_mod_time = (now_utc - timedelta(seconds=180)).isoformat()   # 3 minutes old (> 2m)

        mock_files = [
            {"Path": "strain/recent.dat", "Name": "recent.dat", "Size": 1024, "ModTime": recent_mod_time},
            {"Path": "strain/mature.dat", "Name": "mature.dat", "Size": 2048, "ModTime": mature_mod_time}
        ]

        # 1. Map with min_age_seconds=120 and allow_overwrite=False
        new_c, upd_c = database.map_task_files(1, "STRAIN", mock_files, min_age_seconds=120, allow_overwrite=False)
        self.assertEqual(new_c, 1)  # Only mature.dat should be registered
        self.assertEqual(upd_c, 0)

        mature_file = database.get_files_paginated(search="mature.dat")["items"]
        self.assertEqual(len(mature_file), 1)
        self.assertEqual(mature_file[0]["file_name"], "mature.dat")
        self.assertEqual(mature_file[0]["status"], "pending")

        recent_file = database.get_files_paginated(search="recent.dat")["items"]
        self.assertEqual(len(recent_file), 0)  # Should NOT be registered yet!

        # 2. Mark mature.dat as completed
        database.set_file_completed(mature_file[0]["id"])
        check_completed = database.get_files_paginated(search="mature.dat")["items"][0]
        self.assertEqual(check_completed["status"], "completed")

        # 3. Simulate another scan from the interrogator NAS where mature.dat still exists
        new_c2, upd_c2 = database.map_task_files(1, "STRAIN", mock_files, min_age_seconds=120, allow_overwrite=False)
        self.assertEqual(new_c2, 0)
        self.assertEqual(upd_c2, 0)

        # mature.dat must remain 'completed' (not overwritten)
        recheck_file = database.get_files_paginated(search="mature.dat")["items"][0]
        self.assertEqual(recheck_file["status"], "completed")

    def test_task_min_file_age_api(self):
        """Verify min_file_age is stored and returned in Task API."""
        payload = {
            "name": "Strain Interrogator Task",
            "alias": "STRAIN_NAS",
            "source_type": "sftp",
            "source_host": "192.168.1.100",
            "source_user": "nas_user",
            "source_path": "/nas/strain_data",
            "dest_type": "s3",
            "s3_bucket": "archive-bucket",
            "sync_interval": 30,
            "min_file_age": 2
        }
        res = self.client.post("/api/tasks", json=payload)
        self.assertEqual(res.status_code, 200)
        t_id = res.json()["task_id"]

        get_res = self.client.get(f"/api/tasks/{t_id}")
        self.assertEqual(get_res.status_code, 200)
        t_data = get_res.json()
        self.assertEqual(t_data["min_file_age"], 2)
        self.assertEqual(t_data["sync_interval"], 30)

    def test_file_live_progress_tracking(self):
        """Verify files table has progress_pct and API merges active live transfer progress."""
        mock_file = [{"Path": "strain/test_progress.dat", "Name": "test_progress.dat", "Size": 5000, "ModTime": "2026-10-15T12:00:00Z"}]
        database.map_task_files(1, "STRAIN", mock_file, min_age_seconds=0, allow_overwrite=True)
        
        file_item = database.get_files_paginated(search="test_progress.dat")["items"][0]
        file_id = file_item["id"]
        self.assertEqual(file_item["progress_pct"], 0.0)

        # 1. When transferring, progress starts at 0.0
        database.set_file_transferring(file_id)
        f_trans = database.get_files_paginated(search="test_progress.dat")["items"][0]
        self.assertEqual(f_trans["status"], "transferring")
        self.assertEqual(f_trans["progress_pct"], 0.0)

        # 2. Simulate active live progress in sync_engine
        sync_engine.active_progress[file_id] = {
            "percentage": 57.5,
            "bytes": 2875,
            "total_bytes": 5000,
            "speed": 1024000.0,
            "eta": 2
        }

        api_res = self.client.get(f"/api/files?search=test_progress.dat")
        self.assertEqual(api_res.status_code, 200)
        api_data = api_res.json()["items"][0]
        self.assertEqual(api_data["progress_pct"], 57.5)
        self.assertEqual(api_data["live_speed"], 1024000.0)
        self.assertEqual(api_data["live_eta"], 2)

        # 3. Clean up active progress and mark completed -> 100%
        sync_engine.active_progress.pop(file_id, None)
        database.set_file_completed(file_id)
        f_comp = database.get_files_paginated(search="test_progress.dat")["items"][0]
        self.assertEqual(f_comp["status"], "completed")
        self.assertEqual(f_comp["progress_pct"], 100.0)

    def test_rclone_s3_chunk_and_timeout_parameters(self):
        """Verify RcloneManager config and execution cmd contain 5M chunks, 10m timeout, 5 retries, etc."""
        settings = {
            "source_host": "127.0.0.1",
            "source_user": "tester",
            "dest_type": "s3",
            "s3_provider": "AWS",
            "s3_access_key": "mykey",
            "s3_secret_key": "mysecret",
            "s3_bucket": "test-bucket"
        }
        with tempfile.NamedTemporaryFile("w+", suffix=".conf", delete=False) as tmp:
            tmp_path = Path(tmp.name)
        try:
            RcloneManager.generate_config_file(settings, target_path=tmp_path)
            content = tmp_path.read_text(encoding="utf-8")
            self.assertIn("chunk_size = 5M", content)
            self.assertIn("upload_concurrency = 2", content)
            self.assertIn("force_path_style = true", content)
            self.assertIn("no_check_bucket = true", content)
        finally:
            if tmp_path.exists():
                tmp_path.unlink()

        # Check command construction in execute_file_transfer
        with unittest.mock.patch("subprocess.Popen") as mock_popen:
            mock_proc = unittest.mock.MagicMock()
            mock_proc.stderr = None
            mock_proc.returncode = 0
            mock_popen.return_value = mock_proc

            ok, _ = RcloneManager.execute_file_transfer(settings, "test.dat")
            self.assertTrue(ok)
            called_cmd = mock_popen.call_args[0][0]
            self.assertIn("--s3-chunk-size", called_cmd)
            self.assertIn("5M", called_cmd)
            self.assertIn("--s3-upload-concurrency", called_cmd)
            self.assertIn("2", called_cmd)
            self.assertIn("--timeout", called_cmd)
            self.assertIn("10m", called_cmd)
            self.assertIn("--retries", called_cmd)
            self.assertIn("5", called_cmd)
            self.assertIn("--no-check-certificate", called_cmd)
            self.assertIn("--s3-force-path-style", called_cmd)
            self.assertIn("--s3-no-check-bucket", called_cmd)

    def test_rclone_ftp_config_path_and_transfer_execution(self):
        """Verify RcloneManager handles FTP destinations natively with proper config, paths, and flags."""
        ftp_settings = {
            "source_host": "192.168.1.10",
            "source_user": "nasuser",
            "source_path": "/var/data/fbe",
            "dest_type": "ftp",
            "ftp_host": "ftp.example.com",
            "ftp_port": "21",
            "ftp_user": "ftpuser",
            "ftp_pass": "ftppassword123",
            "ftp_path": "/remotedir/fbe"
        }

        # 1. Test rclone.conf generation for FTP
        with tempfile.NamedTemporaryFile("w+", suffix=".conf", delete=False) as tmp:
            tmp_path = Path(tmp.name)
        try:
            RcloneManager.generate_config_file(ftp_settings, target_path=tmp_path)
            content = tmp_path.read_text(encoding="utf-8")
            self.assertIn("[dest_remote]", content)
            self.assertIn("type = ftp", content)
            self.assertIn("host = ftp.example.com", content)
            self.assertIn("user = ftpuser", content)
            self.assertIn("port = 21", content)
            self.assertIn("concurrency = 0", content)
            self.assertIn("idle_timeout = 1m", content)
            self.assertIn("tls = false", content)
            self.assertIn("pass = ", content)  # Obscured password
        finally:
            if tmp_path.exists():
                tmp_path.unlink()

        # 2. Test destination target path resolution (absolute vs relative)
        abs_target = RcloneManager.get_destination_target(ftp_settings, "readings/fbe_01.dat")
        self.assertEqual(abs_target, "dest_remote:/remotedir/fbe/readings/fbe_01.dat")

        rel_settings = dict(ftp_settings)
        rel_settings["ftp_path"] = "uploads/fbe"
        rel_target = RcloneManager.get_destination_target(rel_settings, "readings/fbe_01.dat")
        self.assertEqual(rel_target, "dest_remote:uploads/fbe/readings/fbe_01.dat")

        empty_settings = dict(ftp_settings)
        empty_settings["ftp_path"] = ""
        empty_target = RcloneManager.get_destination_target(empty_settings, "file.bin")
        self.assertEqual(empty_target, "dest_remote:file.bin")

        # 3. Test FTP transfer execution command
        with unittest.mock.patch("subprocess.Popen") as mock_popen:
            mock_proc = unittest.mock.MagicMock()
            mock_proc.stderr = None
            mock_proc.returncode = 0
            mock_popen.return_value = mock_proc

            ok, _ = RcloneManager.execute_file_transfer(ftp_settings, "readings/fbe_01.dat")
            self.assertTrue(ok)
            called_cmd = mock_popen.call_args[0][0]
            self.assertIn("copyto", called_cmd)
            self.assertIn("--ftp-no-check-certificate", called_cmd)
            self.assertIn("--retries", called_cmd)
            self.assertIn("5", called_cmd)
            self.assertIn("--timeout", called_cmd)
            self.assertIn("10m", called_cmd)
            self.assertIn("--stats", called_cmd)
            self.assertIn("500ms", called_cmd)
            self.assertIn("--use-json-log", called_cmd)
            self.assertIn("sftp_source:/var/data/fbe/readings/fbe_01.dat", called_cmd)
            self.assertIn("dest_remote:/remotedir/fbe/readings/fbe_01.dat", called_cmd)

    def test_date_range_helper_parsing(self):
        """Test parse_filter_datetime for various date and datetime formats."""
        # 1. Date-only start: YYYY-MM-DD
        dt1 = database.parse_filter_datetime("2026-09-02", is_end_of_day=False)
        self.assertIsNotNone(dt1)
        self.assertEqual(dt1.year, 2026)
        self.assertEqual(dt1.month, 9)
        self.assertEqual(dt1.day, 2)
        self.assertEqual(dt1.hour, 0)
        self.assertEqual(dt1.minute, 0)

        # 2. Date-only end: YYYY-MM-DD
        dt2 = database.parse_filter_datetime("2026-09-02", is_end_of_day=True)
        self.assertIsNotNone(dt2)
        self.assertEqual(dt2.hour, 23)
        self.assertEqual(dt2.minute, 59)
        self.assertEqual(dt2.second, 59)

        # 3. Datetime-local: YYYY-MM-DDTHH:mm
        dt3 = database.parse_filter_datetime("2026-09-02T14:35")
        self.assertIsNotNone(dt3)
        self.assertEqual(dt3.hour, 14)
        self.assertEqual(dt3.minute, 35)

        # 4. Empty / invalid
        self.assertIsNone(database.parse_filter_datetime(""))
        self.assertIsNone(database.parse_filter_datetime(None))
        self.assertIsNone(database.parse_filter_datetime("invalid-string"))

    def test_map_task_files_with_date_range_filtering(self):
        """
        Verify that map_task_files respects date_from and date_to filters:
        - When only date_from is specified (e.g. 2026-09-02), files before that date are excluded.
        - When both are specified, only files in range are included.
        """
        task_id = database.create_task({
            "name": "Date Filter Task",
            "alias": "DATE_TEST",
            "source_host": "nas.local",
            "source_user": "nasuser",
            "source_path": "/data/test"
        })

        test_files = [
            {"Path": "file_aug.dat", "Name": "file_aug.dat", "Size": 100, "ModTime": "2026-08-31T23:59:00Z"},
            {"Path": "file_sep01.dat", "Name": "file_sep01.dat", "Size": 200, "ModTime": "2026-09-01T12:00:00Z"},
            {"Path": "file_sep02.dat", "Name": "file_sep02.dat", "Size": 300, "ModTime": "2026-09-02T08:30:00Z"},
            {"Path": "file_sep05.dat", "Name": "file_sep05.dat", "Size": 400, "ModTime": "2026-09-05T14:00:00Z"},
            {"Path": "file_sep10.dat", "Name": "file_sep10.dat", "Size": 500, "ModTime": "2026-09-10T18:00:00Z"},
        ]

        # Case 1: date_from="2026-09-02" and date_to="" -> only file_sep02, file_sep05, file_sep10
        new_c, _ = database.map_task_files(
            task_id, "DATE_TEST", test_files, min_age_seconds=0,
            date_from="2026-09-02", date_to=""
        )
        self.assertEqual(new_c, 3)
        files = database.get_pending_task_files(task_id, limit=100)
        file_names = [f["file_name"] for f in files]
        self.assertIn("file_sep02.dat", file_names)
        self.assertIn("file_sep05.dat", file_names)
        self.assertIn("file_sep10.dat", file_names)
        self.assertNotIn("file_aug.dat", file_names)
        self.assertNotIn("file_sep01.dat", file_names)

        # Clear files for next test
        with database.get_db() as conn:
            conn.execute("DELETE FROM files WHERE task_id = ?", (task_id,))

        # Case 2: date_from="2026-09-02T00:00" and date_to="2026-09-05T23:59" -> file_sep02, file_sep05
        new_c, _ = database.map_task_files(
            task_id, "DATE_TEST", test_files, min_age_seconds=0,
            date_from="2026-09-02T00:00", date_to="2026-09-05T23:59"
        )
        self.assertEqual(new_c, 2)
        files = database.get_pending_task_files(task_id, limit=100)
        file_names = [f["file_name"] for f in files]
        self.assertEqual(sorted(file_names), ["file_sep02.dat", "file_sep05.dat"])

    def test_task_crud_with_date_range_and_pruning(self):
        """Test creating and updating tasks with date_from / date_to, and pending file pruning."""
        res = self.client.post("/api/tasks", json={
            "name": "Strain Range Task",
            "alias": "STRAIN_RANGE",
            "source_host": "nas.internal",
            "source_user": "admin",
            "source_path": "/var/data/strain",
            "dest_type": "s3",
            "s3_bucket": "das-strain-bucket",
            "date_from": "2026-09-02",
            "date_to": "2026-09-10",
            "auto_start": 0
        })
        self.assertEqual(res.status_code, 200)
        task_id = res.json()["task_id"]

        # Verify task stores date_from and date_to
        task_res = self.client.get(f"/api/tasks/{task_id}")
        self.assertEqual(task_res.status_code, 200)
        t_data = task_res.json()
        self.assertEqual(t_data["date_from"], "2026-09-02")
        self.assertEqual(t_data["date_to"], "2026-09-10")

        # Insert mock pending files for this task
        with database.get_db() as conn:
            conn.execute("""
                INSERT INTO files (task_id, task_alias, file_path, file_name, file_size, mod_time, status)
                VALUES (?, 'STRAIN_RANGE', 'f1.dat', 'f1.dat', 100, '2026-09-03T10:00:00Z', 'pending'),
                       (?, 'STRAIN_RANGE', 'f2.dat', 'f2.dat', 200, '2026-09-08T10:00:00Z', 'pending'),
                       (?, 'STRAIN_RANGE', 'f3.dat', 'f3.dat', 300, '2026-09-15T10:00:00Z', 'pending')
            """, (task_id, task_id, task_id))

        # Update task to narrow date_to to 2026-09-05 -> f2 and f3 should be pruned
        upd_res = self.client.put(f"/api/tasks/{task_id}", json={
            "name": "Strain Range Task Updated",
            "alias": "STRAIN_RANGE",
            "source_host": "nas.internal",
            "source_user": "admin",
            "source_path": "/var/data/strain",
            "dest_type": "s3",
            "s3_bucket": "das-strain-bucket",
            "date_from": "2026-09-02",
            "date_to": "2026-09-05",
            "auto_start": 0
        })
        self.assertEqual(upd_res.status_code, 200)

        pending = database.get_pending_task_files(task_id)
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["file_name"], "f1.dat")

    def test_scan_preview_with_date_range(self):
        """Test /api/tasks/scan-preview applies date_from filtering."""
        mock_scanned = [
            {"Path": "old.dat", "Name": "old.dat", "Size": 1000, "ModTime": "2026-08-15T10:00:00Z"},
            {"Path": "new.dat", "Name": "new.dat", "Size": 2000, "ModTime": "2026-09-03T12:00:00Z"}
        ]
        with unittest.mock.patch("app.rclone_manager.RcloneManager.scan_source_files", return_value=(True, mock_scanned, "")):
            res = self.client.post("/api/tasks/scan-preview", json={
                "settings": {
                    "source_host": "nas.internal",
                    "source_user": "admin",
                    "date_from": "2026-09-01"
                }
            })
            self.assertEqual(res.status_code, 200)
            data = res.json()
            self.assertTrue(data["success"])
            self.assertEqual(data["count"], 1)
            self.assertEqual(data["files"][0]["Name"], "new.dat")
            self.assertIn("Filtro: desde 2026-09-01", data["message"])

    def test_task_pause_cancels_active_rclone_process_and_resets_file(self):
        """Verify that pausing an active task immediately terminates the running rclone process and re-queues file."""
        import unittest.mock
        task_id = database.create_task({
            "alias": "PAUSETST",
            "name": "Test Pause Task",
            "source_type": "sftp",
            "source_host": "nas.local",
            "source_user": "user",
            "dest_type": "s3",
            "s3_bucket": "my-bucket",
            "auto_start": 0
        })
        files = [{"Path": "test_pause.dat", "Name": "test_pause.dat", "Size": 600000000, "ModTime": "2026-10-15T12:00:00Z"}]
        database.map_task_files(task_id, "PAUSETST", files, min_age_seconds=0)
        pending = database.get_pending_task_files(task_id)
        self.assertEqual(len(pending), 1)
        file_id = pending[0]["id"]

        # Simulate file transitioning to transferring
        database.set_file_transferring(file_id)
        f_before = database.get_files_by_ids([file_id])[0]
        self.assertEqual(f_before["status"], "transferring")

        runner = sync_engine.get_or_create_runner(task_id)
        runner.state = "running"
        runner.is_syncing = True
        runner.current_file_id = file_id

        mock_proc = unittest.mock.MagicMock()
        mock_proc.poll.return_value = None
        runner.current_process = mock_proc

        # Execute pause
        runner.pause()

        # Check runner state
        self.assertEqual(runner.state, "paused")
        self.assertFalse(runner.is_syncing)
        self.assertIsNone(runner.current_process)
        self.assertIsNone(runner.current_file_id)
        self.assertEqual(runner.paused_file_id, file_id)

        # Process termination must be called
        self.assertTrue(mock_proc.terminate.called)
        self.assertTrue(mock_proc.kill.called)

        # File must be reverted to pending so it is not marked as error
        f_after = database.get_files_by_ids([file_id])[0]
        self.assertEqual(f_after["status"], "pending")

        # Resume task
        runner.resume()
        self.assertEqual(runner.state, "running")

        # Global pause
        sync_engine.pause()
        self.assertEqual(sync_engine.state, "paused")
        self.assertEqual(runner.state, "paused")

        # Global resume
        sync_engine.resume()
        self.assertEqual(sync_engine.state, "running")
        self.assertEqual(runner.state, "running")

        # Global stop
        sync_engine.stop()
        self.assertEqual(sync_engine.state, "stopped")
        self.assertEqual(runner.state, "stopped")
        self.assertIsNone(runner.paused_file_id)

    def test_task_pause_resumes_exact_file_without_skipping(self):
        """Verify that pausing during a transfer re-queues the file, and resuming immediately re-attempts the exact same file instead of moving to the next."""
        import unittest.mock
        task_id = database.create_task({
            "alias": "RESUMETST",
            "name": "Test Resume Same File",
            "source_type": "sftp",
            "source_host": "nas.local",
            "source_user": "user",
            "dest_type": "s3",
            "s3_bucket": "my-bucket",
            "s3_access_key": "dummy_key",
            "s3_secret_key": "dummy_secret",
            "auto_start": 0
        })
        files = [
            {"Path": "file_1.dat", "Name": "file_1.dat", "Size": 100, "ModTime": "2026-10-15T12:00:00Z"},
            {"Path": "file_2.dat", "Name": "file_2.dat", "Size": 200, "ModTime": "2026-10-15T12:00:00Z"},
        ]
        database.map_task_files(task_id, "RESUMETST", files, min_age_seconds=0)

        runner = sync_engine.get_or_create_runner(task_id)
        runner.state = "running"
        runner.stop_requested = False

        transferred_files = []

        def mock_execute(task, file_path, conf_path, reg_cb, prog_cb):
            transferred_files.append(file_path)
            if len(transferred_files) == 1:
                # First attempt for file_1: user pauses!
                runner.pause()
                return (False, "Killed by pause")
            elif len(transferred_files) == 2:
                # Second attempt should be file_1 again!
                return (True, "Success file 1")
            else:
                # Third attempt should be file_2
                runner.stop()
                return (True, "Success file 2")

        async def run_test():
            with unittest.mock.patch("app.sync_engine.RcloneManager.list_source_files", return_value=(True, [], "")), \
                 unittest.mock.patch("app.sync_engine.RcloneManager.execute_file_transfer", side_effect=mock_execute):
                
                async def resume_after_pause():
                    while runner.state != "paused":
                        await asyncio.sleep(0.01)
                    # Simulate user hitting "Reanudar"
                    runner.resume()

                resume_task = asyncio.create_task(resume_after_pause())
                cycle_task = asyncio.create_task(runner.run_sync_cycle())
                
                await asyncio.wait_for(asyncio.gather(resume_task, cycle_task), timeout=5.0)

        asyncio.run(run_test())

        # Assertions:
        self.assertGreaterEqual(len(transferred_files), 2)
        self.assertEqual(transferred_files[0], "file_1.dat")
        self.assertEqual(transferred_files[1], "file_1.dat")
        self.assertEqual(transferred_files[2], "file_2.dat")

if __name__ == "__main__":
    unittest.main()


