import asyncio
import logging
import subprocess
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from pathlib import Path

from app import database
from app.config import DATA_DIR
from app.rclone_manager import RcloneManager
from app.websocket_manager import ws_manager

logger = logging.getLogger("sync_app.engine")

class TaskRunner:
    """
    Manages an individual asynchronous background sync task (e.g. STRAIN or FBE).
    Operates its own loop, process cancellation, and mapping.
    """
    def __init__(self, task_id: int):
        self.task_id = task_id
        self.state: str = "stopped"  # "running", "paused", "stopped"
        self.is_syncing: bool = False
        self.current_process: Optional[subprocess.Popen] = None
        self.current_scan_process: Optional[subprocess.Popen] = None
        self.current_file_id: Optional[int] = None
        self.current_file_path: Optional[str] = None
        self.current_file_progress: float = 0.0
        self.paused_file_id: Optional[int] = None
        self.background_task: Optional[asyncio.Task] = None
        self._trigger_event: Optional[asyncio.Event] = None
        self.stop_requested: bool = False
        self.conf_path = DATA_DIR / f"rclone_task_{task_id}.conf"

    @property
    def trigger_event(self) -> asyncio.Event:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if self._trigger_event is None or (loop and getattr(self._trigger_event, "_loop", None) not in (None, loop)):
            self._trigger_event = asyncio.Event()
        return self._trigger_event

    @property
    def active_file(self) -> Optional[str]:
        return self.current_file_path

    def get_task_data(self) -> Optional[Dict[str, Any]]:
        return database.get_task_by_id(self.task_id)

    def start(self):
        """Starts or resumes this specific task."""
        self.state = "running"
        self.stop_requested = False
        database.set_task_runtime_state(self.task_id, "running")
        try:
            loop = asyncio.get_running_loop()
            if self.background_task is None or self.background_task.done():
                self.background_task = loop.create_task(self.scheduler_loop())
        except RuntimeError:
            pass
        t = self.get_task_data()
        alias = t["alias"] if t else f"#{self.task_id}"
        database.add_log("INFO", f"Tarea [{alias}] iniciada.")
        ws_manager.broadcast_sync("task_state", {"task_id": self.task_id, "state": self.state, "alias": alias})
        self.trigger_event.set()

    def pause(self):
        """Pauses this specific task immediately."""
        self.state = "paused"
        self.is_syncing = False
        database.set_task_runtime_state(self.task_id, "paused", is_syncing=False, current_file=None)

        if self.current_process and self.current_process.poll() is None:
            try:
                logger.info(f"Terminating active rclone process for paused task {self.task_id}...")
                self.current_process.terminate()
                self.current_process.kill()
            except Exception as e:
                logger.warning(f"Error terminating rclone process on pause: {e}")
        self.current_process = None

        if self.current_scan_process and self.current_scan_process.poll() is None:
            try:
                self.current_scan_process.terminate()
                self.current_scan_process.kill()
            except Exception:
                pass
        self.current_scan_process = None

        if self.current_file_id:
            self.paused_file_id = self.current_file_id
            try:
                database.retry_file(self.current_file_id)
            except Exception:
                pass
            self.current_file_id = None
            self.current_file_path = None

        t = self.get_task_data()
        alias = t["alias"] if t else f"#{self.task_id}"
        database.add_log("WARNING", f"Tarea [{alias}] pausada.")
        ws_manager.broadcast_sync("task_state", {
            "task_id": self.task_id,
            "state": self.state,
            "is_syncing": False,
            "alias": alias
        })
        ws_manager.broadcast_sync("files_changed")

    def resume(self):
        """Resumes this specific task if paused."""
        if self.state == "paused":
            self.state = "running"
            self.is_syncing = True
            database.set_task_runtime_state(self.task_id, "running", is_syncing=True)
            try:
                loop = asyncio.get_running_loop()
                if self.background_task is not None and self.background_task.done():
                    self.background_task = loop.create_task(self.scheduler_loop())
            except RuntimeError:
                pass
            t = self.get_task_data()
            alias = t["alias"] if t else f"#{self.task_id}"
            database.add_log("INFO", f"Tarea [{alias}] reanudada.")
            ws_manager.broadcast_sync("task_state", {
                "task_id": self.task_id,
                "state": self.state,
                "is_syncing": True,
                "alias": alias
            })
            self.trigger_event.set()

    def stop(self):
        """Stops this specific task immediately and kills any running rclone transfer."""
        self.state = "stopped"
        self.stop_requested = True
        self.paused_file_id = None
        database.set_task_runtime_state(self.task_id, "stopped", is_syncing=False, current_file=None)

        if self.current_process and self.current_process.poll() is None:
            try:
                logger.info(f"Terminating active rclone process for task {self.task_id}...")
                self.current_process.terminate()
                self.current_process.kill()
            except Exception as e:
                logger.error(f"Error terminating process: {e}")
        self.current_process = None

        if self.current_scan_process and self.current_scan_process.poll() is None:
            try:
                self.current_scan_process.terminate()
                self.current_scan_process.kill()
            except Exception:
                pass
        self.current_scan_process = None

        if self.current_file_id:
            try:
                database.retry_file(self.current_file_id)
            except Exception:
                pass
            self.current_file_id = None
            self.current_file_path = None

        t = self.get_task_data()
        alias = t["alias"] if t else f"#{self.task_id}"
        database.add_log("INFO", f"Tarea [{alias}] detenida.")
        ws_manager.broadcast_sync("task_state", {
            "task_id": self.task_id,
            "state": self.state,
            "is_syncing": False,
            "alias": alias
        })
        ws_manager.broadcast_sync("files_changed")
        self.trigger_event.set()

    def _register_process(self, proc: subprocess.Popen):
        self.current_process = proc
        if self.state in ("paused", "stopped") or self.stop_requested:
            try:
                proc.terminate()
                proc.kill()
            except Exception:
                pass

    def _register_scan_process(self, proc: subprocess.Popen):
        self.current_scan_process = proc
        if self.state in ("paused", "stopped") or self.stop_requested:
            try:
                proc.terminate()
                proc.kill()
            except Exception:
                pass

    async def scan_and_map_new_files(self, task: Optional[Dict[str, Any]] = None) -> int:
        """
        Scans remote SFTP source for newly added files, maps them into SQLite,
        and broadcasts 'files_changed' and 'status_update' so the UI table updates immediately.
        """
        if self.state != "running" or self.stop_requested:
            return 0

        t = task or self.get_task_data()
        if not t or not t.get("source_host") or not t.get("source_user"):
            return 0

        alias = t.get("alias", f"#{self.task_id}")
        recursive = bool(t.get("recursive_scan", 1))

        try:
            success, files_data, err_msg = await asyncio.to_thread(
                RcloneManager.list_source_files,
                t,
                recursive,
                self.conf_path,
                None,
                self._register_scan_process
            )
            self.current_scan_process = None

            if not success or not files_data:
                return 0

            new_c, upd_c = database.map_task_files(
                self.task_id,
                alias,
                files_data,
                min_age_seconds=0,
                allow_overwrite=False,
                date_from=t.get("date_from"),
                date_to=t.get("date_to")
            )

            if new_c > 0:
                database.add_log("INFO", f"[{alias}] {new_c} nuevo(s) archivo(s) detectado(s) en origen y agregado(s) a la cola.")
                ws_manager.broadcast_sync("files_changed")
                sync_engine.broadcast_status()

            return new_c
        except Exception as e:
            logger.warning(f"Error escaneando nuevos archivos para tarea {alias}: {e}")
            return 0

    def sync_now(self):
        """Forces an immediate sync cycle for this task and scans for new files immediately."""
        t = self.get_task_data()
        alias = t["alias"] if t else f"#{self.task_id}"
        database.add_log("INFO", f"Tarea [{alias}]: Sincronización y escaneo inmediato solicitado.")
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.scan_and_map_new_files(t))
        except Exception:
            pass
        self.trigger_event.set()

    async def run_sync_cycle(self) -> Dict[str, Any]:
        """Runs a complete sync cycle for this task."""
        if self.is_syncing:
            return {"status": "already_syncing"}

        if self.state != "running" or self.stop_requested:
            return {"status": "not_running"}

        task = self.get_task_data()
        if not task:
            return {"status": "task_not_found"}

        alias = task["alias"]
        if not task.get("source_host") or not task.get("source_user"):
            logger.debug(f"Task {alias} source not fully configured. Skipping.")
            return {"status": "unconfigured"}

        dest_type = task.get("dest_type", "s3")
        if dest_type == "s3" and (not task.get("s3_access_key") or not task.get("s3_secret_key")):
            logger.debug(f"Task {alias} S3 destination not fully configured. Skipping.")
            return {"status": "unconfigured"}
        if dest_type == "ftp" and not task.get("ftp_host"):
            logger.debug(f"Task {alias} FTP destination not fully configured (missing ftp_host). Skipping.")
            return {"status": "unconfigured"}

        self.is_syncing = True
        database.set_task_runtime_state(self.task_id, self.state, is_syncing=True)
        history_id = database.start_sync_history_record()

        discovered_count = 0
        transferred_count = 0
        failed_count = 0
        cycle_status = "completed"
        cycle_message = f"Ciclo completado para tarea [{alias}]."

        try:
            # 1. Discover & Map files
            recursive = bool(task.get("recursive_scan", 1))
            min_age_minutes = int(task.get("min_file_age", 2) or 0)
            min_age_sec = min_age_minutes * 60

            success, files_data, err_msg = await asyncio.to_thread(
                RcloneManager.list_source_files, task, recursive, self.conf_path, None, self._register_process
            )
            self.current_process = None

            if self.stop_requested or self.state == "stopped":
                database.finish_sync_history_record(history_id, "stopped", 0, 0, 0, "Detenido durante escaneo")
                return {"status": "stopped"}

            if self.state == "paused":
                database.finish_sync_history_record(history_id, "paused", 0, 0, 0, "Pausado durante escaneo")
                return {"status": "paused"}

            if not success:
                cycle_status = "failed"
                cycle_message = f"Error escaneando SFTP ({alias}): {err_msg}"
                database.add_log("ERROR", cycle_message)
                database.finish_sync_history_record(history_id, cycle_status, 0, 0, 0, cycle_message)
                return {"status": "failed", "message": cycle_message}

            new_c, upd_c = database.map_task_files(
                self.task_id, 
                alias, 
                files_data, 
                min_age_seconds=0,
                allow_overwrite=False,
                date_from=task.get("date_from"),
                date_to=task.get("date_to")
            )
            discovered_count = len(files_data)
            database.add_log("INFO", f"[{alias}] {discovered_count} archivos listados ({new_c} nuevos registrados, existentes preservados).")
            if new_c > 0 or upd_c > 0:
                ws_manager.broadcast_sync("files_changed")
                sync_engine.broadcast_status()

            # 2. Transfer pending files for this task
            while True:
                if self.state == "stopped" or self.stop_requested:
                    cycle_status = "stopped"
                    cycle_message = f"Tarea [{alias}] detenida por el usuario."
                    break

                while self.state == "paused":
                    await asyncio.sleep(1)
                    if self.state == "stopped" or self.stop_requested:
                        break

                if self.state == "stopped" or self.stop_requested:
                    cycle_status = "stopped"
                    cycle_message = f"Tarea [{alias}] detenida por el usuario."
                    break

                self.is_syncing = True
                database.set_task_runtime_state(self.task_id, "running", is_syncing=True)

                file_row = None
                if self.paused_file_id:
                    target_rows = database.get_files_by_ids([self.paused_file_id])
                    if target_rows and target_rows[0].get("status") in ("pending", "transferring"):
                        file_row = target_rows[0]
                    self.paused_file_id = None

                if not file_row:
                    next_files = database.get_pending_task_files(self.task_id, limit=1, min_age_seconds=min_age_sec)
                    if not next_files:
                        unmature = database.get_pending_task_files(self.task_id, limit=1, min_age_seconds=0)
                        if unmature and min_age_sec > 0:
                            await asyncio.sleep(3)
                            if self.state != "running" or self.stop_requested:
                                break
                            continue
                        break
                    file_row = dict(next_files[0])

                file_id = file_row["id"]
                file_path = file_row["file_path"]

                self.current_file_id = file_id
                self.current_file_path = file_path
                database.set_file_transferring(file_id)
                database.set_task_runtime_state(self.task_id, self.state, is_syncing=True, current_file=file_path)
                ws_manager.broadcast_sync("file_transferring", {
                    "task_id": self.task_id,
                    "file_id": file_id,
                    "file_path": file_path,
                    "alias": alias
                })

                def on_transfer_progress(progress_dict: Dict[str, Any]):
                    pct = progress_dict.get("percentage", 0.0)
                    bytes_done = progress_dict.get("bytes", 0)
                    total_bytes = progress_dict.get("total_bytes", 0)
                    speed = progress_dict.get("speed", 0.0)
                    eta = progress_dict.get("eta", None)

                    self.current_file_progress = pct
                    sync_engine.active_progress[file_id] = {
                        "percentage": pct,
                        "bytes": bytes_done,
                        "total_bytes": total_bytes,
                        "speed": speed,
                        "eta": eta
                    }

                    ws_manager.broadcast_sync("file_progress", {
                        "task_id": self.task_id,
                        "file_id": file_id,
                        "file_path": file_path,
                        "alias": alias,
                        "percentage": pct,
                        "bytes": bytes_done,
                        "total_bytes": total_bytes,
                        "speed": speed,
                        "eta": eta
                    })

                transfer_finished = asyncio.Event()

                async def periodic_watcher():
                    scan_interval = max(10, int(task.get("sync_interval", 60)))
                    while not transfer_finished.is_set():
                        try:
                            await asyncio.wait_for(transfer_finished.wait(), timeout=scan_interval)
                            break
                        except asyncio.TimeoutError:
                            if self.state == "running" and not self.stop_requested:
                                try:
                                    await self.scan_and_map_new_files(task)
                                except Exception as e:
                                    logger.warning(f"Error en escaneo periódico durante transferencia: {e}")

                watcher_task = asyncio.create_task(periodic_watcher())
                try:
                    transfer_ok, transfer_msg = await asyncio.to_thread(
                        RcloneManager.execute_file_transfer,
                        task,
                        file_path,
                        self.conf_path,
                        self._register_process,
                        on_transfer_progress
                    )
                finally:
                    transfer_finished.set()
                    watcher_task.cancel()
                    try:
                        await watcher_task
                    except asyncio.CancelledError:
                        pass

                sync_engine.active_progress.pop(file_id, None)
                self.current_file_progress = 0.0
                self.current_process = None
                self.current_file_id = None
                self.current_file_path = None

                if self.stop_requested or self.state == "stopped":
                    self.paused_file_id = None
                    database.retry_file(file_id)
                    cycle_status = "stopped"
                    cycle_message = f"Tarea [{alias}] detenida por el usuario."
                    break

                if self.state == "paused" or self.paused_file_id:
                    if not self.paused_file_id:
                        self.paused_file_id = file_id
                    database.retry_file(file_id)
                    cycle_status = "paused"
                    while self.state == "paused":
                        await asyncio.sleep(0.05)
                        if self.state == "stopped" or self.stop_requested:
                            break
                    if self.state == "stopped" or self.stop_requested:
                        self.paused_file_id = None
                        cycle_status = "stopped"
                        cycle_message = f"Tarea [{alias}] detenida por el usuario."
                        break
                    self.is_syncing = True
                    database.set_task_runtime_state(self.task_id, "running", is_syncing=True)
                    continue

                if transfer_ok:
                    database.set_file_completed(file_id)
                    transferred_count += 1
                    database.add_log("INFO", f"[{alias}] Enviado: {file_path}")
                    ws_manager.broadcast_sync("file_completed", {
                        "task_id": self.task_id,
                        "file_id": file_id,
                        "file_path": file_path,
                        "alias": alias
                    })
                    try:
                        await self.scan_and_map_new_files(task)
                    except Exception:
                        pass
                else:
                    database.set_file_error(file_id, transfer_msg)
                    failed_count += 1
                    database.add_log("ERROR", f"[{alias}] Error en {file_path}: {transfer_msg}")
                    ws_manager.broadcast_sync("file_error", {
                        "task_id": self.task_id,
                        "file_id": file_id,
                        "file_path": file_path,
                        "alias": alias,
                        "error": transfer_msg
                    })

        except Exception as e:
            cycle_status = "failed"
            cycle_message = f"Excepción en tarea [{alias}]: {str(e)}"
            database.add_log("ERROR", cycle_message)
            logger.exception(f"Error in task {alias}")
        finally:
            self.is_syncing = False
            self.current_file_id = None
            self.current_file_path = None
            self.current_process = None
            database.set_task_runtime_state(self.task_id, self.state, is_syncing=False, current_file=None)
            database.finish_sync_history_record(history_id, cycle_status, discovered_count, transferred_count, failed_count, cycle_message)
            database.update_task_times(self.task_id, last_run=datetime.now().isoformat())
            ws_manager.broadcast_sync("task_state", {
                "task_id": self.task_id,
                "state": self.state,
                "is_syncing": False,
                "alias": alias
            })

        return {
            "status": cycle_status,
            "discovered": discovered_count,
            "transferred": transferred_count,
            "failed": failed_count,
            "message": cycle_message
        }

    async def scheduler_loop(self):
        logger.info(f"Iniciando bucle asíncrono para tarea ID {self.task_id}...")
        while True:
            try:
                task = self.get_task_data()
                if not task:
                    # Task may have been removed
                    await asyncio.sleep(5)
                    continue

                interval = max(5, int(task.get("sync_interval", 60)))
                if self.state == "running":
                    await self.run_sync_cycle()

                self.trigger_event.clear()
                try:
                    await asyncio.wait_for(self.trigger_event.wait(), timeout=interval)
                except asyncio.TimeoutError:
                    pass
            except asyncio.CancelledError:
                logger.info(f"Bucle de tarea ID {self.task_id} cancelado.")
                break
            except Exception as e:
                logger.error(f"Error en bucle de tarea {self.task_id}: {e}")
                await asyncio.sleep(5)


class MultiSyncEngine:
    """
    Top-level manager coordinating all concurrent TaskRunners, boot recovery,
    manual selective file transfers, and scan previews.
    Also provides backward-compatible singleton interface.
    """
    def __init__(self):
        self.runners: Dict[int, TaskRunner] = {}
        self.active_progress: Dict[int, Dict[str, Any]] = {}
        self.background_task: Optional[asyncio.Task] = None

    @property
    def state(self) -> str:
        """Returns 'running' if at least one task is running, 'paused' if paused, else 'stopped'."""
        if not self.runners:
            # Check legacy state in task_state table
            return database.get_task_state().get("state", "stopped")
        states = [r.state for r in self.runners.values()]
        if "running" in states:
            return "running"
        if "paused" in states:
            return "paused"
        return "stopped"

    @property
    def is_syncing(self) -> bool:
        return any(r.is_syncing for r in self.runners.values())

    @property
    def current_file_path(self) -> Optional[str]:
        for r in self.runners.values():
            if r.current_file_path:
                return r.current_file_path
        return None

    @property
    def active_file(self) -> Optional[str]:
        return self.current_file_path

    def init_and_recover(self):
        """Initializes database schema, executes crash recovery, and auto-starts tasks."""
        database.init_db()
        recovery = database.recover_tasks_from_crash()
        legacy_rec = database.recover_from_crash()
        logger.info(f"Contingency recovery: {recovery}, legacy: {legacy_rec}")

        database.add_log(
            "INFO",
            "Sistema iniciado. Verificación de contingencia multi-tarea completada.",
            f"Archivos recuperados: {recovery['interrupted_files_recovered']}"
        )

        # Initialize runners for all tasks in database
        all_tasks = database.get_all_tasks()
        for t in all_tasks:
            t_id = t["id"]
            runner = self.get_or_create_runner(t_id)
            if t.get("auto_start") == 1 or t.get("state") == "running":
                runner.start()
            else:
                runner.state = "stopped"
                database.set_task_runtime_state(t_id, "stopped", is_syncing=False)

    def get_or_create_runner(self, task_id: int) -> TaskRunner:
        if task_id not in self.runners:
            runner = TaskRunner(task_id)
            try:
                loop = asyncio.get_running_loop()
                runner.background_task = loop.create_task(runner.scheduler_loop())
            except RuntimeError:
                runner.background_task = None
            self.runners[task_id] = runner
        return self.runners[task_id]

    def remove_runner(self, task_id: int):
        if task_id in self.runners:
            runner = self.runners.pop(task_id)
            runner.stop()
            if runner.background_task:
                runner.background_task.cancel()

    # --- Task Controls ---
    def start_task(self, task_id: int):
        runner = self.get_or_create_runner(task_id)
        runner.start()
        self.broadcast_status()
        ws_manager.broadcast_sync("engine_state", {"state": self.state})

    def pause_task(self, task_id: int):
        runner = self.get_or_create_runner(task_id)
        runner.pause()
        self.broadcast_status()
        ws_manager.broadcast_sync("engine_state", {"state": self.state})

    def resume_task(self, task_id: int):
        runner = self.get_or_create_runner(task_id)
        runner.resume()
        self.broadcast_status()
        ws_manager.broadcast_sync("engine_state", {"state": self.state})

    def stop_task(self, task_id: int):
        runner = self.get_or_create_runner(task_id)
        runner.stop()
        self.broadcast_status()
        ws_manager.broadcast_sync("engine_state", {"state": self.state})

    def sync_task_now(self, task_id: int):
        runner = self.get_or_create_runner(task_id)
        runner.sync_now()
        self.broadcast_status()

    # --- Selective File Transfer ---
    async def transfer_selected_files(self, file_ids: List[int]) -> Dict[str, Any]:
        """
        Immediately transfers selected files using their respective task configurations.
        """
        files = database.get_files_by_ids(file_ids)
        if not files:
            return {"success": False, "message": "No se encontraron los archivos solicitados."}

        success_count = 0
        error_count = 0

        for f in files:
            file_id = f["id"]
            file_path = f["file_path"]
            task_id = f["task_id"]

            task = database.get_task_by_id(task_id) if task_id else None
            if not task:
                task = database.get_all_settings()

            database.set_file_transferring(file_id)

            runner = self.runners.get(task_id) if task_id else None
            conf_path = runner.conf_path if runner else None
            alias = task.get("alias", "")

            def on_selected_progress(p_dict: Dict[str, Any]):
                pct = p_dict.get("percentage", 0.0)
                bytes_done = p_dict.get("bytes", 0)
                total_bytes = p_dict.get("total_bytes", 0)
                speed = p_dict.get("speed", 0.0)
                eta = p_dict.get("eta", None)

                sync_engine.active_progress[file_id] = {
                    "percentage": pct,
                    "bytes": bytes_done,
                    "total_bytes": total_bytes,
                    "speed": speed,
                    "eta": eta
                }

                ws_manager.broadcast_sync("file_progress", {
                    "task_id": task_id,
                    "file_id": file_id,
                    "file_path": file_path,
                    "alias": alias,
                    "percentage": pct,
                    "bytes": bytes_done,
                    "total_bytes": total_bytes,
                    "speed": speed,
                    "eta": eta
                })

            ok, msg = await asyncio.to_thread(
                RcloneManager.execute_file_transfer,
                task,
                file_path,
                conf_path,
                None,
                on_selected_progress
            )

            sync_engine.active_progress.pop(file_id, None)

            if ok:
                database.set_file_completed(file_id)
                success_count += 1
            else:
                database.set_file_error(file_id, msg)
                error_count += 1

        alias_summary = f"Archivos procesados: {len(files)}"
        result = {
            "success": True,
            "transferred": success_count,
            "failed": error_count,
            "total": len(files),
            "message": f"Transferencia finalizada: {success_count} exitoso(s), {error_count} con error."
        }
        self.broadcast_status()
        ws_manager.broadcast_sync("files_changed")
        return result

    def broadcast_status(self):
        """Broadcasts live stats and engine state to all active WebSocket clients."""
        if not ws_manager.has_connections():
            return
        stats = database.get_stats()
        stats["state"] = self.state
        stats["task_state"] = self.state
        stats["is_syncing"] = bool(self.is_syncing or stats.get("is_syncing", False) or (stats.get("transferring", 0) > 0))
        stats["current_file"] = self.current_file_path or stats.get("current_file")
        stats["active_file"] = stats["current_file"]
        ws_manager.broadcast_sync("status_update", stats)

    # --- Global / Backward Compatibility Methods ---
    def start(self):
        """Starts all tasks (or legacy singleton)."""
        database.set_task_state("running")
        all_tasks = database.get_all_tasks()
        if all_tasks:
            for t in all_tasks:
                runner = self.get_or_create_runner(t["id"])
                runner.start()
        else:
            for runner in list(self.runners.values()):
                runner.start()
        self.broadcast_status()
        ws_manager.broadcast_sync("engine_state", {"state": self.state})
        ws_manager.broadcast_sync("task_state", {"state": "running"})

    def pause(self):
        database.set_task_state("paused")
        all_tasks = database.get_all_tasks()
        if all_tasks:
            for t in all_tasks:
                runner = self.get_or_create_runner(t["id"])
                runner.pause()
        else:
            for runner in list(self.runners.values()):
                runner.pause()
        self.broadcast_status()
        ws_manager.broadcast_sync("engine_state", {"state": self.state})
        ws_manager.broadcast_sync("task_state", {"state": "paused"})

    def resume(self):
        database.set_task_state("running")
        all_tasks = database.get_all_tasks()
        if all_tasks:
            for t in all_tasks:
                runner = self.get_or_create_runner(t["id"])
                runner.resume()
        else:
            for runner in list(self.runners.values()):
                runner.resume()
        self.broadcast_status()
        ws_manager.broadcast_sync("engine_state", {"state": self.state})
        ws_manager.broadcast_sync("task_state", {"state": "running"})

    def stop(self):
        database.set_task_state("stopped", is_syncing=False)
        all_tasks = database.get_all_tasks()
        if all_tasks:
            for t in all_tasks:
                runner = self.get_or_create_runner(t["id"])
                runner.stop()
        else:
            for runner in list(self.runners.values()):
                runner.stop()
        self.broadcast_status()
        ws_manager.broadcast_sync("engine_state", {"state": self.state})
        ws_manager.broadcast_sync("task_state", {"state": "stopped", "is_syncing": False})
        ws_manager.broadcast_sync("files_changed")

    def sync_now(self):
        all_tasks = database.get_all_tasks()
        if all_tasks:
            for t in all_tasks:
                runner = self.get_or_create_runner(t["id"])
                runner.sync_now()
        else:
            for runner in list(self.runners.values()):
                runner.sync_now()

    async def scan_all_tasks(self) -> Dict[str, Any]:
        """
        Scans remote files for all configured tasks and maps them into SQLite,
        WITHOUT starting task runners or changing running state.
        """
        tasks = database.get_all_tasks()
        if not tasks:
            legacy_settings = database.get_all_settings()
            if legacy_settings.get("source_host"):
                tasks = [legacy_settings]

        total_discovered = 0
        tasks_scanned = 0

        for t in tasks:
            t_id = t.get("id", 1)
            t_alias = t.get("alias", "DEFAULT")
            runner = self.get_or_create_runner(t_id)

            recursive = str(t.get("recursive_scan", "1")) in ("1", "true", "True", True, 1)
            min_age_minutes = int(t.get("min_file_age", 2) or 2)
            min_age_str = f"{min_age_minutes}m" if min_age_minutes > 0 else None
            min_age_sec = min_age_minutes * 60

            try:
                ok, files_data, err = await asyncio.to_thread(
                    RcloneManager.list_source_files,
                    t,
                    recursive,
                    runner.conf_path,
                    None
                )
                if ok and files_data:
                    new_c, upd_c = database.map_task_files(
                        t_id, 
                        t_alias, 
                        files_data, 
                        min_age_seconds=0,
                        allow_overwrite=False,
                        date_from=t.get("date_from"),
                        date_to=t.get("date_to")
                    )
                    total_discovered += new_c
            except Exception as e:
                logger.error(f"Error escaneando tarea {t_alias}: {e}")

            tasks_scanned += 1

        self.broadcast_status()
        ws_manager.broadcast_sync("files_changed")
        return {
            "success": True,
            "tasks_scanned": tasks_scanned,
            "total_discovered": total_discovered,
            "message": f"Escaneo completado: {total_discovered} archivo(s) nuevos registrados (>2m de antigüedad) en {tasks_scanned} tarea(s)."
        }

    async def scan_all_and_start(self) -> Dict[str, Any]:
        """
        Scans remote files for all configured tasks, maps them into SQLite,
        and starts execution for all tasks.
        """
        tasks = database.get_all_tasks()
        if not tasks:
            legacy_settings = database.get_all_settings()
            if legacy_settings.get("source_host"):
                tasks = [legacy_settings]

        total_discovered = 0
        tasks_started = 0

        for t in tasks:
            t_id = t.get("id", 1)
            t_alias = t.get("alias", "DEFAULT")
            runner = self.get_or_create_runner(t_id)

            recursive = str(t.get("recursive_scan", "1")) in ("1", "true", "True", True, 1)
            min_age_minutes = int(t.get("min_file_age", 2) or 2)
            min_age_str = f"{min_age_minutes}m" if min_age_minutes > 0 else None
            min_age_sec = min_age_minutes * 60

            try:
                ok, files_data, err = await asyncio.to_thread(
                    RcloneManager.list_source_files,
                    t,
                    recursive,
                    runner.conf_path,
                    min_age_str
                )
                if ok and files_data:
                    new_c, upd_c = database.map_task_files(
                        t_id, 
                        t_alias, 
                        files_data, 
                        min_age_seconds=min_age_sec,
                        allow_overwrite=False,
                        date_from=t.get("date_from"),
                        date_to=t.get("date_to")
                    )
                    total_discovered += new_c
            except Exception as e:
                logger.error(f"Error escaneando tarea {t_alias}: {e}")

            runner.start()
            tasks_started += 1

        database.set_task_state("running")
        self.broadcast_status()
        ws_manager.broadcast_sync("files_changed")
        return {
            "success": True,
            "tasks_started": tasks_started,
            "total_discovered": total_discovered,
            "message": f"Escaneo completado: {total_discovered} archivo(s) descubiertos/actualizados. {tasks_started} tarea(s) iniciadas."
        }

    async def scheduler_loop(self):
        """Master supervisor loop."""
        tick = 0
        while True:
            try:
                tick += 1
                # Periodically ensure all DB tasks have active runners
                if tick % 3 == 0:
                    tasks = database.get_all_tasks()
                    for t in tasks:
                        t_id = t["id"]
                        if t_id not in self.runners:
                            runner = self.get_or_create_runner(t_id)
                            if t.get("state") == "running":
                                runner.start()

                # If there are active WebSocket connections, push live status tick
                if ws_manager.has_connections():
                    self.broadcast_status()

                await asyncio.sleep(2)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in multi-sync supervisor: {e}")
                await asyncio.sleep(3)

# Singleton Instance
sync_engine = MultiSyncEngine()
