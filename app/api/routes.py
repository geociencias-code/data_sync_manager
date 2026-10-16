import os
import sys
import json
import asyncio
import platform
import subprocess
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List

from fastapi import APIRouter, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

from app import database
from app.config import TEMPLATES_DIR, BASE_DIR, get_lan_ip, get_tailscale_ip, get_all_network_interfaces
from app.rclone_manager import RcloneManager
from app.sync_engine import sync_engine
from app.websocket_manager import ws_manager

logger = logging.getLogger("sync_app.routes")
router = APIRouter()

# --- Request Models ---
class SettingsPayload(BaseModel):
    # SFTP Source
    source_type: str = "sftp"
    source_host: str = ""
    source_port: str = "22"
    source_user: str = ""
    source_pass: Optional[str] = None
    source_key_file: Optional[str] = ""
    source_path: Optional[str] = ""
    
    # Destination
    dest_type: str = "s3"  # "s3" or "ftp"
    
    # S3 Dest
    s3_provider: str = "AWS"
    s3_bucket: Optional[str] = ""
    s3_access_key: Optional[str] = ""
    s3_secret_key: Optional[str] = None
    s3_region: str = "us-east-1"
    s3_endpoint: Optional[str] = ""
    s3_path: Optional[str] = ""
    
    # FTP Dest
    ftp_host: Optional[str] = ""
    ftp_port: str = "21"
    ftp_user: Optional[str] = ""
    ftp_pass: Optional[str] = None
    ftp_path: Optional[str] = ""
    
    # Scheduler
    sync_interval: str = "60"
    auto_start: str = "1"
    recursive_scan: str = "1"
    delete_source_after: str = "0"
    max_retries: str = "3"
    date_from: Optional[str] = ""
    date_to: Optional[str] = ""

class TestConnectionPayload(BaseModel):
    settings: Dict[str, Any]

class TaskPayload(BaseModel):
    name: str
    alias: str
    description: Optional[str] = ""
    source_type: str = "sftp"
    source_host: str = ""
    source_port: str = "22"
    source_user: str = ""
    source_pass: Optional[str] = None
    source_key_file: Optional[str] = ""
    source_path: Optional[str] = ""
    dest_type: str = "s3"
    s3_provider: str = "AWS"
    s3_bucket: Optional[str] = ""
    s3_access_key: Optional[str] = ""
    s3_secret_key: Optional[str] = None
    s3_region: str = "us-east-1"
    s3_endpoint: Optional[str] = ""
    s3_path: Optional[str] = ""
    ftp_host: Optional[str] = ""
    ftp_port: str = "21"
    ftp_user: Optional[str] = ""
    ftp_pass: Optional[str] = None
    ftp_path: Optional[str] = ""
    date_from: Optional[str] = ""
    date_to: Optional[str] = ""
    sync_interval: int = 60
    min_file_age: int = 2
    recursive_scan: int = 1
    auto_start: int = 1

class TransferSelectedPayload(BaseModel):
    file_ids: List[int]

# --- Frontend Root ---
@router.get("/", response_class=HTMLResponse)
async def serve_index():
    index_file = TEMPLATES_DIR / "index.html"
    if not index_file.exists():
        raise HTTPException(status_code=404, detail="Template index.html no encontrado.")
    return HTMLResponse(content=index_file.read_text(encoding="utf-8"))

def get_current_system_status() -> Dict[str, Any]:
    stats = database.get_stats()
    # Ensure current in-memory engine state is reflected
    engine_state = sync_engine.state
    if engine_state:
        stats["task_state"] = engine_state

    is_syncing = bool(sync_engine.is_syncing or stats.get("is_syncing", False) or (stats.get("transferring", 0) > 0))
    current_file = sync_engine.current_file_path or stats.get("current_file")

    current_progress = None
    for runner in sync_engine.runners.values():
        if runner.current_file_progress:
            current_progress = runner.current_file_progress
            break

    stats["is_syncing"] = is_syncing
    stats["current_file"] = current_file
    stats["current_file_progress"] = current_progress
    
    port = int(os.getenv("PORT", 8000))
    lan_ip = get_lan_ip()
    ts_ip = get_tailscale_ip()
    interfaces = get_all_network_interfaces()
    stats["lan_ip"] = lan_ip
    stats["tailscale_ip"] = ts_ip
    stats["port"] = port
    stats["local_url"] = f"http://localhost:{port}"
    stats["network_url"] = f"http://{lan_ip}:{port}" if lan_ip != "127.0.0.1" else f"http://localhost:{port}"
    stats["tailscale_url"] = f"http://{ts_ip}:{port}" if ts_ip else None
    stats["network_interfaces"] = interfaces
    return stats

# --- Status & Control Endpoints ---
@router.get("/api/status")
async def get_status():
    return get_current_system_status()

@router.get("/api/network-interfaces")
async def get_network_ips():
    return {"interfaces": get_all_network_interfaces()}

@router.post("/api/control/{action}")
async def control_task(action: str):
    valid_actions = ["start", "pause", "resume", "stop", "sync_now"]
    if action not in valid_actions:
        raise HTTPException(status_code=400, detail=f"Acción inválida '{action}'. Válidas: {valid_actions}")
        
    if action == "start":
        sync_engine.start()
    elif action == "pause":
        sync_engine.pause()
    elif action == "resume":
        sync_engine.resume()
    elif action == "stop":
        sync_engine.stop()
    elif action == "sync_now":
        sync_engine.sync_now()
        
    return {
        "status": "success",
        "action": action,
        "current_state": sync_engine.state,
        "is_syncing": sync_engine.is_syncing
    }

# --- Multi-Task Management Endpoints ---
@router.get("/api/tasks")
async def list_tasks():
    tasks = database.get_all_tasks()
    for t in tasks:
        t_id = t["id"]
        runner = sync_engine.runners.get(t_id)
        if runner:
            t["live_state"] = runner.state
            t["is_syncing"] = runner.is_syncing
            t["current_file"] = runner.current_file_path
        else:
            t["live_state"] = t.get("state", "stopped")
            t["is_syncing"] = False
            t["current_file"] = None
    return {"tasks": tasks}

@router.post("/api/tasks")
async def create_task(payload: TaskPayload):
    existing = database.get_task_by_alias(payload.alias)
    if existing:
        raise HTTPException(status_code=400, detail=f"Ya existe una tarea con el alias '{payload.alias}'.")
        
    task_id = database.create_task(payload.model_dump())
    runner = sync_engine.get_or_create_runner(task_id)
    if payload.auto_start == 1:
        runner.start()
    return {"status": "success", "task_id": task_id, "message": f"Tarea '{payload.name}' creada con éxito."}

@router.get("/api/tasks/{task_id}")
async def get_task(task_id: int):
    task = database.get_task_by_id(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Tarea no encontrada.")
    masked = dict(task)
    masked["has_source_pass"] = bool(task.get("source_pass"))
    masked["has_s3_secret"] = bool(task.get("s3_secret_key"))
    masked["has_ftp_pass"] = bool(task.get("ftp_pass"))
    masked["source_pass"] = "********" if task.get("source_pass") else ""
    masked["s3_secret_key"] = "********" if task.get("s3_secret_key") else ""
    masked["ftp_pass"] = "********" if task.get("ftp_pass") else ""
    return masked

@router.put("/api/tasks/{task_id}")
async def update_task_endpoint(task_id: int, payload: TaskPayload):
    task = database.get_task_by_id(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Tarea no encontrada.")
    
    data = payload.model_dump()
    if data.get("source_pass") == "********":
        del data["source_pass"]
    if data.get("s3_secret_key") == "********":
        del data["s3_secret_key"]
    if data.get("ftp_pass") == "********":
        del data["ftp_pass"]
        
    database.update_task(task_id, data)
    return {"status": "success", "message": "Tarea actualizada correctamente."}

@router.delete("/api/tasks/{task_id}")
async def delete_task_endpoint(task_id: int):
    sync_engine.remove_runner(task_id)
    database.delete_task(task_id)
    return {"status": "success", "message": "Tarea y sus registros eliminados."}

@router.post("/api/tasks/{task_id}/control/{action}")
async def control_single_task(task_id: int, action: str):
    valid_actions = ["start", "pause", "resume", "stop", "sync_now"]
    if action not in valid_actions:
        raise HTTPException(status_code=400, detail=f"Acción inválida '{action}'. Válidas: {valid_actions}")
        
    task = database.get_task_by_id(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Tarea no encontrada.")
        
    if action == "start":
        sync_engine.start_task(task_id)
    elif action == "pause":
        sync_engine.pause_task(task_id)
    elif action == "resume":
        sync_engine.resume_task(task_id)
    elif action == "stop":
        sync_engine.stop_task(task_id)
    elif action == "sync_now":
        sync_engine.sync_task_now(task_id)
        
    runner = sync_engine.get_or_create_runner(task_id)
    return {
        "status": "success",
        "task_id": task_id,
        "action": action,
        "state": runner.state,
        "is_syncing": runner.is_syncing
    }

@router.post("/api/tasks/scan-preview")
async def scan_preview_task(payload: TestConnectionPayload):
    """
    On-demand remote scan to preview files available in the SFTP source folder
    before or during task configuration, applying optional date filters.
    """
    settings = dict(payload.settings)
    recursive = str(settings.get("recursive_scan", "1")) in ("1", "true", "True", True, 1)
    success, files, err = await asyncio.to_thread(
        RcloneManager.scan_source_files,
        settings,
        recursive,
        3
    )
    if not success:
        return {"success": False, "message": err, "files": [], "count": 0, "total_size": 0}

    # Filter files by date range if specified in settings
    date_from = settings.get("date_from")
    date_to = settings.get("date_to")
    dt_from = database.parse_filter_datetime(date_from, is_end_of_day=False) if date_from else None
    dt_to = database.parse_filter_datetime(date_to, is_end_of_day=True) if date_to else None

    if dt_from is not None or dt_to is not None:
        filtered = []
        for f in files:
            mod_dt = database.parse_iso_datetime(f.get("ModTime", ""))
            if not mod_dt:
                continue
            if dt_from is not None and mod_dt < dt_from:
                continue
            if dt_to is not None and mod_dt > dt_to:
                continue
            filtered.append(f)
        files = filtered

    total_size = sum(f.get("Size", 0) for f in files)
    filter_msg = ""
    if dt_from and dt_to:
        filter_msg = f" (Filtro: {date_from} hasta {date_to})"
    elif dt_from:
        filter_msg = f" (Filtro: desde {date_from})"
    elif dt_to:
        filter_msg = f" (Filtro: hasta {date_to})"

    return {
        "success": True, 
        "files": files[:150],
        "count": len(files),
        "total_size": total_size,
        "message": f"Se detectaron {len(files)} archivo(s) en la carpeta remota{filter_msg}."
    }

@router.get("/api/task-aliases")
async def list_task_aliases():
    return {"aliases": database.get_all_task_aliases()}

@router.post("/api/files/transfer-selected")
async def transfer_selected_files_endpoint(payload: TransferSelectedPayload):
    """Immediately transfers selected files from the UI table."""
    result = await sync_engine.transfer_selected_files(payload.file_ids)
    return result

@router.post("/api/tasks/scan-all")
async def scan_all_tasks_only_endpoint():
    """
    Triggers remote file scan across all tasks, discovering and mapping files
    into SQLite WITHOUT starting task runners or changing running state.
    """
    result = await sync_engine.scan_all_tasks()
    return result

@router.post("/api/tasks/scan-and-start")
async def scan_and_start_all_tasks():
    """
    Triggers remote file scan across all tasks, maps discovered files to SQLite,
    and starts the execution of all tasks.
    """
    result = await sync_engine.scan_all_and_start()
    return result

@router.post("/api/network/ping")
async def ping_network_endpoints(payload: Optional[TestConnectionPayload] = None, target: Optional[str] = Query(None)):
    """
    Tests ping, DNS resolution, and TCP latency for origin (SFTP NAS),
    destination (S3 / FTP), or both.
    """
    settings = {}
    if payload and payload.settings:
        settings = dict(payload.settings)
    else:
        tasks = database.get_all_tasks()
        if tasks:
            settings = dict(tasks[0])
        else:
            settings = database.get_all_settings()
    result = await asyncio.to_thread(RcloneManager.ping_endpoints, settings, target=target)
    return result

@router.post("/api/tasks/{task_id}/ping")
async def ping_single_task_endpoints(task_id: int, target: Optional[str] = Query(None)):
    """
    Pings the specific origin, destination, or both of an existing task by ID.
    """
    task = database.get_task_by_id(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Tarea no encontrada.")
    result = await asyncio.to_thread(RcloneManager.ping_endpoints, task, target=target)
    result["task_alias"] = task.get("alias")
    result["task_name"] = task.get("name")
    return result

# --- WebSocket Real-Time Channel ---
@router.websocket("/ws")
async def websocket_live_channel(websocket: WebSocket):
    """
    Live real-time WebSocket connection for DataSync Manager frontend.
    Pushes status updates, task runner events, and file transfer progress
    without requiring constant client-side HTTP polling.
    """
    await ws_manager.connect(websocket)
    try:
        # Send initial full state immediately upon connection
        initial_stats = get_current_system_status()
        
        await websocket.send_text(json.dumps({
            "type": "initial_state",
            "data": {
                "stats": initial_stats,
                "server_time": datetime.now().isoformat()
            }
        }))

        while True:
            raw_text = await websocket.receive_text()
            try:
                msg = json.loads(raw_text)
                action = msg.get("action")
                if action == "ping":
                    await websocket.send_text(json.dumps({"type": "pong", "data": {"timestamp": datetime.now().isoformat()}}))
                elif action == "refresh":
                    st = get_current_system_status()
                    await websocket.send_text(json.dumps({
                        "type": "status_update",
                        "data": st
                    }))
            except Exception:
                pass
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
    except Exception as e:
        logger.exception(f"Error inesperado en WebSocket: {e}")
        ws_manager.disconnect(websocket)

# --- Settings Endpoints ---
@router.get("/api/config")
async def get_config():
    raw = database.get_all_settings()
    # Mask sensitive credentials for security in frontend
    masked = dict(raw)
    masked["has_source_pass"] = bool(raw.get("source_pass"))
    masked["has_s3_secret"] = bool(raw.get("s3_secret_key"))
    masked["has_ftp_pass"] = bool(raw.get("ftp_pass"))
    
    # Don't send raw passwords to UI unless user explicitly changes them
    masked["source_pass"] = "********" if raw.get("source_pass") else ""
    masked["s3_secret_key"] = "********" if raw.get("s3_secret_key") else ""
    masked["ftp_pass"] = "********" if raw.get("ftp_pass") else ""
    return masked

@router.post("/api/config")
async def save_config(payload: SettingsPayload):
    current = database.get_all_settings()
    data = payload.model_dump()
    
    # Preserve existing passwords if user didn't enter a new one (e.g. left masked or empty)
    if not data.get("source_pass") or data.get("source_pass") == "********":
        data["source_pass"] = current.get("source_pass", "")
        
    if not data.get("s3_secret_key") or data.get("s3_secret_key") == "********":
        data["s3_secret_key"] = current.get("s3_secret_key", "")
        
    if not data.get("ftp_pass") or data.get("ftp_pass") == "********":
        data["ftp_pass"] = current.get("ftp_pass", "")
        
    database.update_settings(data)
    
    # Re-generate rclone configuration file
    try:
        RcloneManager.generate_config_file(data)
        database.add_log("INFO", "Configuración de remotos y programación actualizada correctamente.")
    except Exception as e:
        database.add_log("ERROR", f"Error generando rclone.conf: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error actualizando configuración rclone: {str(e)}")
        
    return {"status": "success", "message": "Configuración guardada exitosamente."}

# --- Connectivity Tests ---
@router.post("/api/config/test-source")
async def test_source_conn(payload: Optional[TestConnectionPayload] = None):
    settings = database.get_all_settings()
    if payload and payload.settings:
        task_id = payload.settings.get("id")
        existing_task = database.get_task_by_id(int(task_id)) if task_id else None
        for k, v in payload.settings.items():
            if v == "********":
                if existing_task and existing_task.get(k):
                    settings[k] = existing_task[k]
                elif k in settings:
                    continue
            else:
                settings[k] = v
            
    success, msg = RcloneManager.test_source_connection(settings)
    return {"success": success, "message": msg}

@router.post("/api/config/test-dest")
async def test_dest_conn(payload: Optional[TestConnectionPayload] = None):
    settings = database.get_all_settings()
    if payload and payload.settings:
        task_id = payload.settings.get("id")
        existing_task = database.get_task_by_id(int(task_id)) if task_id else None
        for k, v in payload.settings.items():
            if v == "********":
                if existing_task and existing_task.get(k):
                    settings[k] = existing_task[k]
                elif k in settings:
                    continue
            else:
                settings[k] = v
            
    success, msg = RcloneManager.test_dest_connection(settings)
    return {"success": success, "message": msg}

# --- Files Explorer & Retry Endpoints ---
@router.get("/api/files")
async def list_files(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=5, le=100),
    status: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    task_alias: Optional[str] = Query(None)
):
    data = database.get_files_paginated(
        page=page, 
        per_page=per_page, 
        status=status, 
        search=search,
        task_alias=task_alias
    )
    for item in data.get("items", []):
        f_id = item.get("id")
        if f_id in sync_engine.active_progress:
            active = sync_engine.active_progress[f_id]
            item["progress_pct"] = active.get("percentage", 0.0)
            item["live_speed"] = active.get("speed", 0.0)
            item["live_eta"] = active.get("eta", None)
        elif item.get("status") == "completed":
            item["progress_pct"] = 100.0
        elif item.get("status") == "pending":
            item["progress_pct"] = 0.0
    return data

@router.post("/api/files/{file_id}/retry")
async def retry_single_file(file_id: int):
    success = database.retry_file(file_id)
    if not success:
        raise HTTPException(status_code=404, detail="Archivo no encontrado.")
    database.add_log("INFO", f"Archivo ID #{file_id} reencolado para transferencia manual.")
    return {"status": "success", "message": "Archivo reencolado para transferencia."}

@router.post("/api/files/retry-all-errors")
async def retry_all_error_files():
    count = database.retry_all_errors()
    database.add_log("INFO", f"Se reencolaron {count} archivos que tenían estado de error.")
    return {"status": "success", "count": count, "message": f"{count} archivos reencolados."}

# --- Logs & History Endpoints ---
@router.get("/api/logs")
async def list_logs(limit: int = Query(50, ge=10, le=200)):
    return database.get_logs(limit=limit)

@router.get("/api/history")
async def list_history(limit: int = Query(15, ge=5, le=50)):
    return database.get_sync_history(limit=limit)

# --- Contingency & OS Autostart Service Endpoints ---
@router.get("/api/service/status")
async def get_service_status():
    system = platform.system()
    home = Path.home()
    is_installed = False
    service_path = ""
    
    if system == "Darwin":
        plist_path = home / "Library" / "LaunchAgents" / "com.syncdata.sftp_s3.plist"
        is_installed = plist_path.exists()
        service_path = str(plist_path)
    elif system == "Linux":
        unit_path = Path("/etc/systemd/system/syncdata-sftp-s3.service")
        user_unit = home / ".config" / "systemd" / "user" / "syncdata-sftp-s3.service"
        is_installed = unit_path.exists() or user_unit.exists()
        service_path = str(unit_path if unit_path.exists() else user_unit)
    elif system == "Windows":
        appdata = os.environ.get("APPDATA")
        startup_script = Path(appdata) / "Microsoft" / "Windows" / "Start Menu" / "Programs" / "Startup" / "DataSyncPro_AutoStart.vbs" if appdata else None
        is_installed = startup_script.exists() if startup_script else False
        service_path = str(startup_script) if startup_script else ""
        
    venv_py = BASE_DIR / "venv" / ("Scripts/python.exe" if system == "Windows" else "bin/python")
    py_exec = str(venv_py) if venv_py.exists() else sys.executable
    
    return {
        "os": system,
        "is_installed": is_installed,
        "service_path": service_path,
        "python_path": py_exec,
        "app_dir": str(BASE_DIR)
    }

@router.post("/api/service/install")
async def install_os_service():
    system = platform.system()
    venv_py = BASE_DIR / "venv" / ("Scripts/python.exe" if system == "Windows" else "bin/python")
    py_exec = str(venv_py) if venv_py.exists() else sys.executable
    app_dir = str(BASE_DIR)
    
    if system == "Darwin":
        home = Path.home()
        launch_dir = home / "Library" / "LaunchAgents"
        launch_dir.mkdir(parents=True, exist_ok=True)
        plist_path = launch_dir / "com.syncdata.sftp_s3.plist"
        
        plist_content = f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.syncdata.sftp_s3</string>
    <key>ProgramArguments</key>
    <array>
        <string>{py_exec}</string>
        <string>{app_dir}/main.py</string>
    </array>
    <key>WorkingDirectory</key>
    <string>{app_dir}</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>{app_dir}/data/service.log</string>
    <key>StandardErrorPath</key>
    <string>{app_dir}/data/service_error.log</string>
</dict>
</plist>
"""
        plist_path.write_text(plist_content, encoding="utf-8")
        
        # Load the service using launchctl
        try:
            subprocess.run(["launchctl", "unload", str(plist_path)], capture_output=True)
            subprocess.run(["launchctl", "load", str(plist_path)], capture_output=True, check=True)
            database.add_log("INFO", "Servicio LaunchAgent instalado y cargado en macOS para auto-inicio tras reinicio.")
            return {
                "success": True, 
                "message": f"Servicio macOS registrado con éxito en {plist_path}. La app iniciará automáticamente al encender el equipo."
            }
        except Exception:
            return {
                "success": True, 
                "message": f"Archivo plist creado en {plist_path}. Para activarlo manualmente: launchctl load {plist_path}"
            }
    elif system == "Windows":
        appdata = os.environ.get("APPDATA")
        if not appdata:
            return {"success": False, "message": "No se pudo localizar el directorio APPDATA en Windows."}
            
        startup_dir = Path(appdata) / "Microsoft" / "Windows" / "Start Menu" / "Programs" / "Startup"
        startup_dir.mkdir(parents=True, exist_ok=True)
        vbs_path = startup_dir / "DataSyncPro_AutoStart.vbs"
        
        clean_py = py_exec.replace("/", "\\")
        clean_app = Path(app_dir) / "main.py"
        clean_app_str = str(clean_app).replace("/", "\\")
        
        vbs_content = f'Set WshShell = CreateObject("WScript.Shell")\n' \
                      f'WshShell.CurrentDirectory = "{str(BASE_DIR).replace("/", chr(92))}"\n' \
                      f'WshShell.Run """{clean_py}"" ""{clean_app_str}""", 0, False\n'
                      
        vbs_path.write_text(vbs_content, encoding="utf-8")
        database.add_log("INFO", f"Script de inicio silencioso en Windows configurado en: {vbs_path}")
        return {
            "success": True,
            "message": f"Servicio de autoinicio configurado en Windows ({vbs_path}). La app se ejecutará en segundo plano automáticamente al iniciar sesión o reiniciar el sistema."
        }
    elif system == "Linux":
        home = Path.home()
        user_systemd = home / ".config" / "systemd" / "user"
        user_systemd.mkdir(parents=True, exist_ok=True)
        service_file = user_systemd / "syncdata-sftp-s3.service"
        
        content = f"""[Unit]
Description=DataSync Manager SFTP to S3/FTP Manager
After=network.target

[Service]
Type=simple
WorkingDirectory={app_dir}
ExecStart={py_exec} {app_dir}/main.py
Restart=always
RestartSec=10
StandardOutput=append:{app_dir}/data/service.log
StandardError=append:{app_dir}/data/service_error.log

[Install]
WantedBy=default.target
"""
        service_file.write_text(content, encoding="utf-8")
        try:
            subprocess.run(["systemctl", "--user", "daemon-reload"], capture_output=True)
            subprocess.run(["systemctl", "--user", "enable", "--now", "syncdata-sftp-s3.service"], capture_output=True, check=True)
            database.add_log("INFO", "Servicio systemd de usuario configurado y activado en Linux.")
            return {
                "success": True,
                "message": f"Servicio systemd activado en Linux ({service_file}). Arrancará automáticamente con el sistema."
            }
        except Exception:
            return {
                "success": True,
                "message": f"Unidad creada en {service_file}. Para servicio global de sistema ejecuta: sudo ./scripts/install_service.sh"
            }
    else:
        return {
            "success": False,
            "message": f"Sistema operativo '{system}' no soportado para instalación automática de servicio."
        }
