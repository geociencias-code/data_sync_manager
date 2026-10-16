import sqlite3
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple, Union
from contextlib import contextmanager

from app.config import DB_PATH

logger = logging.getLogger("sync_app.database")

@contextmanager
def get_db():
    """Context manager for SQLite connections with WAL mode and row factory."""
    conn = sqlite3.connect(str(DB_PATH), timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def init_db():
    """Initializes the database schema and default records."""
    with get_db() as conn:
        cursor = conn.cursor()
        
        # 1. Settings table (key-value store for remotes & scheduler configuration)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # 2. Files mapped from source (SFTP)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER,
                task_alias TEXT,
                file_path TEXT NOT NULL,
                file_name TEXT NOT NULL,
                file_size INTEGER NOT NULL DEFAULT 0,
                mod_time TEXT,
                status TEXT NOT NULL DEFAULT 'pending', -- 'pending', 'transferring', 'completed', 'error'
                error_message TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                first_mapped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_attempt_at TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP
            );
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_files_status ON files(status);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_files_path ON files(file_path);")

        # Column migration check for files
        cursor.execute("PRAGMA table_info(files);")
        cols = [c["name"] for c in cursor.fetchall()]
        if "updated_at" not in cols:
            cursor.execute("ALTER TABLE files ADD COLUMN updated_at TIMESTAMP;")
        if "task_id" not in cols:
            cursor.execute("ALTER TABLE files ADD COLUMN task_id INTEGER;")
        if "task_alias" not in cols:
            cursor.execute("ALTER TABLE files ADD COLUMN task_alias TEXT;")
        if "progress_pct" not in cols:
            cursor.execute("ALTER TABLE files ADD COLUMN progress_pct REAL DEFAULT 0.0;")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_files_alias ON files(task_alias);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_files_task_id ON files(task_id);")

        # 2.1 Multi-Task sync_tasks table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sync_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                alias TEXT NOT NULL UNIQUE,          -- ej. 'STRAIN', 'FBE'
                description TEXT,
                source_type TEXT NOT NULL DEFAULT 'sftp',
                source_host TEXT NOT NULL DEFAULT '',
                source_port TEXT NOT NULL DEFAULT '22',
                source_user TEXT NOT NULL DEFAULT '',
                source_pass TEXT DEFAULT '',
                source_key_file TEXT DEFAULT '',
                source_path TEXT DEFAULT '',         -- ej. 'datos/strain' o 'datos/fbe'
                dest_type TEXT NOT NULL DEFAULT 's3',
                s3_provider TEXT DEFAULT 'AWS',
                s3_bucket TEXT DEFAULT '',
                s3_access_key TEXT DEFAULT '',
                s3_secret_key TEXT DEFAULT '',
                s3_region TEXT DEFAULT 'us-east-1',
                s3_endpoint TEXT DEFAULT '',
                s3_path TEXT DEFAULT '',             -- ej. 'backup/strain'
                ftp_host TEXT DEFAULT '',
                ftp_port TEXT DEFAULT '21',
                ftp_user TEXT DEFAULT '',
                ftp_pass TEXT DEFAULT '',
                ftp_path TEXT DEFAULT '',
                date_from TEXT DEFAULT '',
                date_to TEXT DEFAULT '',
                sync_interval INTEGER NOT NULL DEFAULT 60,
                min_file_age INTEGER NOT NULL DEFAULT 2,   -- minutos mínimos de antigüedad antes de sincronizar
                recursive_scan INTEGER NOT NULL DEFAULT 1,
                auto_start INTEGER NOT NULL DEFAULT 1,
                state TEXT NOT NULL DEFAULT 'stopped', -- 'running', 'paused', 'stopped'
                is_syncing INTEGER NOT NULL DEFAULT 0,
                current_file TEXT,
                last_run TIMESTAMP,
                next_run TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tasks_alias ON sync_tasks(alias);")

        # Migrate existing sync_tasks table if min_file_age or date filters are missing
        cursor.execute("PRAGMA table_info(sync_tasks)")
        sync_task_cols = [c["name"] for c in cursor.fetchall()]
        if "min_file_age" not in sync_task_cols:
            cursor.execute("ALTER TABLE sync_tasks ADD COLUMN min_file_age INTEGER NOT NULL DEFAULT 2")
        if "date_from" not in sync_task_cols:
            cursor.execute("ALTER TABLE sync_tasks ADD COLUMN date_from TEXT DEFAULT ''")
        if "date_to" not in sync_task_cols:
            cursor.execute("ALTER TABLE sync_tasks ADD COLUMN date_to TEXT DEFAULT ''")

        # 3. Task state & Scheduler control table (holds singleton record id=1)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS task_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                state TEXT NOT NULL DEFAULT 'stopped', -- 'running', 'paused', 'stopped'
                is_syncing INTEGER NOT NULL DEFAULT 0,
                current_file TEXT,
                last_run TIMESTAMP,
                next_run TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cursor.execute("""
            INSERT OR IGNORE INTO task_state (id, state, is_syncing, current_file)
            VALUES (1, 'stopped', 0, NULL);
        """)

        # 4. Sync history table (record of each sync cycle)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sync_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                finished_at TIMESTAMP,
                status TEXT NOT NULL, -- 'completed', 'failed', 'stopped', 'interrupted'
                files_discovered INTEGER DEFAULT 0,
                files_transferred INTEGER DEFAULT 0,
                files_failed INTEGER DEFAULT 0,
                message TEXT
            );
        """)

        # 5. Application logs table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS system_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                level TEXT NOT NULL DEFAULT 'INFO',
                message TEXT NOT NULL,
                details TEXT
            );
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_logs_time ON system_logs(timestamp);")

        # Default Settings if not existing
        default_settings = {
            "source_type": "sftp",
            "source_host": "",
            "source_port": "22",
            "source_user": "",
            "source_pass": "",
            "source_key_file": "",
            "source_path": "",
            "dest_type": "s3", # 's3' or 'ftp'
            # S3 settings
            "s3_provider": "AWS",
            "s3_bucket": "",
            "s3_access_key": "",
            "s3_secret_key": "",
            "s3_region": "us-east-1",
            "s3_endpoint": "",
            "s3_path": "",
            # FTP settings
            "ftp_host": "",
            "ftp_port": "21",
            "ftp_user": "",
            "ftp_pass": "",
            "ftp_path": "",
            # Scheduler settings
            "sync_interval": "60", # seconds
            "auto_start": "1", # 1 = true, 0 = false
            "recursive_scan": "1",
            "delete_source_after": "0",
            "max_retries": "3"
        }
        
        for k, v in default_settings.items():
            cursor.execute("""
                INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?);
            """, (k, v))

    logger.info("Database schema initialized successfully.")

# --- Settings Operations ---
def get_all_settings() -> Dict[str, str]:
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT key, value FROM settings")
        rows = cursor.fetchall()
        return {row["key"]: row["value"] for row in rows}

def update_settings(settings_dict: Dict[str, str]):
    with get_db() as conn:
        cursor = conn.cursor()
        for k, v in settings_dict.items():
            cursor.execute("""
                INSERT INTO settings (key, value, updated_at) 
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP
            """, (k, str(v)))

# --- Multi-Task Management Operations (Tasks Table) ---
def get_all_tasks() -> List[Dict[str, Any]]:
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM sync_tasks ORDER BY id ASC")
        tasks = [dict(row) for row in cursor.fetchall()]
        
        for t in tasks:
            t_id = t["id"]
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                    SUM(CASE WHEN status = 'transferring' THEN 1 ELSE 0 END) as transferring,
                    SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as error,
                    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
                    SUM(file_size) as total_size,
                    SUM(CASE WHEN status = 'completed' THEN file_size ELSE 0 END) as completed_size
                FROM files WHERE task_id = ?
            """, (t_id,))
            s = cursor.fetchone()
            t["total_files"] = s["total"] or 0
            t["completed_files"] = s["completed"] or 0
            t["transferring_files"] = s["transferring"] or 0
            t["error_files"] = s["error"] or 0
            t["pending_files"] = s["pending"] or 0
            t["total_size"] = s["total_size"] or 0
            t["completed_size"] = s["completed_size"] or 0
            
        return tasks

def get_task_by_id(task_id: int) -> Optional[Dict[str, Any]]:
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM sync_tasks WHERE id = ?", (task_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

def get_task_by_alias(alias: str) -> Optional[Dict[str, Any]]:
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM sync_tasks WHERE alias = ?", (alias.strip().upper(),))
        row = cursor.fetchone()
        return dict(row) if row else None

def parse_iso_datetime(dt_str: Optional[str]) -> Optional[datetime]:
    """Parses ISO-8601 timestamps from rclone into UTC-aware datetime objects."""
    if not dt_str:
        return None
    try:
        s = dt_str.strip().replace("Z", "+00:00")
        if "." in s:
            base, rest = s.split(".", 1)
            tz_idx = -1
            for i, ch in enumerate(rest):
                if ch in ("+", "-"):
                    tz_idx = i
                    break
            if tz_idx != -1:
                micro = rest[:tz_idx][:6].ljust(6, "0")
                tz = rest[tz_idx:]
                s = f"{base}.{micro}{tz}"
            else:
                micro = rest[:6].ljust(6, "0")
                s = f"{base}.{micro}"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is not None:
            return dt.astimezone(timezone.utc)
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None

def parse_filter_datetime(val: Optional[Union[str, datetime]], is_end_of_day: bool = False) -> Optional[datetime]:
    """
    Parses date or datetime strings provided by the user for date filtering.
    - If date only ('YYYY-MM-DD'), sets time to 00:00:00 (or 23:59:59.999999 if is_end_of_day=True).
    - If datetime ('YYYY-MM-DDTHH:mm' or 'YYYY-MM-DD HH:mm:ss'), preserves the time.
    Returns UTC-aware datetime.
    """
    if not val:
        return None
    if isinstance(val, datetime):
        if val.tzinfo is not None:
            return val.astimezone(timezone.utc)
        return val.replace(tzinfo=timezone.utc)
    
    s = str(val).strip()
    if not s:
        return None
    
    # If date only (10 chars, e.g. "2026-09-02")
    if len(s) == 10 and s.count("-") == 2:
        if is_end_of_day:
            s += "T23:59:59.999999"
        else:
            s += "T00:00:00"
            
    s = s.replace(" ", "T").replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is not None:
            return dt.astimezone(timezone.utc)
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(s, fmt)
                if is_end_of_day and fmt == "%Y-%m-%d":
                    dt = dt.replace(hour=23, minute=59, second=59, microsecond=999999)
                return dt.replace(tzinfo=timezone.utc)
            except Exception:
                continue
    return None

def create_task(data: Dict[str, Any]) -> int:
    alias = data.get("alias", "").strip().upper()
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO sync_tasks (
                name, alias, description,
                source_type, source_host, source_port, source_user, source_pass, source_key_file, source_path,
                dest_type, s3_provider, s3_bucket, s3_access_key, s3_secret_key, s3_region, s3_endpoint, s3_path,
                ftp_host, ftp_port, ftp_user, ftp_pass, ftp_path,
                date_from, date_to,
                sync_interval, min_file_age, recursive_scan, auto_start, state
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            str(data.get("name", "") or "").strip() or f"Tarea {alias}",
            alias,
            str(data.get("description", "") or "").strip(),
            str(data.get("source_type", "sftp") or "sftp"),
            str(data.get("source_host", "") or "").strip(),
            str(data.get("source_port", "22") or "22").strip() or "22",
            str(data.get("source_user", "") or "").strip(),
            data.get("source_pass", "") or "",
            str(data.get("source_key_file", "") or "").strip(),
            str(data.get("source_path", "") or "").strip(),
            str(data.get("dest_type", "s3") or "s3"),
            str(data.get("s3_provider", "AWS") or "AWS"),
            str(data.get("s3_bucket", "") or "").strip(),
            str(data.get("s3_access_key", "") or "").strip(),
            data.get("s3_secret_key", "") or "",
            str(data.get("s3_region", "us-east-1") or "us-east-1").strip(),
            str(data.get("s3_endpoint", "") or "").strip(),
            str(data.get("s3_path", "") or "").strip(),
            str(data.get("ftp_host", "") or "").strip(),
            str(data.get("ftp_port", "21") or "21").strip() or "21",
            str(data.get("ftp_user", "") or "").strip(),
            data.get("ftp_pass", "") or "",
            str(data.get("ftp_path", "") or "").strip(),
            str(data.get("date_from", "") or "").strip(),
            str(data.get("date_to", "") or "").strip(),
            int(data.get("sync_interval", 60) or 60),
            int(data.get("min_file_age", 2) or 2),
            1 if str(data.get("recursive_scan", "1")) in ("1", "true", "True") else 0,
            1 if str(data.get("auto_start", "1")) in ("1", "true", "True") else 0,
            "stopped"
        ))
        return cursor.lastrowid

def update_task(task_id: int, data: Dict[str, Any]) -> bool:
    with get_db() as conn:
        cursor = conn.cursor()
        fields = [
            "name", "alias", "description",
            "source_type", "source_host", "source_port", "source_user",
            "source_key_file", "source_path",
            "dest_type", "s3_provider", "s3_bucket", "s3_access_key", "s3_region", "s3_endpoint", "s3_path",
            "ftp_host", "ftp_port", "ftp_user", "ftp_path",
            "date_from", "date_to",
            "sync_interval", "min_file_age", "recursive_scan", "auto_start"
        ]
        updates = []
        params = []
        for f in fields:
            if f in data:
                val = data[f]
                if f == "alias" and val:
                    val = str(val).strip().upper()
                updates.append(f"{f} = ?")
                params.append(val)
                
        if data.get("source_pass") is not None and data.get("source_pass") != "":
            updates.append("source_pass = ?")
            params.append(data["source_pass"])
        if data.get("s3_secret_key") is not None and data.get("s3_secret_key") != "":
            updates.append("s3_secret_key = ?")
            params.append(data["s3_secret_key"])
        if data.get("ftp_pass") is not None and data.get("ftp_pass") != "":
            updates.append("ftp_pass = ?")
            params.append(data["ftp_pass"])
            
        updates.append("updated_at = CURRENT_TIMESTAMP")
        params.append(task_id)
        
        sql = f"UPDATE sync_tasks SET {', '.join(updates)} WHERE id = ?"
        cursor.execute(sql, params)
        
        if "alias" in data and data["alias"]:
            new_alias = data["alias"].strip().upper()
            cursor.execute("UPDATE files SET task_alias = ? WHERE task_id = ?", (new_alias, task_id))

        if "date_from" in data or "date_to" in data:
            cursor.execute("SELECT date_from, date_to FROM sync_tasks WHERE id = ?", (task_id,))
            t_row = cursor.fetchone()
            if t_row:
                d_from = t_row["date_from"]
                d_to = t_row["date_to"]
                dt_from = parse_filter_datetime(d_from, is_end_of_day=False) if d_from else None
                dt_to = parse_filter_datetime(d_to, is_end_of_day=True) if d_to else None
                if dt_from or dt_to:
                    cursor.execute("SELECT id, mod_time FROM files WHERE task_id = ? AND status = 'pending'", (task_id,))
                    pending = cursor.fetchall()
                    to_delete = []
                    for p in pending:
                        m_dt = parse_iso_datetime(p["mod_time"])
                        if not m_dt:
                            to_delete.append(p["id"])
                        elif dt_from and m_dt < dt_from:
                            to_delete.append(p["id"])
                        elif dt_to and m_dt > dt_to:
                            to_delete.append(p["id"])
                    if to_delete:
                        cursor.execute(f"DELETE FROM files WHERE id IN ({','.join(['?']*len(to_delete))})", to_delete)
            
        return cursor.rowcount > 0

def delete_task(task_id: int) -> bool:
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM files WHERE task_id = ?", (task_id,))
        cursor.execute("DELETE FROM sync_tasks WHERE id = ?", (task_id,))
        return cursor.rowcount > 0

def set_task_runtime_state(
    task_id: int, 
    state: str, 
    is_syncing: Optional[bool] = None, 
    current_file: Optional[str] = None
):
    with get_db() as conn:
        cursor = conn.cursor()
        updates = ["state = ?", "updated_at = CURRENT_TIMESTAMP"]
        params = [state]
        if is_syncing is not None:
            updates.append("is_syncing = ?")
            params.append(1 if is_syncing else 0)
        if current_file is not None or is_syncing is False:
            updates.append("current_file = ?")
            params.append(current_file if is_syncing else None)
        params.append(task_id)
        cursor.execute(f"UPDATE sync_tasks SET {', '.join(updates)} WHERE id = ?", params)

def update_task_times(task_id: int, last_run: Optional[str] = None, next_run: Optional[str] = None):
    with get_db() as conn:
        cursor = conn.cursor()
        updates = []
        params = []
        if last_run:
            updates.append("last_run = ?")
            params.append(last_run)
        if next_run:
            updates.append("next_run = ?")
            params.append(next_run)
        if updates:
            params.append(task_id)
            cursor.execute(f"UPDATE sync_tasks SET {', '.join(updates)} WHERE id = ?", params)

def recover_tasks_from_crash() -> Dict[str, Any]:
    interrupted_count = 0
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) as count FROM files WHERE status = 'transferring'")
        row = cursor.fetchone()
        interrupted_count = row["count"] if row else 0
        if interrupted_count > 0:
            cursor.execute("""
                UPDATE files 
                SET status = 'pending', 
                    error_message = 'Interrumpido por reinicio o cierre. Reencolado automáticamente.',
                    updated_at = CURRENT_TIMESTAMP
                WHERE status = 'transferring'
            """)
            
        cursor.execute("UPDATE sync_tasks SET is_syncing = 0, current_file = NULL")
        cursor.execute("""
            SELECT id, alias, name, state, auto_start 
            FROM sync_tasks 
            WHERE auto_start = 1 OR state = 'running'
        """)
        tasks_to_resume = [dict(r) for r in cursor.fetchall()]
        
        return {
            "interrupted_files_recovered": interrupted_count,
            "tasks_to_resume": tasks_to_resume
        }

# --- Contingency & State Recovery Operations ---
def recover_from_crash() -> Dict[str, Any]:
    """
    Called upon application boot.
    1. Interrupted files in 'transferring' state are reset to 'pending'
       so they are retried automatically.
    2. Any broken sync_history record left open is marked 'interrupted'.
    3. Checks if the task was previously 'running' or if auto_start is enabled.
    """
    interrupted_count = 0
    with get_db() as conn:
        cursor = conn.cursor()
        
        # 1. Check transferring files
        cursor.execute("SELECT COUNT(*) as count FROM files WHERE status = 'transferring'")
        row = cursor.fetchone()
        interrupted_count = row["count"] if row else 0
        
        if interrupted_count > 0:
            cursor.execute("""
                UPDATE files 
                SET status = 'pending', 
                    progress_pct = 0.0,
                    error_message = 'Interrumpido por reinicio o cierre del sistema. Reencolado automáticamente.',
                    updated_at = CURRENT_TIMESTAMP
                WHERE status = 'transferring'
            """)
            cursor.execute("""
                INSERT INTO system_logs (level, message, details)
                VALUES (?, ?, ?)
            """, (
                "WARNING", 
                f"Contingencia: Se recuperaron {interrupted_count} archivo(s) interrumpidos por reinicio del sistema.",
                "Los archivos han sido devueltos al estado 'pending' para reanudar el envío."
            ))
            
        # 2. Update open sync history
        cursor.execute("""
            UPDATE sync_history 
            SET status = 'interrupted', 
                finished_at = CURRENT_TIMESTAMP, 
                message = 'Proceso interrumpido inesperadamente (reinicio de máquina o parada forzada)'
            WHERE finished_at IS NULL
        """)

        # 3. Reset is_syncing flag in task_state
        cursor.execute("""
            UPDATE task_state 
            SET is_syncing = 0, current_file = NULL 
            WHERE id = 1
        """)
        
        # 4. Check auto-start configuration & previous state
        cursor.execute("SELECT state FROM task_state WHERE id = 1")
        task_state_row = cursor.fetchone()
        prev_state = task_state_row["state"] if task_state_row else "stopped"
        
        cursor.execute("SELECT value FROM settings WHERE key = 'auto_start'")
        auto_start_row = cursor.fetchone()
        auto_start = (auto_start_row["value"] == "1") if auto_start_row else True
        
        should_auto_resume = auto_start or (prev_state == "running")
        
        return {
            "interrupted_files_recovered": interrupted_count,
            "previous_state": prev_state,
            "auto_start": auto_start,
            "should_auto_resume": should_auto_resume
        }

# --- File Operations ---
def map_task_files(
    task_id: int, 
    task_alias: str, 
    files_data: Optional[List[Dict[str, Any]]] = None,
    discovered_files: Optional[List[Dict[str, Any]]] = None,
    min_age_seconds: int = 120,
    allow_overwrite: bool = False,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None
) -> Tuple[int, int]:
    """
    Inserts newly discovered files for a specific task.
    - If min_age_seconds > 0, skips files created/modified within that threshold
      (e.g. 2 minutes) so interrogator files are not read while being written.
    - If date_from and/or date_to are provided, only files within that range are processed.
    - If allow_overwrite is False (default), existing files in the database are NEVER overwritten.
    Returns (new_files_count, updated_files_count).
    """
    items_to_process = files_data if files_data is not None else (discovered_files or [])
    new_count = 0
    updated_count = 0
    clean_alias = task_alias.strip().upper() if task_alias else None
    now_utc = datetime.now(timezone.utc)
    
    dt_from = parse_filter_datetime(date_from, is_end_of_day=False) if date_from else None
    dt_to = parse_filter_datetime(date_to, is_end_of_day=True) if date_to else None
    
    with get_db() as conn:
        cursor = conn.cursor()
        for item in items_to_process:
            path = item.get("Path")
            name = item.get("Name")
            size = item.get("Size", 0)
            mod_time = item.get("ModTime", "")
            
            if not path:
                continue

            mod_dt = parse_iso_datetime(mod_time) if mod_time else None

            # Check minimum age: ignore files younger than min_age_seconds (e.g. 2 minutes)
            if min_age_seconds > 0 and mod_dt is not None:
                age_seconds = (now_utc - mod_dt).total_seconds()
                if age_seconds < min_age_seconds:
                    # File is currently being written or created within last 2 minutes; skip for now
                    continue

            # Date range filtering (date_from and/or date_to)
            if dt_from is not None or dt_to is not None:
                if mod_dt is None:
                    continue
                if dt_from is not None and mod_dt < dt_from:
                    continue
                if dt_to is not None and mod_dt > dt_to:
                    continue
                
            cursor.execute("SELECT id, file_size, mod_time, status FROM files WHERE task_id = ? AND file_path = ?", (task_id, path))
            existing = cursor.fetchone()
            
            if existing is None:
                cursor.execute("""
                    INSERT INTO files (task_id, task_alias, file_path, file_name, file_size, mod_time, status)
                    VALUES (?, ?, ?, ?, ?, ?, 'pending')
                """, (task_id, clean_alias, path, name, size, mod_time))
                new_count += 1
            else:
                # File already registered. Do NOT overwrite or reset unless allow_overwrite is True
                if allow_overwrite and (existing["file_size"] != size or existing["mod_time"] != mod_time) and existing["status"] == "completed":
                    cursor.execute("""
                        UPDATE files 
                        SET file_size = ?, mod_time = ?, status = 'pending', error_message = NULL, updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """, (size, mod_time, existing["id"]))
                    updated_count += 1
                    
    return new_count, updated_count

def map_discovered_files(files_data: List[Dict[str, Any]], allow_overwrite: bool = True) -> Tuple[int, int]:
    """Legacy singleton mapper for compatibility."""
    return map_task_files(1, "DEFAULT", files_data, min_age_seconds=0, allow_overwrite=allow_overwrite)

def get_pending_task_files(task_id: int, limit: int = 50, min_age_seconds: int = 0) -> List[sqlite3.Row]:
    with get_db() as conn:
        cursor = conn.cursor()
        if min_age_seconds > 0:
            now_utc = datetime.now(timezone.utc)
            cursor.execute("""
                SELECT * FROM files 
                WHERE task_id = ? AND status = 'pending' 
                ORDER BY id ASC
            """, (task_id,))
            rows = cursor.fetchall()
            ready_files = []
            for r in rows:
                mod_time = r["mod_time"]
                if mod_time:
                    mod_dt = parse_iso_datetime(mod_time)
                    if mod_dt:
                        age = (now_utc - mod_dt).total_seconds()
                        if age < min_age_seconds:
                            continue
                ready_files.append(r)
                if len(ready_files) >= limit:
                    break
            return ready_files
        else:
            cursor.execute("""
                SELECT * FROM files 
                WHERE task_id = ? AND status = 'pending' 
                ORDER BY id ASC 
                LIMIT ?
            """, (task_id, limit))
            return cursor.fetchall()

def get_pending_files(limit: int = 50) -> List[sqlite3.Row]:
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM files 
            WHERE status = 'pending' 
            ORDER BY id ASC 
            LIMIT ?
        """, (limit,))
        return cursor.fetchall()

def get_files_by_ids(file_ids: List[int]) -> List[Dict[str, Any]]:
    if not file_ids:
        return []
    placeholders = ",".join(["?"] * len(file_ids))
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM files WHERE id IN ({placeholders})", file_ids)
        return [dict(row) for row in cursor.fetchall()]

def get_all_task_aliases() -> List[Dict[str, Any]]:
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT alias, name FROM (
                SELECT alias, name FROM sync_tasks WHERE alias IS NOT NULL AND alias != ''
                UNION
                SELECT task_alias as alias, task_alias as name FROM files WHERE task_alias IS NOT NULL AND task_alias != ''
            ) ORDER BY alias ASC
        """)
        return [dict(r) for r in cursor.fetchall()]

def set_file_transferring(file_id: int):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE files 
            SET status = 'transferring', 
                progress_pct = 0.0,
                last_attempt_at = CURRENT_TIMESTAMP, 
                attempts = attempts + 1
            WHERE id = ?
        """, (file_id,))

def set_file_completed(file_id: int):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE files 
            SET status = 'completed', 
                progress_pct = 100.0,
                error_message = NULL, 
                completed_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (file_id,))

def set_file_error(file_id: int, error_message: str):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE files 
            SET status = 'error', 
                error_message = ?
            WHERE id = ?
        """, (error_message, file_id))

def retry_file(file_id: int) -> bool:
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE files 
            SET status = 'pending', progress_pct = 0.0, error_message = NULL 
            WHERE id = ?
        """, (file_id,))
        return cursor.rowcount > 0

def retry_all_errors() -> int:
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE files 
            SET status = 'pending', progress_pct = 0.0, error_message = NULL 
            WHERE status = 'error'
        """)
        return cursor.rowcount

def get_files_paginated(
    page: int = 1, 
    per_page: int = 20, 
    status: Optional[str] = None, 
    search: Optional[str] = None,
    task_alias: Optional[str] = None
) -> Dict[str, Any]:
    offset = (page - 1) * per_page
    where_clauses = []
    params = []
    
    if status and status != "all":
        where_clauses.append("status = ?")
        params.append(status)
        
    if task_alias and task_alias != "all":
        where_clauses.append("task_alias = ?")
        params.append(task_alias.strip().upper())

    if search:
        where_clauses.append("(file_path LIKE ? OR file_name LIKE ? OR error_message LIKE ? OR task_alias LIKE ?)")
        search_pattern = f"%{search}%"
        params.extend([search_pattern, search_pattern, search_pattern, search_pattern])
        
    where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Count total
        count_sql = f"SELECT COUNT(*) as total FROM files {where_sql}"
        cursor.execute(count_sql, params)
        total = cursor.fetchone()["total"]
        
        # Select items
        select_sql = f"""
            SELECT * FROM files 
            {where_sql} 
            ORDER BY 
                CASE status 
                    WHEN 'transferring' THEN 1 
                    WHEN 'error' THEN 2 
                    WHEN 'pending' THEN 3 
                    ELSE 4 
                END, 
                id DESC 
            LIMIT ? OFFSET ?
        """
        cursor.execute(select_sql, params + [per_page, offset])
        rows = cursor.fetchall()
        
        items = [dict(row) for row in rows]
        total_pages = (total + per_page - 1) // per_page if total > 0 else 1
        
        return {
            "items": items,
            "total": total,
            "page": page,
            "per_page": per_page,
            "total_pages": total_pages
        }

def get_stats() -> Dict[str, Any]:
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN status = 'transferring' THEN 1 ELSE 0 END) as transferring,
                SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as error,
                SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
                SUM(file_size) as total_size,
                SUM(CASE WHEN status = 'completed' THEN file_size ELSE 0 END) as completed_size
            FROM files
        """)
        row = cursor.fetchone()
        
        # Check active tasks in sync_tasks
        cursor.execute("""
            SELECT 
                COUNT(*) as total_tasks,
                SUM(CASE WHEN state = 'running' THEN 1 ELSE 0 END) as running_tasks,
                SUM(CASE WHEN state = 'paused' THEN 1 ELSE 0 END) as paused_tasks,
                SUM(CASE WHEN is_syncing = 1 THEN 1 ELSE 0 END) as syncing_tasks
            FROM sync_tasks
        """)
        tasks_summary = cursor.fetchone()

        cursor.execute("SELECT current_file FROM sync_tasks WHERE is_syncing = 1 AND current_file IS NOT NULL LIMIT 1")
        active_file_row = cursor.fetchone()
        active_current_file = active_file_row["current_file"] if active_file_row else None

        if tasks_summary and (tasks_summary["total_tasks"] or 0) > 0:
            if tasks_summary["running_tasks"] > 0:
                overall_state = "running"
            elif tasks_summary["paused_tasks"] > 0:
                overall_state = "paused"
            else:
                overall_state = "stopped"
            is_syncing = bool(tasks_summary["syncing_tasks"] > 0)
            current_file = active_current_file
        else:
            # Fallback to legacy task_state
            cursor.execute("SELECT * FROM task_state WHERE id = 1")
            task_row = cursor.fetchone()
            overall_state = task_row["state"] if task_row else "stopped"
            is_syncing = bool(task_row["is_syncing"]) if task_row else False
            current_file = task_row["current_file"] if task_row else None

        cursor.execute("SELECT last_run, next_run FROM sync_tasks ORDER BY updated_at DESC LIMIT 1")
        time_row = cursor.fetchone()
        
        return {
            "total_files": row["total"] or 0,
            "completed": row["completed"] or 0,
            "transferring": row["transferring"] or 0,
            "error": row["error"] or 0,
            "pending": row["pending"] or 0,
            "total_size": row["total_size"] or 0,
            "completed_size": row["completed_size"] or 0,
            "task_state": overall_state,
            "is_syncing": is_syncing,
            "current_file": current_file,
            "last_run": time_row["last_run"] if time_row else None,
            "next_run": time_row["next_run"] if time_row else None
        }

# --- Task State Operations ---
def get_task_state() -> Dict[str, Any]:
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM task_state WHERE id = 1")
        row = cursor.fetchone()
        if row:
            return dict(row)
        return {"state": "stopped", "is_syncing": 0, "current_file": None}

def set_task_state(state: str, is_syncing: Optional[bool] = None, current_file: Optional[str] = None):
    with get_db() as conn:
        cursor = conn.cursor()
        updates = ["state = ?", "updated_at = CURRENT_TIMESTAMP"]
        params = [state]
        
        if is_syncing is not None:
            updates.append("is_syncing = ?")
            params.append(1 if is_syncing else 0)
            
        if current_file is not None or is_syncing is False:
            updates.append("current_file = ?")
            params.append(current_file if is_syncing else None)
            
        params.append(1)
        sql = f"UPDATE task_state SET {', '.join(updates)} WHERE id = ?"
        cursor.execute(sql, params)

def update_task_schedule_times(last_run: Optional[str] = None, next_run: Optional[str] = None):
    with get_db() as conn:
        cursor = conn.cursor()
        updates = []
        params = []
        if last_run:
            updates.append("last_run = ?")
            params.append(last_run)
        if next_run:
            updates.append("next_run = ?")
            params.append(next_run)
            
        if updates:
            params.append(1)
            cursor.execute(f"UPDATE task_state SET {', '.join(updates)} WHERE id = ?", params)

# --- History and System Logs ---
def start_sync_history_record() -> int:
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO sync_history (started_at, status, files_discovered, files_transferred, files_failed, message)
            VALUES (CURRENT_TIMESTAMP, 'running', 0, 0, 0, 'Sincronización en curso...')
        """)
        return cursor.lastrowid

def finish_sync_history_record(
    history_id: int, 
    status: str, 
    discovered: int, 
    transferred: int, 
    failed: int, 
    message: str
):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE sync_history 
            SET finished_at = CURRENT_TIMESTAMP, 
                status = ?, 
                files_discovered = ?, 
                files_transferred = ?, 
                files_failed = ?, 
                message = ?
            WHERE id = ?
        """, (status, discovered, transferred, failed, message, history_id))

def get_sync_history(limit: int = 15) -> List[Dict[str, Any]]:
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM sync_history ORDER BY id DESC LIMIT ?
        """, (limit,))
        return [dict(row) for row in cursor.fetchall()]

def add_log(level: str, message: str, details: Optional[str] = None):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO system_logs (level, message, details)
            VALUES (?, ?, ?)
        """, (level, message, details))

def get_logs(limit: int = 50) -> List[Dict[str, Any]]:
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM system_logs ORDER BY id DESC LIMIT ?
        """, (limit,))
        return [dict(row) for row in cursor.fetchall()]
