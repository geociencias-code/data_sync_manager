"""
Demonstration script showing end-to-end sync workflow using local mock remotes with rclone.
"""
import os
import sys
import shutil
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app import database
from app.rclone_manager import RcloneManager
from app.sync_engine import sync_engine

BASE_DIR = Path(__file__).resolve().parent.parent
MOCK_DIR = BASE_DIR / "data" / "demo_mock"
SRC_DIR = MOCK_DIR / "mock_sftp_inbox"
DST_DIR = MOCK_DIR / "mock_s3_bucket"

def run_demo():
    print("\n--- INICIANDO SIMULACIÓN END-TO-END ---")
    
    # 1. Prepare demo directories
    if MOCK_DIR.exists():
        shutil.rmtree(MOCK_DIR)
    SRC_DIR.mkdir(parents=True, exist_ok=True)
    DST_DIR.mkdir(parents=True, exist_ok=True)
    
    # Create sample files in mock source
    (SRC_DIR / "datos_ventas_2026.csv").write_text("id,producto,monto\n1,Laptop,1200\n2,Mouse,25\n")
    (SRC_DIR / "reporte_auditoria.json").write_text('{"estado": "aprobado", "auditor": "Admin"}')
    (SRC_DIR / "usuarios.xml").write_text('<usuarios><user>Carlos</user></usuarios>')
    
    sub = SRC_DIR / "subcarpeta"
    sub.mkdir()
    (sub / "datos_historicos.txt").write_text("Registros 2020-2026")
    
    print(f"✓ Creados 4 archivos de prueba en mock source: {SRC_DIR}")
    
    # 2. Map files into SQLite directly via lsjson using rclone
    import subprocess, json
    res = subprocess.run(["rclone", "lsjson", "-R", str(SRC_DIR)], capture_output=True, text=True, check=True)
    files_data = json.loads(res.stdout)
    new_c, upd_c = database.map_discovered_files(files_data)
    print(f"✓ Mapeados en SQLite: {new_c} archivos nuevos.")
    
    # Check stats
    stats = database.get_stats()
    print(f"✓ Estado en BD: {stats['total_files']} totales, {stats['pending']} pendientes.")
    
    # 3. Simulate file transfers using rclone copyto
    pending = database.get_pending_files(limit=10)
    for f in pending:
        file_path = f["file_path"]
        src_file = SRC_DIR / file_path
        dst_file = DST_DIR / file_path
        
        database.set_file_transferring(f["id"])
        print(f"  -> Enviándose: {file_path} ...")
        
        # Run rclone copyto
        dst_file.parent.mkdir(parents=True, exist_ok=True)
        transfer_proc = subprocess.run(["rclone", "copyto", str(src_file), str(dst_file)], capture_output=True, text=True)
        if transfer_proc.returncode == 0:
            database.set_file_completed(f["id"])
            print(f"  ✓ Completado: {file_path}")
        else:
            database.set_file_error(f["id"], transfer_proc.stderr)
            print(f"  ✗ Error: {file_path} -> {transfer_proc.stderr}")
            
    # 4. Final verify
    stats_after = database.get_stats()
    print(f"\n✓ Verificación Final en SQLite:")
    print(f"  Total: {stats_after['total_files']}")
    print(f"  Completados: {stats_after['completed']}")
    print(f"  Pendientes: {stats_after['pending']}")
    print(f"  Con Error: {stats_after['error']}")
    print(f"  Archivos en destino: {len(list(DST_DIR.rglob('*')))} items.")
    print("--- SIMULACIÓN EXITOSA ---\n")

if __name__ == "__main__":
    database.init_db()
    run_demo()
