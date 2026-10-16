#!/usr/bin/env python3
"""
Automated Setup & Dependency Downloader for DataSync Pro
Compatible with macOS, Linux, and Windows.

Tasks performed:
1. Detects OS and hardware architecture.
2. Installs required Python dependencies from requirements.txt.
3. Checks for Rclone; if missing, automatically downloads and extracts the official
   binary for the current OS/architecture into the local bin/ folder.
4. Initializes the SQLite database and directory structure.
5. Verifies installation and provides launch instructions.
"""

import os
import sys
import platform
import shutil
import subprocess
import urllib.request
import zipfile
import tempfile
from pathlib import Path

# Base Paths
BASE_DIR = Path(__file__).resolve().parent
BIN_DIR = BASE_DIR / "bin"
DATA_DIR = BASE_DIR / "data"
REQ_FILE = BASE_DIR / "requirements.txt"
VENV_DIR = BASE_DIR / "venv"

# ANSI Colors for terminal output
GREEN = "\033[92m" if sys.platform != "win32" or "WT_SESSION" in os.environ else ""
BLUE = "\033[94m" if sys.platform != "win32" or "WT_SESSION" in os.environ else ""
YELLOW = "\033[93m" if sys.platform != "win32" or "WT_SESSION" in os.environ else ""
RED = "\033[91m" if sys.platform != "win32" or "WT_SESSION" in os.environ else ""
BOLD = "\033[1m" if sys.platform != "win32" or "WT_SESSION" in os.environ else ""
RESET = "\033[0m" if sys.platform != "win32" or "WT_SESSION" in os.environ else ""

def get_venv_python() -> Path:
    if sys.platform == "win32":
        return VENV_DIR / "Scripts" / "python.exe"
    return VENV_DIR / "bin" / "python"

def is_inside_venv() -> bool:
    return getattr(sys, "base_prefix", sys.prefix) != sys.prefix or os.environ.get("VIRTUAL_ENV") is not None

def ensure_virtualenv():
    """
    Ensures a clean virtual environment exists to prevent PEP 668 externally-managed-environment
    errors on Homebrew, Debian, Ubuntu, etc.
    If not running inside the venv, it creates it and re-launches itself inside it.
    """
    if is_inside_venv():
        return
        
    venv_python = get_venv_python()
    if not venv_python.exists():
        log_info(f"Creando entorno virtual Python en {VENV_DIR}...")
        try:
            subprocess.run([sys.executable, "-m", "venv", str(VENV_DIR)], check=True)
            log_success("Entorno virtual 'venv/' creado exitosamente.")
        except Exception as e:
            log_error(f"Error al crear el entorno virtual: {e}")
            sys.exit(1)
            
    # Re-exec this script using the venv's python
    log_info(f"Re-ejecutando en el entorno virtual ({venv_python})...\n")
    res = subprocess.run([str(venv_python), str(Path(__file__).resolve())] + sys.argv[1:])
    sys.exit(res.returncode)

def log_info(msg):
    print(f"{BLUE}[INFO]{RESET} {msg}")

def log_success(msg):
    print(f"{GREEN}[OK]{RESET} {BOLD}{msg}{RESET}")

def log_warn(msg):
    print(f"{YELLOW}[AVISO]{RESET} {msg}")

def log_error(msg):
    print(f"{RED}[ERROR]{RESET} {msg}")

def get_platform_info():
    system = platform.system().lower()
    machine = platform.machine().lower()
    
    # Normalize OS name
    if "darwin" in system:
        os_name = "darwin"
    elif "windows" in system or system.startswith("win"):
        os_name = "windows"
    else:
        os_name = "linux"
    
    # Normalize Architecture
    if machine in ["x86_64", "amd64"]:
        arch = "amd64"
    elif machine in ["arm64", "aarch64"]:
        arch = "arm64"
    elif machine in ["i386", "i686", "x86"]:
        arch = "386"
    elif "armv7" in machine or "armv6" in machine:
        arch = "arm-v7"
    else:
        arch = "amd64" # Default fallback
        
    return os_name, arch

def get_rclone_download_url(os_name, arch):
    """
    Returns the official rclone download URL from downloads.rclone.org
    """
    if os_name == "darwin":
        # macOS
        if arch == "arm64":
            return "https://downloads.rclone.org/rclone-current-osx-arm64.zip", "osx-arm64"
        else:
            return "https://downloads.rclone.org/rclone-current-osx-amd64.zip", "osx-amd64"
    elif os_name == "windows":
        # Windows
        if arch == "arm64":
            return "https://downloads.rclone.org/rclone-current-windows-arm64.zip", "windows-arm64"
        elif arch == "386":
            return "https://downloads.rclone.org/rclone-current-windows-386.zip", "windows-386"
        else:
            return "https://downloads.rclone.org/rclone-current-windows-amd64.zip", "windows-amd64"
    else:
        # Linux
        if arch == "arm64":
            return "https://downloads.rclone.org/rclone-current-linux-arm64.zip", "linux-arm64"
        elif arch == "arm-v7":
            return "https://downloads.rclone.org/rclone-current-linux-arm-v7.zip", "linux-arm-v7"
        else:
            return "https://downloads.rclone.org/rclone-current-linux-amd64.zip", "linux-amd64"

def step_install_python_dependencies():
    log_info("Instalando dependencias de Python (FastAPI, Uvicorn, APScheduler, etc.)...")
    if not REQ_FILE.exists():
        log_error(f"No se encontró el archivo {REQ_FILE}")
        return False
        
    cmd = [sys.executable, "-m", "pip", "install", "-r", str(REQ_FILE), "--upgrade"]
    try:
        subprocess.run(cmd, check=True)
        log_success("Dependencias de Python instaladas correctamente.")
        return True
    except subprocess.CalledProcessError as e:
        log_error(f"Error al instalar dependencias de Python: {e}")
        return False

def check_rclone_available():
    """Checks if rclone is available in local bin/, system PATH, or known OS locations."""
    is_windows = sys.platform == "win32" or "windows" in platform.system().lower()
    exe_name = "rclone.exe" if is_windows else "rclone"
    
    # 1. Project local bin/
    local_bin = BIN_DIR / exe_name
    if local_bin.exists():
        return str(local_bin)
        
    # 2. System PATH
    system_rclone = shutil.which("rclone") or shutil.which("rclone.exe")
    if system_rclone:
        return system_rclone
        
    # 3. Common Windows install locations (Winget, Scoop, Choco, Program Files)
    if is_windows:
        home = Path.home()
        known_windows_paths = [
            home / "AppData" / "Local" / "Microsoft" / "WinGet" / "Links" / "rclone.exe",
            Path("C:/Program Files/Rclone/rclone.exe"),
            Path("C:/rclone/rclone.exe"),
            home / "scoop" / "shims" / "rclone.exe",
            Path("C:/ProgramData/chocolatey/bin/rclone.exe"),
        ]
        for p in known_windows_paths:
            if p.exists():
                return str(p)
                
    return None

def download_rclone_zip(url, target_zip):
    """
    Downloads file from url to target_zip using custom headers to avoid
    HTTP 403 Forbidden from Cloudflare / downloads.rclone.org, with fallbacks.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "*/*"
    }

    # Method 1: Python urllib with browser User-Agent
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=60) as resp, open(target_zip, "wb") as f:
            total_size = int(resp.info().get("Content-Length", -1))
            downloaded = 0
            chunk_size = 64 * 1024
            while True:
                chunk = resp.read(chunk_size)
                if not chunk:
                    break
                f.write(chunk)
                downloaded += len(chunk)
                if total_size > 0:
                    pct = int(downloaded * 100 / total_size)
                    mb_down = downloaded / (1024 * 1024)
                    mb_tot = total_size / (1024 * 1024)
                    print(f"\rDescargando Rclone... {pct}% ({mb_down:.1f}MB / {mb_tot:.1f}MB)", end="", flush=True)
                else:
                    print(f"\rDescargando Rclone... {downloaded // 1024} KB", end="", flush=True)
            print(" completado.")
            return True
    except Exception as e:
        print()
        log_warn(f"Descarga directa vía Python falló ({e}). Intentando con utilidades del sistema...")

    # Method 2: curl fallback (built into Windows 10/11 and Linux/macOS)
    curl_bin = shutil.which("curl") or shutil.which("curl.exe")
    if curl_bin:
        try:
            print("Descargando archivo zip con curl...", end="", flush=True)
            cmd = [curl_bin, "-fSL", "-A", headers["User-Agent"], "-o", str(target_zip), url]
            subprocess.run(cmd, check=True)
            print(" completado.")
            return True
        except Exception as e:
            print()
            log_warn(f"Descarga con curl falló: {e}")

    # Method 3: PowerShell fallback on Windows
    if sys.platform == "win32" or "windows" in platform.system().lower():
        try:
            print("Descargando archivo zip con PowerShell...", end="", flush=True)
            ps_cmd = f"$ProgressPreference = 'SilentlyContinue'; Invoke-WebRequest -Uri '{url}' -OutFile '{target_zip}' -UserAgent '{headers['User-Agent']}'"
            subprocess.run(["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", ps_cmd], check=True)
            print(" completado.")
            return True
        except Exception as e:
            print()
            log_warn(f"Descarga con PowerShell falló: {e}")

    return False

def step_install_rclone():
    existing_rclone = check_rclone_available()
    if existing_rclone:
        try:
            res = subprocess.run([existing_rclone, "version"], capture_output=True, text=True)
            first_line = res.stdout.strip().split("\n")[0]
            log_success(f"Rclone ya se encuentra instalado y listo: {first_line} ({existing_rclone})")
            return True
        except Exception:
            pass

    os_name, arch = get_platform_info()
    url, pkg_id = get_rclone_download_url(os_name, arch)
    log_warn(f"Rclone no encontrado en el sistema. Descargando versión oficial para {os_name.upper()} ({arch})...")
    log_info(f"URL de descarga: {url}")

    BIN_DIR.mkdir(parents=True, exist_ok=True)
    exe_name = "rclone.exe" if os_name == "windows" else "rclone"
    target_bin = BIN_DIR / exe_name

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_zip = Path(tmp_dir) / "rclone.zip"
        
        # Download with progress and custom headers
        if not download_rclone_zip(url, tmp_zip):
            log_error(f"No fue posible descargar Rclone desde {url}.")
            
            # Windows fallback: winget
            if os_name == "windows" and (shutil.which("winget") or shutil.which("winget.exe")):
                log_info("Intentando instalación automática mediante Windows Package Manager (winget)...")
                try:
                    subprocess.run(["winget", "install", "Rclone.Rclone", "--accept-source-agreements", "--accept-package-agreements", "--silent"], check=True)
                    found = check_rclone_available()
                    if found:
                        log_success(f"Rclone instalado con éxito vía winget: {found}")
                        return True
                except Exception as we:
                    log_warn(f"Winget falló: {we}")
            return False

        # Extract executable from zip
        try:
            with zipfile.ZipFile(str(tmp_zip), 'r') as zip_ref:
                target_member = None
                for member in zip_ref.namelist():
                    if member.endswith("/" + exe_name) or member == exe_name:
                        target_member = member
                        break
                        
                if not target_member:
                    log_error(f"No se encontró el ejecutable '{exe_name}' dentro del archivo comprimido.")
                    return False
                    
                zip_ref.extract(target_member, tmp_dir)
                extracted_exe = Path(tmp_dir) / target_member
                
                # Move to bin/
                if target_bin.exists():
                    target_bin.unlink()
                shutil.copy2(str(extracted_exe), str(target_bin))
                
            # Make executable on Unix
            if os_name != "windows":
                target_bin.chmod(0o755)

            # Test extracted binary
            res = subprocess.run([str(target_bin), "version"], capture_output=True, text=True, check=True)
            ver = res.stdout.strip().split("\n")[0]
            log_success(f"Rclone instalado con éxito en {target_bin}: {ver}")
            return True
        except Exception as e:
            log_error(f"Error al extraer o configurar Rclone: {e}")
            return False

def step_init_database():
    log_info("Inicializando esquema de base de datos SQLite con soporte WAL...")
    try:
        if str(BASE_DIR) not in sys.path:
            sys.path.insert(0, str(BASE_DIR))
        from app import database
        database.init_db()
        log_success("Base de datos SQLite inicializada correctamente en data/sync_data.db.")
        return True
    except Exception as e:
        log_error(f"Error inicializando base de datos: {e}")
        return False

def main():
    print(f"\n{BOLD}================================================================{RESET}")
    print(f"{BOLD}  🚀 ASISTENTE DE INSTALACIÓN Y CONFIGURACIÓN AUTOMÁTICA{RESET}")
    print(f"{BOLD}     DataSync Pro: SFTP ➔ S3 / FTP Data Sync Manager{RESET}")
    print(f"{BOLD}================================================================{RESET}\n")

    # Step 0: Ensure running inside a dedicated virtual environment (PEP 668 compliance)
    ensure_virtualenv()

    os_name, arch = get_platform_info()
    log_info(f"Sistema Operativo detectado: {BOLD}{os_name.upper()}{RESET} ({arch})")
    log_info(f"Entorno Virtual Python activo: {BOLD}{sys.prefix}{RESET}")
    log_info(f"Versión de Python: {platform.python_version()} ({sys.executable})")

    # Step 1: Python Dependencies
    if not step_install_python_dependencies():
        log_error("Fallo la instalación de dependencias de Python.")
        sys.exit(1)

    # Step 2: Rclone Binary
    if not step_install_rclone():
        log_error("Fallo la configuración de Rclone.")
        sys.exit(1)

    # Step 3: SQLite Database
    if not step_init_database():
        log_error("Fallo la inicialización de la base de datos.")
        sys.exit(1)

    print(f"\n{GREEN}{BOLD}================================================================{RESET}")
    print(f"{GREEN}{BOLD}  ✅ ¡INSTALACIÓN COMPLETADA EXITOSAMENTE!{RESET}")
    print(f"{GREEN}{BOLD}================================================================{RESET}\n")

    print(f"Para iniciar la aplicación:")
    if sys.platform == "win32":
        print(f"  • Ejecuta el archivo: {BOLD}start.bat{RESET} o escribe: {BOLD}python main.py{RESET}")
    else:
        print(f"  • Ejecuta el script:  {BOLD}./scripts/start.sh{RESET} o escribe: {BOLD}python3 main.py{RESET}")

    print(f"\nLuego abre tu navegador en: {BOLD}{BLUE}http://localhost:8000{RESET}\n")

if __name__ == "__main__":
    main()
