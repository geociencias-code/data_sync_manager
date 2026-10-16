import os
from pathlib import Path

# Base Paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
SCRIPTS_DIR = BASE_DIR / "scripts"
BIN_DIR = BASE_DIR / "bin"

# Ensure directories exist
DATA_DIR.mkdir(parents=True, exist_ok=True)
BIN_DIR.mkdir(parents=True, exist_ok=True)

# Add BIN_DIR to PATH so local rclone executable is found on Windows, Mac, and Linux
current_path = os.environ.get("PATH", "")
if str(BIN_DIR) not in current_path:
    os.environ["PATH"] = f"{str(BIN_DIR)}{os.pathsep}{current_path}"

# Database & Rclone files
DB_NAME = os.environ.get("SYNC_DB_NAME", "sync_data.db")
DB_PATH = DATA_DIR / DB_NAME
RCLONE_CONF_PATH = DATA_DIR / "rclone.conf"

# App directories
TEMPLATES_DIR = BASE_DIR / "app" / "templates"
STATIC_DIR = BASE_DIR / "app" / "static"

# Default scheduler config
DEFAULT_SYNC_INTERVAL = 60  # seconds
DEFAULT_AUTO_START = True

from typing import List, Dict, Any

def get_lan_ip() -> str:
    """Returns the primary LAN IP address of the host machine for network access."""
    import socket
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"

def get_tailscale_ip():
    """Detects Tailscale IPv4 address if Tailscale is active on the host machine."""
    import subprocess
    import re
    # 1. Try tailscale CLI
    for ts_path in ["/Applications/Tailscale.app/Contents/MacOS/Tailscale", "tailscale"]:
        try:
            res = subprocess.run([ts_path, "ip", "-4"], capture_output=True, text=True, timeout=1)
            if res.returncode == 0 and res.stdout.strip():
                return res.stdout.strip().split("\n")[0]
        except Exception:
            pass
    # 2. Try regex in ifconfig/ipconfig for 100.x.y.z
    try:
        cmd = ["ifconfig"] if os.name != "nt" else ["ipconfig"]
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=1)
        m = re.search(r"(?:inet|IPv4[^\d]*)\s*(100\.\d{1,3}\.\d{1,3}\.\d{1,3})", res.stdout)
        if m:
            return m.group(1)
    except Exception:
        pass
    return None

def get_all_network_interfaces() -> List[Dict[str, Any]]:
    """
    Discovers all active network IPv4 addresses on the host machine across macOS, Linux, and Windows.
    Returns a structured list with interface names, IPs, categories, and direct browser URLs.
    """
    import subprocess
    import re

    port = int(os.getenv("PORT", 8000))
    results = []
    seen_ips = set()

    if os.name == 'nt':
        try:
            out = subprocess.check_output(['ipconfig'], text=True, errors='ignore', timeout=2)
            current_adapter = 'Adaptador'
            for line in out.splitlines():
                line_str = line.strip()
                if 'adapter' in line_str.lower():
                    current_adapter = line_str.split(':')[0].replace('Ethernet adapter', '').replace('Wireless LAN adapter', '').strip()
                m = re.search(r'IPv4[^\d]*:\s*([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)', line_str)
                if m:
                    ip = m.group(1)
                    if ip not in seen_ips:
                        seen_ips.add(ip)
                        results.append({'interface': current_adapter, 'ip': ip})
        except Exception:
            pass
    else:
        # macOS / Linux (ifconfig)
        try:
            out = subprocess.check_output(['ifconfig'], text=True, errors='ignore', timeout=2)
            current_if = ''
            for line in out.splitlines():
                if line and not line.startswith('\t') and not line.startswith(' '):
                    current_if = line.split(':')[0].strip()
                m = re.search(r'inet\s+([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)', line)
                if m:
                    ip = m.group(1)
                    if ip not in seen_ips:
                        seen_ips.add(ip)
                        results.append({'interface': current_if, 'ip': ip})
        except Exception:
            # Fallback for Linux without ifconfig: ip -4 addr
            try:
                out = subprocess.check_output(['ip', '-4', 'addr'], text=True, errors='ignore', timeout=2)
                for line in out.splitlines():
                    m = re.search(r'inet\s+([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)/\d+\s+.*?\s+(\S+)$', line.strip())
                    if m:
                        ip = m.group(1)
                        iface = m.group(2)
                        if ip not in seen_ips:
                            seen_ips.add(ip)
                            results.append({'interface': iface, 'ip': ip})
            except Exception:
                pass

    # Ensure 127.0.0.1 is present
    if "127.0.0.1" not in seen_ips:
        results.append({'interface': 'lo0', 'ip': '127.0.0.1'})
        seen_ips.add("127.0.0.1")

    # Order: Local LAN/Wi-Fi first, then VPN/Tailscale, then Localhost
    enriched = []
    for item in results:
        ip = item['ip']
        iface = item['interface']
        if ip.startswith("127."):
            label = "Acceso Local (Este equipo)"
            category = "Local"
            icon = "laptop"
            sort_order = 3
        elif ip.startswith("100.") or "tailscale" in iface.lower() or "utun" in iface.lower() or "vpn" in iface.lower():
            label = f"VPN / Tailscale ({iface})"
            category = "VPN"
            icon = "shield"
            sort_order = 2
        elif ip.startswith("192.168.") or ip.startswith("10.") or ip.startswith("172."):
            label = f"Red Local LAN / Wi-Fi ({iface})"
            category = "LAN"
            icon = "wifi"
            sort_order = 1
        else:
            label = f"Red ({iface})"
            category = "Red"
            icon = "globe"
            sort_order = 2

        enriched.append({
            "interface": iface,
            "ip": ip,
            "label": label,
            "category": category,
            "icon": icon,
            "url": f"http://{ip}:{port}",
            "sort_order": sort_order
        })

    enriched.sort(key=lambda x: x["sort_order"])
    return enriched
