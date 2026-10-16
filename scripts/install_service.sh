#!/bin/bash
# Script para configurar el servicio de autoinicio del sistema (macOS LaunchAgent / Linux Systemd)
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
if [ -f "$DIR/venv/bin/python" ]; then
    PYTHON_EXEC="$DIR/venv/bin/python"
elif [ -f "$DIR/venv/Scripts/python.exe" ]; then
    PYTHON_EXEC="$DIR/venv/Scripts/python.exe"
else
    PYTHON_EXEC="$(which python3 || which python)"
fi
OS="$(uname -s)"

echo "Configurando servicio de inicio automático para resiliencia ante reinicios..."
echo "Sistema detectado: $OS"
echo "Directorio de la aplicación: $DIR"
echo "Intérprete Python utilizado: $PYTHON_EXEC"

if [ "$OS" = "Darwin" ]; then
    # macOS LaunchAgent
    PLIST_DIR="$HOME/Library/LaunchAgents"
    PLIST_FILE="$PLIST_DIR/com.syncdata.sftp_s3.plist"
    
    mkdir -p "$PLIST_DIR"
    mkdir -p "$DIR/data"

    cat <<EOF > "$PLIST_FILE"
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.syncdata.sftp_s3</string>
    <key>ProgramArguments</key>
    <array>
        <string>$PYTHON_EXEC</string>
        <string>$DIR/main.py</string>
    </array>
    <key>WorkingDirectory</key>
    <string>$DIR</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>$DIR/data/service.log</string>
    <key>StandardErrorPath</key>
    <string>$DIR/data/service_error.log</string>
</dict>
</plist>
EOF

    echo "Cargando servicio con launchctl..."
    launchctl unload "$PLIST_FILE" 2>/dev/null || true
    launchctl load "$PLIST_FILE"
    echo "¡Servicio registrado con éxito en macOS!"
    echo "Ubicación: $PLIST_FILE"
    echo "El sincronizador se iniciará automáticamente cada vez que inicies sesión / enciendas la máquina."

elif [ "$OS" = "Linux" ]; then
    # Linux Systemd Service
    SERVICE_FILE="/etc/systemd/system/syncdata-sftp-s3.service"
    
    echo "Generando archivo de unidad systemd..."
    cat <<EOF | sudo tee "$SERVICE_FILE" > /dev/null
[Unit]
Description=SFTP to S3/FTP Data Sync Manager
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$DIR
ExecStart=$PYTHON_EXEC $DIR/main.py
Restart=always
RestartSec=10
StandardOutput=append:$DIR/data/service.log
StandardError=append:$DIR/data/service_error.log

[Install]
WantedBy=multi-user.target
EOF

    echo "Habilitando servicio con systemctl..."
    sudo systemctl daemon-reload
    sudo systemctl enable syncdata-sftp-s3.service
    sudo systemctl start syncdata-sftp-s3.service
    echo "¡Servicio systemd habilitado y arrancado en Linux!"
else
    echo "Sistema operativo no reconocido directamente. Consulta la documentación para registrarlo como servicio."
fi

