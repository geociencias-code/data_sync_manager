#!/usr/bin/env bash
# ==============================================================================
# Script de inicio para DataSync Manager con Entorno Virtual
# ==============================================================================
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
cd "$DIR"

# Determinar el ejecutable de Python (preferir venv)
if [ -f "$DIR/venv/bin/python" ]; then
    PYTHON_EXEC="$DIR/venv/bin/python"
elif [ -f "$DIR/venv/Scripts/python.exe" ]; then
    PYTHON_EXEC="$DIR/venv/Scripts/python.exe"
else
    echo "[AVISO] No se encontró el entorno virtual 'venv/'. Ejecutando instalación inicial..."
    ./install.sh
    PYTHON_EXEC="$DIR/venv/bin/python"
fi

export HOST="${HOST:-0.0.0.0}"
export PORT="${PORT:-8000}"

echo "Iniciando DataSync Manager con: $PYTHON_EXEC ..."
exec "$PYTHON_EXEC" main.py
