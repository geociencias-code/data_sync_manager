#!/usr/bin/env bash
# ==============================================================================
# Instalador automático para macOS y Linux con Entorno Virtual (PEP 668)
# ==============================================================================
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR"

echo "=================================================================="
echo "  Iniciando Instalador Automático de DataSync Manager (Linux/macOS)"
echo "=================================================================="
echo "Verificando instalación de Python 3..."
if command -v python3 &>/dev/null; then
    SYS_PYTHON="python3"
elif command -v python &>/dev/null; then
    SYS_PYTHON="python"
else
    echo "ERROR: No se encontró Python 3 instalado en este sistema."
    echo "Por favor instala Python 3 desde https://www.python.org/downloads/ o con tu gestor de paquetes."
    exit 1
fi

# Crear entorno virtual si no existe
VENV_DIR="$DIR/venv"
VENV_PYTHON="$VENV_DIR/bin/python"

if [ ! -f "$VENV_PYTHON" ]; then
    echo "Creando entorno virtual aislado en $VENV_DIR ..."
    "$SYS_PYTHON" -m venv "$VENV_DIR"
    echo "Entorno virtual creado exitosamente."
fi

echo "Ejecutando asistente usando el entorno virtual ($VENV_PYTHON)..."
exec "$VENV_PYTHON" setup_environment.py
