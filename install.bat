@echo off
REM ==============================================================================
REM Instalador automático para Windows (Command Prompt / Doble Clic)
REM ==============================================================================
title DataSync Manager - Instalador de Dependencias
color 0B

echo ==============================================================================
echo   Iniciando Instalador Automatico de DataSync Manager para Windows
echo ==============================================================================
echo.

where python >nul 2>nul
if %errorlevel% neq 0 (
    where py >nul 2>nul
    if %errorlevel% neq 0 (
        color 0C
        echo [ERROR] No se encontro Python instalado en este equipo o no esta en el PATH.
        echo Por favor instala Python 3 marcando la casilla "Add Python to PATH":
        echo https://www.python.org/downloads/
        echo.
        pause
        exit /b 1
    ) else (
        set PYTHON_CMD=py
    )
) else (
    set PYTHON_CMD=python
)

echo Usando interprete: %PYTHON_CMD%
echo Descargando dependencias de Python y configurando Rclone...
echo.

%PYTHON_CMD% setup_environment.py

if %errorlevel% neq 0 (
    color 0C
    echo.
    echo [ERROR] Ocurrio un error durante la instalacion.
    pause
    exit /b %errorlevel%
)

echo.
echo Presiona cualquier tecla para salir...
pause >nul

