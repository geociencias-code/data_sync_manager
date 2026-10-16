@echo off
REM ==============================================================================
REM Lanzador de la aplicación para Windows con Entorno Virtual
REM ==============================================================================
title DataSync Manager - SFTP to S3/FTP Server
color 0A

cd /d "%~dp0"

if exist "venv\Scripts\python.exe" (
    set PYTHON_CMD=venv\Scripts\python.exe
) else (
    where python >nul 2>nul
    if %errorlevel% neq 0 (
        where py >nul 2>nul
        if %errorlevel% neq 0 (
            echo [ERROR] No se encontro Python en el PATH. Ejecuta install.bat primero.
            pause
            exit /b 1
        ) else (
            set PYTHON_CMD=py
        )
    ) else (
        set PYTHON_CMD=python
    )
)

echo ==============================================================================
echo   Iniciando DataSync Manager en Windows (%PYTHON_CMD%)...
echo   Servidor Web: http://localhost:8000
echo ==============================================================================
echo.

%PYTHON_CMD% main.py

if %errorlevel% neq 0 (
    echo.
    echo [AVISO] El servidor se ha detenido.
    pause
)
