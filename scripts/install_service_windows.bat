@echo off
REM ==============================================================================
REM Script para configurar autoinicio tras reinicio en Windows
REM Crea un script VBS silencioso en la carpeta Startup del usuario
REM ==============================================================================
title DataSync Pro - Configurador de Autoinicio en Windows
color 0B

echo ==============================================================================
echo   Configurador de Autoinicio en Segundo Plano para Windows
echo ==============================================================================
echo.

where python >nul 2>nul
if %errorlevel% neq 0 (
    where py >nul 2>nul
    if %errorlevel% neq 0 (
        echo [ERROR] Python no encontrado en el PATH.
        pause
        exit /b 1
    ) else (
        set PYTHON_CMD=py
    )
) else (
    set PYTHON_CMD=python
)

cd /d "%~dp0\.."
set APP_DIR=%CD%
set STARTUP_FOLDER=%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup
set VBS_FILE=%STARTUP_FOLDER%\DataSyncPro_AutoStart.vbs

echo Directorio de la aplicacion: %APP_DIR%
echo Carpeta de inicio de Windows: %STARTUP_FOLDER%

echo Creando script de inicio silencioso en %VBS_FILE% ...

(
echo Set WshShell = CreateObject^("WScript.Shell"^)
echo WshShell.CurrentDirectory = "%APP_DIR%"
echo WshShell.Run """%PYTHON_CMD%"" ""%APP_DIR%\main.py""", 0, False
) > "%VBS_FILE%"

if %errorlevel% equ 0 (
    echo.
    echo ==============================================================================
    echo   [OK] Autoinicio configurado con exito.
    echo   La aplicacion arrancara de forma invisible cada vez que inicies sesion
    echo   o reinicies tu equipo con Windows.
    echo ==============================================================================
) else (
    echo [ERROR] No se pudo escribir en la carpeta de inicio.
)

echo.
pause

