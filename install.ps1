# ==============================================================================
# Instalador automático para Windows PowerShell
# ==============================================================================
Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host "  Iniciando Instalador Automatico de DataSync Pro (PowerShell)    " -ForegroundColor Cyan
Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host ""

$pythonCmd = $null
if (Get-Command python -ErrorAction SilentlyContinue) {
    $pythonCmd = "python"
} elseif (Get-Command py -ErrorAction SilentlyContinue) {
    $pythonCmd = "py"
} elseif (Get-Command python3 -ErrorAction SilentlyContinue) {
    $pythonCmd = "python3"
}

if (-not $pythonCmd) {
    Write-Host "[ERROR] No se encontro Python en el PATH del sistema." -ForegroundColor Red
    Write-Host "Por favor instala Python 3 marcando 'Add Python to PATH' desde https://www.python.org/downloads/" -ForegroundColor Yellow
    exit 1
}

Write-Host "[OK] Utilizando intérprete: $pythonCmd" -ForegroundColor Green
Write-Host "[INFO] Ejecutando setup_environment.py..." -ForegroundColor Cyan
& $pythonCmd setup_environment.py

if ($LASTEXITCODE -eq 0) {
    Write-Host "`n[EXITO] Instalación completada. Puedes iniciar la app ejecutando: .\start.bat o python main.py" -ForegroundColor Green
} else {
    Write-Host "`n[ERROR] Falló la instalación con código de salida: $LASTEXITCODE" -ForegroundColor Red
    Write-Host "[TIP] Si el error fue al descargar Rclone, puedes instalarlo fácilmente en Windows mediante:" -ForegroundColor Yellow
    Write-Host "      winget install Rclone.Rclone" -ForegroundColor Cyan
}

