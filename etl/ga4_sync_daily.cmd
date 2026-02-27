@echo off
setlocal
cd /d "%~dp0.."

chcp 65001 >nul
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8

REM --- Defaults SOLO si no vienen de cmd\_env_neon.local.cmd ---
if not defined NEON_CONNECTION_STRING set "NEON_CONNECTION_STRING=Host=localhost;Port=5433;Username=postgres;Password=postgres;Database=chiru_local"
if not defined GA4_PROPERTY_ID set "GA4_PROPERTY_ID=513497372"
if not defined GA4_CLIENT set "GA4_CLIENT=C:\Users\%USERNAME%\secrets\ga4-oauth-client.json"
if not defined GA4_TOKEN set "GA4_TOKEN=C:\Users\%USERNAME%\secrets\ga4-token.json"

set "LOG=etl\ga4_sync.log"
echo ==== GA4 SYNC START %DATE% %TIME% ====>> "%LOG%"

where python >nul 2>&1 || (
  echo [ERROR] python no encontrado. Instala Python 3.11+ y reinicia la terminal.
  echo [ERROR] python no encontrado>> "%LOG%"
  exit /b 9009
)

python -X utf8 etl\ga4_to_local.py %* 1>>"%LOG%" 2>>&1
set "RC=%ERRORLEVEL%"

if not "%RC%"=="0" (
  echo [ERROR] GA4 sync fallo. Ultimas lineas del log:
  powershell -NoProfile -Command "Get-Content -Tail 60 '%LOG%'"
  exit /b %RC%
)

echo [OK] GA4 sync completo. Log: %LOG%
endlocal