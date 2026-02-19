@echo off
setlocal
cd /d "%~dp0.."

chcp 65001 >nul
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8

set "NEON_CONNECTION_STRING=Host=localhost;Port=5433;Username=postgres;Password=postgres;Database=chiru_local"

set "GA4_PROPERTY_ID=513497372"
set "GA4_CLIENT=C:\Users\%USERNAME%\secrets\ga4-oauth-client.json"
set "GA4_TOKEN=C:\Users\%USERNAME%\secrets\ga4-token.json"

set "LOG=etl\ga4_sync.log"
echo ==== GA4 SYNC START %DATE% %TIME% ====>> "%LOG%"

python -X utf8 etl\ga4_to_local.py %* 1>>"%LOG%" 2>>&1
set "RC=%ERRORLEVEL%"

if not "%RC%"=="0" (
  echo [ERROR] GA4 sync fallo. Revisa %LOG%
  exit /b %RC%
)

echo [OK] GA4 sync completo. Log: %LOG%
endlocal
