@echo off
chcp 65001 >nul
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8

REM Conexion a Postgres local (Docker)
set "NEON_CONNECTION_STRING=postgresql://postgres:postgres@localhost:5433/chiru_local"
REM Parametros de corrida
set "ML_SITE_ID=MPE"
set "KW_LIMIT=10"
set "ITEM_LIMIT=50"
set "LOG=etl\ml_sync_%RANDOM%.log"
python -X utf8 etl\ml_to_local.py --site %ML_SITE_ID% --kw-limit %KW_LIMIT% --limit-items %ITEM_LIMIT% >> "%LOG%" 2>&1

IF %ERRORLEVEL% NEQ 0 (
  echo [ERROR] ML sync fallo. Revisa %LOG%
  exit /b 1
)

echo [OK] ML sync completo. Log: %LOG%
exit /b 0