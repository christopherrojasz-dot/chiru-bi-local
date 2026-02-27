@echo off
chcp 65001 >nul
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8

if defined NEON_URL set "NEON_CONNECTION_STRING=%NEON_URL%"

REM Conexion URL (psycopg2 la soporta perfecto)
if not defined NEON_CONNECTION_STRING set "NEON_CONNECTION_STRING=Host=localhost;Port=5433;Username=postgres;Password=postgres;Database=chiru_local"

REM Log unico por corrida (evita lock)
set "LOG=etl\kaggle_build_%DATE:~-4%%DATE:~3,2%%DATE:~0,2%_%TIME:~0,2%%TIME:~3,2%%TIME:~6,2%.log"
set "LOG=%LOG: =0%"

python -X utf8 etl\kaggle_build_curated.py >> "%LOG%" 2>&1

IF %ERRORLEVEL% NEQ 0 (
  echo [ERROR] Kaggle build fallo. Revisa %LOG%
  exit /b 1
)

echo [OK] Kaggle curated listo. Log: %LOG%
exit /b 0