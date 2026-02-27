@echo off
setlocal EnableExtensions
cd /d "%~dp0.."

if exist "cmd\_env_neon.local.cmd" call "cmd\_env_neon.local.cmd"

if not exist "dumps" mkdir "dumps"

if not defined NEON_PGHOST (echo [ERROR] Falta NEON_PGHOST en cmd\_env_neon.local.cmd & exit /b 1)
if not defined NEON_PGDATABASE (echo [ERROR] Falta NEON_PGDATABASE en cmd\_env_neon.local.cmd & exit /b 1)
if not defined NEON_PGUSER (echo [ERROR] Falta NEON_PGUSER en cmd\_env_neon.local.cmd & exit /b 1)
if not defined NEON_PGPASSWORD (echo [ERROR] Falta NEON_PGPASSWORD en cmd\_env_neon.local.cmd & exit /b 1)

set "PGHOST=%NEON_PGHOST%"
set "PGPORT=%NEON_PGPORT%"
set "PGDATABASE=%NEON_PGDATABASE%"
set "PGUSER=%NEON_PGUSER%"
set "PGPASSWORD=%NEON_PGPASSWORD%"
set "PGSSLMODE=require"
set "PGCHANNELBINDING=require"

docker run --rm ^
  -e PGHOST -e PGPORT -e PGDATABASE -e PGUSER -e PGPASSWORD -e PGSSLMODE -e PGCHANNELBINDING ^
  -v "%cd%\dumps:/work" ^
  postgres:17 ^
  pg_dump --schema-only --no-owner --no-privileges --file "/work/neon_schema.sql"

if errorlevel 1 (
  echo [ERROR] pg_dump fallo.
  exit /b 1
)

echo [OK] Dump generado en dumps\neon_schema.sql
endlocal