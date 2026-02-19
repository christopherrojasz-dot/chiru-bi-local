@echo off
setlocal EnableExtensions
cd /d "%~dp0.."

set "CONTAINER=chiru-pg-local"
set "DB=chiru_local"
set "PGUSER=postgres"

rem --- check contenedor corriendo ---
docker ps -q -f "name=%CONTAINER%" | findstr /r /c:"[0-9a-f]" >nul
if errorlevel 1 (
  echo [ERROR] Contenedor %CONTAINER% no esta corriendo.
  exit /b 1
)

type "sql\99_sanity_checks.sql" | docker exec -i %CONTAINER% psql -U %PGUSER% -d %DB% -v ON_ERROR_STOP=1
if errorlevel 1 exit /b 1

echo [OK] Sanity OK.
endlocal