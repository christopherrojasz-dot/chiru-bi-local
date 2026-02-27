@echo off
setlocal EnableExtensions
cd /d "%~dp0.."

set "CONTAINER=chiru-pg-local"
set "DB=chiru_local"
set "PGUSER=postgres"

rem --- check contenedor corriendo (sin templates, sin for/f) ---
docker ps -q -f "name=%CONTAINER%" | findstr /r /c:"[0-9a-f]" >nul
if errorlevel 1 goto NOT_RUNNING

set "DUMP=dumps\neon_schema.sql"
set "USE_MINIMAL=0"

if not exist "%DUMP%" set "USE_MINIMAL=1"
if exist "%DUMP%" findstr /c:"PLACEHOLDER" "%DUMP%" >nul && set "USE_MINIMAL=1"

if "%USE_MINIMAL%"=="1" goto USE_MINIMAL
echo [INFO] Usando dump: %DUMP%
goto AFTER_DUMP

:USE_MINIMAL
set "DUMP=dumps\minimal_schema.sql"
echo [WARN] Usando schema DEMO (minimal_schema.sql). Reemplaza dumps\neon_schema.sql por el real.

:AFTER_DUMP
docker exec -i %CONTAINER% psql -U %PGUSER% -d postgres -v ON_ERROR_STOP=1 -c "DROP DATABASE IF EXISTS %DB%;" || goto FAIL
docker exec -i %CONTAINER% psql -U %PGUSER% -d postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE %DB%;" || goto FAIL

type "%DUMP%" | docker exec -i %CONTAINER% psql -U %PGUSER% -d %DB% -v ON_ERROR_STOP=1 || goto FAIL
type "sql\90_local_patch.sql" | docker exec -i %CONTAINER% psql -U %PGUSER% -d %DB% -v ON_ERROR_STOP=1 || goto FAIL
type "sql\95_seed_minimal.sql" | docker exec -i %CONTAINER% psql -U %PGUSER% -d %DB% -v ON_ERROR_STOP=1 || goto FAIL
type "sql\99_sanity_checks.sql" | docker exec -i %CONTAINER% psql -U %PGUSER% -d %DB% -v ON_ERROR_STOP=1 || goto FAIL
REM NUEVO: tablas core del Radar (calendar + trends)
type "sql\40_radar_core_tables.sql" | docker exec -i %CONTAINER% psql -U %PGUSER% -d %DB% -v ON_ERROR_STOP=1 || goto FAIL

call "cmd\05_radar_load_inputs.cmd" || goto FAIL

call "cmd\06_radar_build_views.cmd" || goto FAIL

echo [OK] Reset completo.
goto END

:NOT_RUNNING
echo [ERROR] Contenedor %CONTAINER% no esta corriendo. Ejecuta cmd\01_docker_up_local.cmd
exit /b 1

:FAIL
exit /b 1

:END
endlocal