
@echo off
setlocal

REM 01_docker_up_local.cmd
REM Levanta postgres:17 en Docker en puerto 5433.

docker version >nul 2>&1
if errorlevel 1 (
  echo [ERROR] Docker no esta disponible. Abre Docker Desktop y reintenta.
  exit /b 1
)

set "CONTAINER=chiru-pg-local"
set "PORT=5433"
set "PASSWORD=postgres"
set "VOLUME=chiru_pgdata"

docker ps -a --format "{{.Names}}" | findstr /i /x "%CONTAINER%" >nul
if errorlevel 1 (
  echo [INFO] Creando contenedor %CONTAINER%...
  docker run -d ^
    --name %CONTAINER% ^
    -e POSTGRES_PASSWORD=%PASSWORD% ^
    -p %PORT%:5432 ^
    -v %VOLUME%:/var/lib/postgresql/data ^
    postgres:17
  if errorlevel 1 exit /b 1
) else (
  docker start %CONTAINER% >nul
)

timeout /t 3 /nobreak >nul
echo [OK] Postgres local listo en localhost:%PORT%
endlocal
