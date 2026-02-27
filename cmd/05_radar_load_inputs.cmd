@echo off
setlocal EnableExtensions
cd /d "%~dp0.."

if not defined CONTAINER set "CONTAINER=chiru-pg-local"
if not defined DB set "DB=chiru_local"
if not defined PGUSER set "PGUSER=postgres"

echo [INFO] Loading Radar inputs into %DB% (container=%CONTAINER%)

REM A) Asegurar tablas/índices DIM + STG
docker exec -i %CONTAINER% psql -U %PGUSER% -d %DB% -v ON_ERROR_STOP=1 < "sql\20_dim_keyword_categoria.sql"
if errorlevel 1 goto FAIL

REM B) Cargar CSV a STAGING
if not exist "data\dim_keyword_categoria.csv" (
  echo [ERROR] Falta data\dim_keyword_categoria.csv
  goto FAIL
)

docker exec -i %CONTAINER% psql -U %PGUSER% -d %DB% -v ON_ERROR_STOP=1 -c "TRUNCATE analytics.stg_dim_keyword_categoria;"
if errorlevel 1 goto FAIL

docker exec -i %CONTAINER% psql -U %PGUSER% -d %DB% -v ON_ERROR_STOP=1 ^
  -c "COPY analytics.stg_dim_keyword_categoria (keyword_raw,keyword_canonica,categoria,subcategoria,sinonimo_de,es_error_ortografico,prioridad) FROM STDIN WITH (FORMAT csv, HEADER true, NULL '');" ^
  < "data\dim_keyword_categoria.csv"
if errorlevel 1 goto FAIL

REM C) Rebuild FINAL con dedupe (SQL por archivo, no por -c)
docker exec -i %CONTAINER% psql -U %PGUSER% -d %DB% -v ON_ERROR_STOP=1 < "sql\21_dim_keyword_categoria_dedupe.sql"
if errorlevel 1 goto FAIL

REM D) Trends weekly
if not exist "data\trends_weekly.csv" (
  echo [ERROR] Falta data\trends_weekly.csv
  goto FAIL
)

docker exec -i %CONTAINER% psql -U %PGUSER% -d %DB% -v ON_ERROR_STOP=1 -c "TRUNCATE analytics.trends_weekly;"
if errorlevel 1 goto FAIL

docker exec -i %CONTAINER% psql -U %PGUSER% -d %DB% -v ON_ERROR_STOP=1 ^
  -c "COPY analytics.trends_weekly (week_start,keyword_canonica,geo,region,interest) FROM STDIN WITH (FORMAT csv, HEADER true);" ^
  < "data\trends_weekly.csv"
if errorlevel 1 goto FAIL

REM E) Seed calendario
docker exec -i %CONTAINER% psql -U %PGUSER% -d %DB% -v ON_ERROR_STOP=1 < "sql\50_calendar_seed.sql"
if errorlevel 1 goto FAIL

REM F) Sanity en 1 línea
docker exec -i %CONTAINER% psql -U %PGUSER% -d %DB% -v ON_ERROR_STOP=1 -c "SELECT 'commercial_calendar_pe' obj, COUNT(*) rows FROM analytics.commercial_calendar_pe UNION ALL SELECT 'dim_keyword_categoria', COUNT(*) FROM analytics.dim_keyword_categoria UNION ALL SELECT 'trends_weekly', COUNT(*) FROM analytics.trends_weekly ORDER BY 1;"
if errorlevel 1 goto FAIL

echo [OK] Radar inputs loaded.
exit /b 0

:FAIL
echo [ERROR] Radar inputs load failed.
exit /b 