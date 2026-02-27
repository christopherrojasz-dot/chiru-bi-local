@echo off
setlocal EnableExtensions
cd /d "%~dp0.."

if not defined CONTAINER set "CONTAINER=chiru-pg-local"
if not defined DB set "DB=chiru_local"
if not defined PGUSER set "PGUSER=postgres"

echo [INFO] Building Radar views in %DB% (container=%CONTAINER%)

REM 1) Kaggle DDL (tablas + vistas)
docker exec -i %CONTAINER% psql -U %PGUSER% -d %DB% -v ON_ERROR_STOP=1 < "sql\60_kaggle_curated_tables.sql"
if errorlevel 1 goto FAIL

REM 2) Kaggle load (opcional: si existe data\kaggle_raw)
if exist "data\kaggle_raw\" (
  echo [INFO] Kaggle raw detected. Running curated builder...
  call "etl\kaggle_build_curated.cmd"
  if errorlevel 1 goto FAIL
) else (
  echo [WARN] data\kaggle_raw\ no existe. Kaggle queda vacio; el score funciona igual pero sin benchmark.
)
REM 3) Radar + Buscador + Dashboard (orden importa)
for %%F in (
  "sql\80_radar_score_views.sql"
  "sql\85_search_dictionary_views.sql"
  "sql\86_search_experiment_aliases.sql"
  "sql\81_radar_after_views.sql"
  "sql\90_dashboard_view.sql"
) do (
  echo [INFO] Applying %%~F
  docker exec -i %CONTAINER% psql -U %PGUSER% -d %DB% -v ON_ERROR_STOP=1 < "%%~F"
  if errorlevel 1 goto FAIL
)

docker exec -i %CONTAINER% psql -U %PGUSER% -d %DB% -v ON_ERROR_STOP=1 -c "SELECT 'v_radar_weekly_keyword' obj, COUNT(*) rows FROM analytics.v_radar_weekly_keyword UNION ALL SELECT 'v_radar_weekly_categoria', COUNT(*) FROM analytics.v_radar_weekly_categoria UNION ALL SELECT 'v_dashboard_weekly', COUNT(*) FROM analytics.v_dashboard_weekly UNION ALL SELECT 'coverage_before', COUNT(*) FROM analytics.v_search_dictionary_coverage_weekly UNION ALL SELECT 'coverage_after', COUNT(*) FROM analytics.v_search_dictionary_coverage_after_weekly ORDER BY 1;"
if errorlevel 1 goto FAIL

echo [OK] Radar views built.
exit /b 0

:FAIL
echo [ERROR] Radar views build failed.
exit /b 1