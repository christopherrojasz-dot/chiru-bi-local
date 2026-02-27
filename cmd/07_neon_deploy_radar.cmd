@echo off
setlocal EnableExtensions
cd /d "%~dp0.."

call "cmd\_env_neon.local.cmd"

if not defined NEON_URL (
  echo [ERROR] Falta NEON_URL. Revisa cmd\_env_neon.local.cmd
  exit /b 1
)

echo [INFO] Neon smoke test...
docker run --rm -i postgres:17 psql "%NEON_URL%" -v ON_ERROR_STOP=1 -c "select version();" || goto FAIL

echo [INFO] 1) Schemas minimo (analytics + ga4)
docker run --rm -i postgres:17 psql "%NEON_URL%" -v ON_ERROR_STOP=1 -c "CREATE SCHEMA IF NOT EXISTS analytics; CREATE SCHEMA IF NOT EXISTS ga4;" || goto FAIL

echo [INFO] 5) Load DIM (stg -> dedupe -> dim)

docker run --rm -i postgres:17 psql "%NEON_URL%" -v ON_ERROR_STOP=1 -c "TRUNCATE analytics.stg_dim_keyword_categoria;" || goto FAIL

type "data\dim_keyword_categoria.csv" | docker run --rm -i postgres:17 psql "%NEON_URL%" -v ON_ERROR_STOP=1 -c "COPY analytics.stg_dim_keyword_categoria (keyword_raw,keyword_canonica,categoria,subcategoria,sinonimo_de,es_error_ortografico,prioridad) FROM STDIN WITH (FORMAT csv, HEADER true, NULL '');" || goto FAIL

REM MUY IMPORTANTE: truncar DIM ANTES del insert (esto te evita el choque de 'acondicionador')
docker run --rm -i postgres:17 psql "%NEON_URL%" -v ON_ERROR_STOP=1 -c "TRUNCATE analytics.dim_keyword_categoria;" || goto FAIL

docker run --rm -i postgres:17 psql "%NEON_URL%" -v ON_ERROR_STOP=1 -c "WITH ranked AS (SELECT trim(keyword_raw) AS keyword_raw, trim(keyword_canonica) AS keyword_canonica, trim(categoria) AS categoria, trim(subcategoria) AS subcategoria, NULLIF(trim(COALESCE(sinonimo_de,'')),'') AS sinonimo_de, COALESCE(es_error_ortografico,false) AS es_error_ortografico, COALESCE(prioridad,2) AS prioridad, ROW_NUMBER() OVER (PARTITION BY lower(trim(keyword_raw)) ORDER BY COALESCE(prioridad,2) ASC, COALESCE(es_error_ortografico,false) ASC, (lower(trim(keyword_raw))=lower(trim(keyword_canonica))) DESC, length(trim(keyword_canonica)) DESC) AS rn FROM analytics.stg_dim_keyword_categoria WHERE keyword_raw IS NOT NULL AND keyword_canonica IS NOT NULL AND categoria IS NOT NULL AND subcategoria IS NOT NULL) INSERT INTO analytics.dim_keyword_categoria (keyword_raw,keyword_canonica,categoria,subcategoria,sinonimo_de,es_error_ortografico,prioridad) SELECT keyword_raw, keyword_canonica, categoria, subcategoria, sinonimo_de, es_error_ortografico, prioridad FROM ranked WHERE rn=1;" || goto FAIL

echo [INFO] 6) Load Trends weekly (staging -> normalize region -> final)

REM Asegurar staging (region puede venir vacia en CSV)
docker run --rm -i postgres:17 psql "%NEON_URL%" -v ON_ERROR_STOP=1 -c "CREATE TABLE IF NOT EXISTS analytics.stg_trends_weekly (week_start date, keyword_canonica text, geo text, region text, interest int);" || goto FAIL

docker run --rm -i postgres:17 psql "%NEON_URL%" -v ON_ERROR_STOP=1 -c "TRUNCATE analytics.stg_trends_weekly;" || goto FAIL

type "data\trends_weekly.csv" | docker run --rm -i postgres:17 psql "%NEON_URL%" -v ON_ERROR_STOP=1 ^
  -c "COPY analytics.stg_trends_weekly (week_start,keyword_canonica,geo,region,interest) FROM STDIN WITH (FORMAT csv, HEADER true, NULL '');" || goto FAIL

REM Cargar a final garantizando region NOT NULL (ALL)
docker run --rm -i postgres:17 psql "%NEON_URL%" -v ON_ERROR_STOP=1 -c "TRUNCATE analytics.trends_weekly;" || goto FAIL

docker run --rm -i postgres:17 psql "%NEON_URL%" -v ON_ERROR_STOP=1 -c "INSERT INTO analytics.trends_weekly (week_start, keyword_canonica, geo, region, interest) SELECT DISTINCT ON (week_start, keyword_canonica, geo, COALESCE(NULLIF(region,''),'ALL')) week_start, keyword_canonica, geo, COALESCE(NULLIF(region,''),'ALL') AS region, interest FROM analytics.stg_trends_weekly WHERE week_start IS NOT NULL AND keyword_canonica IS NOT NULL AND geo IS NOT NULL ORDER BY week_start, keyword_canonica, geo, COALESCE(NULLIF(region,''),'ALL'), interest DESC;" || goto FAIL

echo [INFO] 7) Seed calendario PE (idempotente)
type "sql\50_calendar_seed.sql" | docker run --rm -i postgres:17 psql "%NEON_URL%" -v ON_ERROR_STOP=1 || goto FAIL

echo [INFO] 8) Kaggle (DDL + load opcional)
type "sql\60_kaggle_curated_tables.sql" | docker run --rm -i postgres:17 psql "%NEON_URL%" -v ON_ERROR_STOP=1 || goto FAIL

if exist "data\kaggle_raw\" (
  echo [INFO] Kaggle raw detected. Running curated builder...
  set "NEON_CONNECTION_STRING=%NEON_URL%"
  call "etl\kaggle_build_curated.cmd" || goto FAIL
) else (
  echo [WARN] No hay data\kaggle_raw\. Kaggle queda vacio en Neon.
)

echo [INFO] 9) Build vistas Radar + Buscador + Dashboard
for %%F in (
  "sql\80_radar_score_views.sql"
  "sql\85_search_dictionary_views.sql"
  "sql\86_search_experiment_aliases.sql"
  "sql\81_radar_after_views.sql"
  "sql\90_dashboard_view.sql"
) do (
  echo [INFO] Applying %%~F
  type "%%~F" | docker run --rm -i postgres:17 psql "%NEON_URL%" -v ON_ERROR_STOP=1 || goto FAIL
)

echo [INFO] 10) Sanity Neon (UNA sola linea, para evitar bug de UNION en CMD)
docker run --rm -i postgres:17 psql "%NEON_URL%" -v ON_ERROR_STOP=1 -c "SELECT 'dim_keyword_categoria' obj, COUNT(*) rows FROM analytics.dim_keyword_categoria UNION ALL SELECT 'trends_weekly', COUNT(*) FROM analytics.trends_weekly UNION ALL SELECT 'commercial_calendar_pe', COUNT(*) FROM analytics.commercial_calendar_pe UNION ALL SELECT 'ga4_search_term_daily', COUNT(*) FROM ga4.search_term_daily UNION ALL SELECT 'v_radar_weekly_keyword', COUNT(*) FROM analytics.v_radar_weekly_keyword UNION ALL SELECT 'v_dashboard_weekly', COUNT(*) FROM analytics.v_dashboard_weekly ORDER BY 1;" || goto FAIL

echo [OK] Neon deploy listo.
exit /b 0

:FAIL
echo [ERROR] Neon deploy fallo.
exit /b 1