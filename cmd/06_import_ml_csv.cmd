@echo off
chcp 65001 >nul
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8

REM 1) Debes descargar el artifact ml_weekly.csv y guardarlo en: data\ml_weekly.csv
IF NOT EXIST "data\ml_weekly.csv" (
  echo [ERROR] Falta data\ml_weekly.csv. Descarga el artifact de GitHub Actions y colocarlo en esa ruta.
  exit /b 1
)

REM 2) Copiar al contenedor
docker cp data\ml_weekly.csv chiru-pg-local:/tmp/ml_weekly.csv

REM 3) Staging + COPY + UPSERT
docker exec -i chiru-pg-local psql -U postgres -d chiru_local -v ON_ERROR_STOP=1 -c "CREATE TEMP TABLE tmp_ml (week_start date, keyword_canonica text, site_id text, results_total int, price_median numeric, price_p25 numeric, price_p75 numeric, top_category_id text, top_category_name text) ON COMMIT DROP;"
docker exec -i chiru-pg-local psql -U postgres -d chiru_local -v ON_ERROR_STOP=1 -c "COPY tmp_ml FROM '/tmp/ml_weekly.csv' WITH (FORMAT csv, HEADER true);"
docker exec -i chiru-pg-local psql -U postgres -d chiru_local -v ON_ERROR_STOP=1 -c "INSERT INTO analytics.competitor_ml_weekly (week_start, keyword_canonica, site_id, results_total, price_median, price_p25, price_p75, top_category_id, top_category_name) SELECT week_start, trim(keyword_canonica), COALESCE(NULLIF(trim(site_id),''),'MPE'), results_total, price_median, price_p25, price_p75, NULLIF(trim(top_category_id),''), NULLIF(trim(top_category_name),'') FROM tmp_ml ON CONFLICT (week_start, keyword_canonica, site_id) DO UPDATE SET results_total=EXCLUDED.results_total, price_median=EXCLUDED.price_median, price_p25=EXCLUDED.price_p25, price_p75=EXCLUDED.price_p75, top_category_id=EXCLUDED.top_category_id, top_category_name=EXCLUDED.top_category_name, loaded_at=now();"

echo [OK] Import ML listo.
exit /b 0