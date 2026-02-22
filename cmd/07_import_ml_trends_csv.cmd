@echo off
chcp 65001 >nul
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8

REM Debes descargar el artifact ml_trends_weekly_csv y ponerlo en: data\ml_trends_weekly.csv
IF NOT EXIST "data\ml_trends_weekly.csv" (
  echo [ERROR] Falta data\ml_trends_weekly.csv. Descarga el artifact y colocalo en esa ruta.
  exit /b 1
)

docker cp data\ml_trends_weekly.csv chiru-pg-local:/tmp/ml_trends_weekly.csv

docker exec -i chiru-pg-local psql -U postgres -d chiru_local -v ON_ERROR_STOP=1 -c "CREATE TEMP TABLE tmp_ml_trends (week_start date, site_id text, category_id text, rank smallint, keyword text, url text) ON COMMIT DROP;"
docker exec -i chiru-pg-local psql -U postgres -d chiru_local -v ON_ERROR_STOP=1 -c "COPY tmp_ml_trends FROM '/tmp/ml_trends_weekly.csv' WITH (FORMAT csv, HEADER true);"
docker exec -i chiru-pg-local psql -U postgres -d chiru_local -v ON_ERROR_STOP=1 -c "INSERT INTO analytics.ml_trends_weekly (week_start, site_id, category_id, rank, keyword, url) SELECT week_start, COALESCE(NULLIF(trim(site_id),''),'MPE'), COALESCE(trim(category_id),''), rank, trim(keyword), NULLIF(trim(url),'') FROM tmp_ml_trends ON CONFLICT (week_start, site_id, category_id, rank) DO UPDATE SET keyword=EXCLUDED.keyword, url=EXCLUDED.url, loaded_at=now();"

echo [OK] Import ML trends listo.
exit /b 0