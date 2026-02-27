-- Componente 2: Optimizacion del buscador (experimento)
-- 1) Diccionario derivado de dim_keyword_categoria

CREATE OR REPLACE VIEW analytics.v_search_dictionary AS
SELECT
  analytics.norm_txt(keyword_raw) AS raw_norm,
  keyword_raw,
  COALESCE(NULLIF(sinonimo_de,''), keyword_canonica) AS keyword_canonica,
  categoria,
  CASE
    WHEN es_error_ortografico THEN 'TYPO'
    WHEN sinonimo_de IS NOT NULL AND sinonimo_de <> '' THEN 'SYNONYM'
    ELSE 'CANON'
  END AS mapping_type,
  prioridad
FROM analytics.dim_keyword_categoria;

-- 2) Mapeo diario de términos buscados (GA4) a canónica/categoría
CREATE OR REPLACE VIEW analytics.v_search_term_mapped_daily AS
SELECT
  s.day,
  s.search_term,
  s.event_count AS searches,
  d.keyword_canonica,
  d.categoria,
  d.mapping_type,
  d.prioridad,
  CASE WHEN d.keyword_canonica IS NULL THEN 1 ELSE 0 END AS is_unmapped
FROM ga4.search_term_daily s
LEFT JOIN analytics.v_search_dictionary d
  ON analytics.norm_txt(s.search_term) = d.raw_norm;

-- 3) Resumen semanal (antes/después) de cobertura del diccionario
CREATE OR REPLACE VIEW analytics.v_search_dictionary_coverage_weekly AS
WITH base AS (
  SELECT
    date_trunc('week', day)::date AS week_start,
    SUM(searches) AS searches_total,
    SUM(CASE WHEN is_unmapped=1 THEN searches ELSE 0 END) AS searches_unmapped
  FROM analytics.v_search_term_mapped_daily
  GROUP BY 1
),
top_unmapped AS (
  SELECT
    date_trunc('week', day)::date AS week_start,
    search_term,
    SUM(searches) AS searches
  FROM analytics.v_search_term_mapped_daily
  WHERE is_unmapped=1
  GROUP BY 1,2
),
top10 AS (
  SELECT
    week_start,
    string_agg(search_term || ' (' || searches || ')', ' | ' ORDER BY searches DESC) AS top_unmapped_terms
  FROM (
    SELECT *,
      ROW_NUMBER() OVER(PARTITION BY week_start ORDER BY searches DESC) AS rn
    FROM top_unmapped
  ) x
  WHERE rn<=10
  GROUP BY 1
)
SELECT
  b.week_start,
  b.searches_total,
  b.searches_unmapped,
  ROUND(100.0 * b.searches_unmapped / NULLIF(b.searches_total,0),2) AS unmapped_pct,
  COALESCE(t.top_unmapped_terms,'') AS top_unmapped_terms
FROM base b
LEFT JOIN top10 t USING (week_start)
ORDER BY b.week_start DESC;

-- 4) Top typos (para priorizar correcciones)
CREATE OR REPLACE VIEW analytics.v_search_top_typos_weekly AS
SELECT
  date_trunc('week', day)::date AS week_start,
  keyword_canonica,
  categoria,
  search_term AS typo_term,
  SUM(searches) AS searches
FROM analytics.v_search_term_mapped_daily
WHERE mapping_type='TYPO'
GROUP BY 1,2,3,4
ORDER BY 1 DESC, 5 DESC;