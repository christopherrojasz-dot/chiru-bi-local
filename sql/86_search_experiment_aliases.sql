-- PASO 10: Experimento de mejora del buscador (simulacion)
-- Tabla de aliases manuales (idempotente) + vista de cobertura "after"

CREATE TABLE IF NOT EXISTS analytics.search_alias_manual (
  raw_term_norm   text PRIMARY KEY,
  canonical_term  text NOT NULL,
  reason          text NOT NULL DEFAULT 'MANUAL',
  created_at      timestamptz NOT NULL DEFAULT now()
);

-- Seed inicial (basado en tus top unmapped reales). Ajustable.
-- Nota: usamos norm_txt para guardar la clave normalizada.
INSERT INTO analytics.search_alias_manual (raw_term_norm, canonical_term, reason) VALUES
(analytics.norm_txt('samsung'), 'samsung', 'brand'),
(analytics.norm_txt('sony'), 'sony', 'brand'),
(analytics.norm_txt('nike'), 'nike', 'brand'),
(analytics.norm_txt('iphone'), 'iphone', 'brand'),
(analytics.norm_txt('cargador'), 'cargador', 'product_term'),
(analytics.norm_txt('inalambrico'), 'inalambrico', 'attribute'),
(analytics.norm_txt('jean'), 'jean', 'product_term'),
(analytics.norm_txt('panel'), 'panel', 'product_term'),
(analytics.norm_txt('new'), 'new', 'noise_or_brand')
ON CONFLICT (raw_term_norm) DO NOTHING;

-- Vista "after": primero intenta diccionario, si no, intenta alias_manual
CREATE OR REPLACE VIEW analytics.v_search_term_mapped_daily_after AS
WITH d AS (
  SELECT raw_norm, keyword_canonica, categoria
  FROM analytics.v_search_dictionary
),
a AS (
  SELECT raw_term_norm, canonical_term
  FROM analytics.search_alias_manual
)
SELECT
  s.day,
  s.search_term,
  s.event_count AS searches,
  COALESCE(d.keyword_canonica, a.canonical_term) AS keyword_canonica,
  d.categoria,
  CASE
    WHEN d.keyword_canonica IS NOT NULL THEN 'DICT'
    WHEN a.canonical_term IS NOT NULL THEN 'ALIAS'
    ELSE 'UNMAPPED'
  END AS mapping_source,
  CASE
    WHEN d.keyword_canonica IS NULL AND a.canonical_term IS NULL THEN 1 ELSE 0
  END AS is_unmapped
FROM ga4.search_term_daily s
LEFT JOIN d
  ON analytics.norm_txt(s.search_term) = d.raw_norm
LEFT JOIN a
  ON analytics.norm_txt(s.search_term) = a.raw_term_norm;

-- Cobertura semanal "after"
CREATE OR REPLACE VIEW analytics.v_search_dictionary_coverage_after_weekly AS
SELECT
  date_trunc('week', day)::date AS week_start,
  SUM(searches) AS searches_total,
  SUM(CASE WHEN is_unmapped=1 THEN searches ELSE 0 END) AS searches_unmapped,
  ROUND(100.0 * SUM(CASE WHEN is_unmapped=1 THEN searches ELSE 0 END)/NULLIF(SUM(searches),0),2) AS unmapped_pct
FROM analytics.v_search_term_mapped_daily_after
GROUP BY 1
ORDER BY 1 DESC;