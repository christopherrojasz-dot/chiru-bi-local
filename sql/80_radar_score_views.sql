-- Componente 1: Modelo de priorizacion (score semanal) - idempotente

-- 1) Normalizador simple (sin extensiones)
CREATE OR REPLACE FUNCTION analytics.norm_txt(txt text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT lower(
    translate(coalesce(txt,''),
      'áéíóúÁÉÍÓÚüÜñÑ',
      'aeiouAEIOUuUnN'
    )
  );
$$;

-- 2) Puente Kaggle: mapeo manual internal_categoria -> patrones Kaggle
CREATE TABLE IF NOT EXISTS analytics.dim_kaggle_category_map (
  internal_categoria text NOT NULL,
  kaggle_pattern     text NOT NULL,
  notes              text NULL,
  created_at         timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (internal_categoria, kaggle_pattern)
);

-- Seeds iniciales (ajustables). Si no matchea alguna categoria, se queda NULL y listo.
INSERT INTO analytics.dim_kaggle_category_map (internal_categoria, kaggle_pattern, notes) VALUES
('Tecnología', 'electronics.%', 'Electronics general'),
('Tecnología', 'computers.%',   'Computers general'),
('Hogar',      'appliances.%',  'Appliances general')
ON CONFLICT (internal_categoria, kaggle_pattern) DO NOTHING;

-- 3) Benchmark Kaggle por categoria interna (weighted avg por n_prices)
CREATE OR REPLACE VIEW analytics.v_kaggle_benchmark_categoria AS
SELECT
  analytics.norm_txt(m.internal_categoria) AS categoria_norm,
  ROUND(
    SUM(b.price_p50 * b.n_prices) / NULLIF(SUM(b.n_prices),0)
  , 2) AS kaggle_price_p50,
  SUM(b.n_prices) AS kaggle_n_prices
FROM analytics.dim_kaggle_category_map m
JOIN analytics.kaggle_price_benchmark b
  ON b.category_norm LIKE m.kaggle_pattern
GROUP BY 1;

-- 4) GA4 search_term -> keyword canonica/categoria (weekly)
CREATE OR REPLACE VIEW analytics.v_search_term_mapped_weekly AS
WITH map AS (
  SELECT
    analytics.norm_txt(keyword_raw) AS raw_norm,
    COALESCE(NULLIF(sinonimo_de,''), keyword_canonica) AS keyword_canonica,
    categoria
  FROM analytics.dim_keyword_categoria
),
base AS (
  SELECT
    date_trunc('week', s.day)::date AS week_start,
    map.keyword_canonica,
    map.categoria,
    SUM(s.event_count) AS searches
  FROM ga4.search_term_daily s
  JOIN map
    ON analytics.norm_txt(s.search_term) = map.raw_norm
  GROUP BY 1,2,3
)
SELECT * FROM base;

-- 5) Control: términos no mapeados (weekly)
CREATE OR REPLACE VIEW analytics.v_search_term_unmapped_weekly AS
WITH map AS (
  SELECT DISTINCT analytics.norm_txt(keyword_raw) AS raw_norm
  FROM analytics.dim_keyword_categoria
)
SELECT
  date_trunc('week', s.day)::date AS week_start,
  s.search_term,
  SUM(s.event_count) AS searches
FROM ga4.search_term_daily s
LEFT JOIN map
  ON analytics.norm_txt(s.search_term) = map.raw_norm
WHERE map.raw_norm IS NULL
GROUP BY 1,2
ORDER BY 1 DESC, 3 DESC;

-- 6) Trends weekly (normalizado por keyword)
CREATE OR REPLACE VIEW analytics.v_trends_weekly_norm AS
SELECT
  week_start,
  analytics.norm_txt(keyword_canonica) AS keyword_norm,
  MAX(interest) AS trends_interest
FROM analytics.trends_weekly
GROUP BY 1,2;

-- 7) Calendar weekly por categoria (tags) + eventos
CREATE OR REPLACE VIEW analytics.v_calendar_weekly_categoria AS
WITH weeks AS (
  SELECT
    generate_series(
      (SELECT COALESCE(min(start_date), current_date) FROM analytics.commercial_calendar_pe),
      (SELECT COALESCE(max(end_date),   current_date) FROM analytics.commercial_calendar_pe),
      interval '1 week'
    )::date AS week_start
)
SELECT
  w.week_start,
  analytics.norm_txt(tag) AS categoria_norm,
  1 AS is_campaign_week,
  MIN(c.prioridad) AS best_priority,
  string_agg(DISTINCT c.event_name, ' | ' ORDER BY c.event_name) AS events
FROM weeks w
JOIN analytics.commercial_calendar_pe c
  ON w.week_start <= c.end_date
 AND (w.week_start + 6) >= c.start_date
CROSS JOIN LATERAL unnest(c.tags) AS tag
GROUP BY 1,2;

-- 8) Radar keyword semanal (Score)
CREATE OR REPLACE VIEW analytics.v_radar_weekly_keyword AS
WITH kw AS (
  SELECT
    week_start,
    categoria,
    keyword_canonica,
    searches,
    analytics.norm_txt(categoria) AS categoria_norm,
    analytics.norm_txt(keyword_canonica) AS keyword_norm
  FROM analytics.v_search_term_mapped_weekly
),
base AS (
  SELECT
    kw.week_start,
    kw.categoria,
    kw.keyword_canonica,
    kw.searches,
    COALESCE(t.trends_interest, 0) AS trends_interest,
    COALESCE(c.is_campaign_week, 0) AS is_campaign_week,
    c.events,
    b.kaggle_price_p50,
    b.kaggle_n_prices
  FROM kw
  LEFT JOIN analytics.v_trends_weekly_norm t
    ON t.week_start = kw.week_start
   AND t.keyword_norm = kw.keyword_norm
  LEFT JOIN analytics.v_calendar_weekly_categoria c
    ON c.week_start = kw.week_start
   AND c.categoria_norm = kw.categoria_norm
  LEFT JOIN analytics.v_kaggle_benchmark_categoria b
    ON b.categoria_norm = kw.categoria_norm
),
stats AS (
  SELECT
    week_start,
    MAX(searches) AS max_searches,
    MAX(trends_interest) AS max_trends
  FROM base
  GROUP BY 1
),
scored AS (
  SELECT
    b.*,
    COALESCE(1.0*b.searches / NULLIF(s.max_searches,0), 0) AS searches_norm,
    COALESCE(1.0*b.trends_interest / NULLIF(s.max_trends,0), 0) AS trends_norm
  FROM base b
  JOIN stats s USING (week_start)
)
SELECT
  week_start,
  categoria,
  keyword_canonica,
  searches,
  trends_interest,
  is_campaign_week,
  events,
  kaggle_price_p50,
  kaggle_n_prices,
  ROUND(
    100.0 * (
      0.55 * searches_norm +
      0.35 * trends_norm +
      0.10 * is_campaign_week
    )
  ,2) AS score_total,
  DENSE_RANK() OVER (
    PARTITION BY week_start
    ORDER BY
      (0.55 * searches_norm + 0.35 * trends_norm + 0.10 * is_campaign_week) DESC,
      categoria,
      keyword_canonica
  ) AS rank_week
FROM scored;

-- 9) Radar categoría semanal (Ranking por categoría)
CREATE OR REPLACE VIEW analytics.v_radar_weekly_categoria AS
SELECT
  week_start,
  categoria,
  SUM(searches) AS searches,
  ROUND(AVG(trends_interest), 2) AS trends_interest_avg,
  MAX(is_campaign_week) AS is_campaign_week,
  ROUND(AVG(score_total), 2) AS score_total_avg,
  DENSE_RANK() OVER (
    PARTITION BY week_start
    ORDER BY AVG(score_total) DESC, categoria
  ) AS rank_categoria
FROM analytics.v_radar_weekly_keyword
GROUP BY 1,2;