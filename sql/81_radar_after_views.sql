-- Radar AFTER: usa el mapeo after (diccionario + alias manual)
-- NO reemplaza nada existente. Solo crea vistas nuevas.

-- 1) Weekly keyword mapped AFTER
CREATE OR REPLACE VIEW analytics.v_search_term_mapped_weekly_after AS
SELECT
  date_trunc('week', day)::date AS week_start,
  COALESCE(keyword_canonica, analytics.norm_txt(search_term)) AS keyword_canonica,
  COALESCE(categoria, 'SinCategoria') AS categoria,
  SUM(searches) AS searches,
  analytics.norm_txt(COALESCE(categoria, 'SinCategoria')) AS categoria_norm,
  analytics.norm_txt(COALESCE(keyword_canonica, search_term)) AS keyword_norm
FROM analytics.v_search_term_mapped_daily_after
WHERE is_unmapped = 0
GROUP BY 1,2,3,5,6;

-- 2) Radar weekly keyword AFTER (mismo score que BEFORE)
CREATE OR REPLACE VIEW analytics.v_radar_weekly_keyword_after AS
WITH kw AS (
  SELECT
    week_start,
    categoria,
    keyword_canonica,
    searches,
    categoria_norm,
    keyword_norm
  FROM analytics.v_search_term_mapped_weekly_after
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

-- 3) Dashboard “after” (todo en uno)
CREATE OR REPLACE VIEW analytics.v_dashboard_weekly_after AS
SELECT
  r.week_start,
  r.categoria,
  r.keyword_canonica,
  r.searches,
  r.trends_interest,
  r.is_campaign_week,
  COALESCE(r.events,'') AS events,
  r.kaggle_price_p50,
  r.score_total,
  r.rank_week,
  b.unmapped_pct AS unmapped_pct_before,
  a.unmapped_pct AS unmapped_pct_after
FROM analytics.v_radar_weekly_keyword_after r
LEFT JOIN analytics.v_search_dictionary_coverage_weekly b
  ON b.week_start = r.week_start
LEFT JOIN analytics.v_search_dictionary_coverage_after_weekly a
  ON a.week_start = r.week_start;