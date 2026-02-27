CREATE OR REPLACE VIEW analytics.v_dashboard_weekly AS
SELECT
  r.week_start,
  r.categoria,
  r.keyword_canonica,
  r.searches,
  r.trends_interest,
  r.is_campaign_week,
  CASE WHEN r.events IS NULL THEN '' ELSE r.events END AS events,
  r.kaggle_price_p50,
  r.score_total,
  r.rank_week,
  b.unmapped_pct AS unmapped_pct_before,
  a.unmapped_pct AS unmapped_pct_after
FROM analytics.v_radar_weekly_keyword r
LEFT JOIN analytics.v_search_dictionary_coverage_weekly b ON b.week_start = r.week_start
LEFT JOIN analytics.v_search_dictionary_coverage_after_weekly a ON a.week_start = r.week_start;
