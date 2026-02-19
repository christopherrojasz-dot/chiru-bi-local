-- 99_sanity_checks.sql

SELECT version() AS postgres_version;

SELECT schema_name
FROM information_schema.schemata
WHERE schema_name IN ('chiru_schema','analytics','ga4')
ORDER BY 1;

SELECT COUNT(*) AS sales_daily_rows FROM analytics.v_sales_daily;
SELECT COUNT(*) AS product_daily_rows FROM analytics.v_product_daily;

SELECT COUNT(*) AS ga4_event_rows FROM ga4.event_daily;
SELECT COUNT(*) AS ga4_funnel_rows FROM ga4.funnel_daily;

SELECT * FROM analytics.v_sales_daily ORDER BY day DESC LIMIT 5;
SELECT * FROM analytics.v_product_daily ORDER BY day DESC, revenue DESC LIMIT 5;
