-- Kaggle curated outputs (idempotente)

CREATE TABLE IF NOT EXISTS analytics.kaggle_price_benchmark (
  source           text NOT NULL,
  category_norm    text NOT NULL,
  currency         text NOT NULL DEFAULT '',
  n_prices         integer NOT NULL,
  price_p25        numeric(12,2),
  price_p50        numeric(12,2),
  price_p75        numeric(12,2),
  loaded_at        timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (source, category_norm, currency)
);

CREATE TABLE IF NOT EXISTS analytics.kaggle_category_weekly (
  source           text NOT NULL,
  week_start       date NOT NULL,
  category_norm    text NOT NULL,
  n_events         integer NOT NULL,
  revenue_proxy    numeric(18,2),
  loaded_at        timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (source, week_start, category_norm)
);

CREATE TABLE IF NOT EXISTS analytics.kaggle_text_terms (
  source           text NOT NULL,
  term             text NOT NULL,
  term_count       integer NOT NULL,
  loaded_at        timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (source, term)
);

CREATE OR REPLACE VIEW analytics.v_kaggle_price_benchmark AS
SELECT source, category_norm, currency, n_prices, price_p25, price_p50, price_p75
FROM analytics.kaggle_price_benchmark;

CREATE OR REPLACE VIEW analytics.v_kaggle_category_weekly AS
SELECT source, week_start, category_norm, n_events, revenue_proxy
FROM analytics.kaggle_category_weekly;

CREATE OR REPLACE VIEW analytics.v_kaggle_text_terms AS
SELECT source, term, term_count
FROM analytics.kaggle_text_terms;