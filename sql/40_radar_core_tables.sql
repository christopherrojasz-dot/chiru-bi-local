-- sql/40_radar_core_tables.sql
-- Tablas CORE del Radar: calendario comercial PE + Google Trends semanal
-- Idempotente: crea si no existe y asegura constraints/indices.

-- 1) Calendario comercial Perú
CREATE TABLE IF NOT EXISTS analytics.commercial_calendar_pe (
  event_name  text        NOT NULL,
  event_type  text        NOT NULL,
  start_date  date        NOT NULL,
  end_date    date        NOT NULL,
  city        text        NOT NULL DEFAULT 'Lima',
  tags        text[]      NOT NULL DEFAULT '{}'::text[],
  prioridad   int         NOT NULL DEFAULT 2,
  notes       text        NULL,
  created_at  timestamptz NOT NULL DEFAULT now()
);

-- Checks/constraints (idempotentes con DO)
DO $$
BEGIN
  -- event_type permitido
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'ck_calendar_event_type'
  ) THEN
    ALTER TABLE analytics.commercial_calendar_pe
      ADD CONSTRAINT ck_calendar_event_type
      CHECK (event_type IN ('SEASON','CAMPAIGN','HOLIDAY','OTHER'));
  END IF;

  -- rango de fechas válido
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'ck_calendar_date_range'
  ) THEN
    ALTER TABLE analytics.commercial_calendar_pe
      ADD CONSTRAINT ck_calendar_date_range
      CHECK (start_date <= end_date);
  END IF;

  -- prioridad razonable
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'ck_calendar_priority'
  ) THEN
    ALTER TABLE analytics.commercial_calendar_pe
      ADD CONSTRAINT ck_calendar_priority
      CHECK (prioridad BETWEEN 1 AND 5);
  END IF;
END $$;

DO $$
DECLARE
  is_unique boolean;
BEGIN
  SELECT i.indisunique
    INTO is_unique
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  JOIN pg_index i ON i.indexrelid = c.oid
  WHERE n.nspname = 'analytics'
    AND c.relname = 'ux_calendar_event_dates';

  -- Si no existe (NULL) o existe pero no es unique, lo (re)creamos
  IF is_unique IS DISTINCT FROM true THEN
    -- si existe como indice pero no-unique, lo botamos
    IF is_unique IS NOT NULL THEN
      EXECUTE 'DROP INDEX IF EXISTS analytics.ux_calendar_event_dates';
    END IF;

    EXECUTE 'CREATE UNIQUE INDEX IF NOT EXISTS ux_calendar_event_dates
             ON analytics.commercial_calendar_pe (event_name, start_date, end_date, city)';
  END IF;
END $$;

-- Índices útiles
CREATE INDEX IF NOT EXISTS ix_calendar_date_range
  ON analytics.commercial_calendar_pe (start_date, end_date);

CREATE INDEX IF NOT EXISTS ix_calendar_city
  ON analytics.commercial_calendar_pe (city);

CREATE INDEX IF NOT EXISTS ix_calendar_tags_gin
  ON analytics.commercial_calendar_pe USING gin (tags);


-- 2) Google Trends weekly (se carga desde data/trends_weekly.csv)
CREATE TABLE IF NOT EXISTS analytics.trends_weekly (
  week_start       date        NOT NULL,
  keyword_canonica text        NOT NULL,
  interest         int         NOT NULL,
  geo              text        NOT NULL DEFAULT 'PE',
  source           text        NOT NULL DEFAULT 'google_trends',
  ingested_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (week_start, keyword_canonica, geo)
);

-- Compatibilidad con CSV (week_start, keyword_canonica, geo, region, interest)
ALTER TABLE analytics.trends_weekly
  ADD COLUMN IF NOT EXISTS region text;

ALTER TABLE analytics.trends_weekly
  ALTER COLUMN region SET DEFAULT 'ALL';

CREATE INDEX IF NOT EXISTS ix_trends_region
  ON analytics.trends_weekly (region);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'ck_trends_interest_range'
  ) THEN
    ALTER TABLE analytics.trends_weekly
      ADD CONSTRAINT ck_trends_interest_range
      CHECK (interest BETWEEN 0 AND 100);
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS ix_trends_keyword_week
  ON analytics.trends_weekly (keyword_canonica, week_start);

CREATE INDEX IF NOT EXISTS ix_trends_week
  ON analytics.trends_weekly (week_start);