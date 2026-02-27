-- sql/20_dim_keyword_categoria.sql
-- Dimensión de keywords/categorías para diccionario + radar
-- Enterprise: staging + dedupe determinístico para evitar ambigüedad

-- FINAL (BI)
CREATE TABLE IF NOT EXISTS analytics.dim_keyword_categoria (
  keyword_id           bigserial PRIMARY KEY,
  keyword_raw          text      NOT NULL,
  keyword_canonica     text      NOT NULL,
  categoria            text      NOT NULL,
  subcategoria         text      NOT NULL,
  sinonimo_de          text      NULL,
  es_error_ortografico boolean   NOT NULL DEFAULT false,
  prioridad            smallint  NOT NULL DEFAULT 2 CHECK (prioridad IN (1,2,3)),
  notes                text      NULL,
  created_at           timestamptz NOT NULL DEFAULT now()
);

-- Compat: si existía el typo create_at, renombrar a created_at
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='analytics' AND table_name='dim_keyword_categoria' AND column_name='create_at'
  ) AND NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='analytics' AND table_name='dim_keyword_categoria' AND column_name='created_at'
  ) THEN
    ALTER TABLE analytics.dim_keyword_categoria RENAME COLUMN create_at TO created_at;
  END IF;
END $$;

-- STAGING (carga cruda CSV, sin constraints)
CREATE TABLE IF NOT EXISTS analytics.stg_dim_keyword_categoria (
  keyword_raw          text,
  keyword_canonica     text,
  categoria            text,
  subcategoria         text,
  sinonimo_de          text,
  es_error_ortografico boolean,
  prioridad            smallint,
  loaded_at            timestamptz NOT NULL DEFAULT now()
);

-- Índices legacy malos (si existían)
DROP INDEX IF EXISTS ux_dim_keyword_canonica_lower;
DROP INDEX IF EXISTS ux_dim_categoria_lower;

-- Único correcto: 1 keyword_raw (case-insensitive) -> 1 mapping final
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_keyword_raw_lower
  ON analytics.dim_keyword_categoria ((lower(keyword_raw)));

-- Performance (no únicos)
CREATE INDEX IF NOT EXISTS ix_dim_keyword_canonica_lower
  ON analytics.dim_keyword_categoria ((lower(keyword_canonica)));

CREATE INDEX IF NOT EXISTS ix_dim_categoria_lower
  ON analytics.dim_keyword_categoria ((lower(categoria)));

CREATE INDEX IF NOT EXISTS ix_dim_subcategoria_lower
  ON analytics.dim_keyword_categoria ((lower(subcategoria)));