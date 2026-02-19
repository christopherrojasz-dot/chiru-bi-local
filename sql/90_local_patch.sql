-- 90_local_patch.sql (IDEMPOTENTE)
-- Crea schemas analytics/ga4 + tablas GA4 + vistas BI.
-- También intenta mover tablas del schema public -> chiru_schema si el dump las puso en public.

CREATE SCHEMA IF NOT EXISTS chiru_schema;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS ga4;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Mover tablas public -> chiru_schema (solo si aplica)
DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'banner',
    'banner_image',
    'banner_product',
    'bill',
    'bills_details',
    'blacklist',
    'cart_item',
    'category',
    'certification',
    'claims',
    'comments',
    'commerce_promotion',
    'commerce_promotion_product',
    'company_sponsorship',
    'favorites',
    'feedbacks',
    'guest_checkout_session',
    'logistics',
    'logistics_order',
    'notification',
    'openpay_saved_cards',
    'police',
    'product',
    'product_image',
    'product_promotion',
    'product_ratings',
    'product_reviews',
    'quotation',
    'refresh_token',
    'report',
    'request_responses',
    'requests',
    'service_image',
    'services',
    'shopping_cart',
    'sponsored_product',
    'sponsorship',
    'sponsorship_content',
    'sponsorship_payment',
    'supplier',
    'supplier_contact',
    'supplier_identification',
    'supplier_image',
    'supplier_promotion',
    'supplier_type',
    'ticket',
    'ticket_detail',
    'user',
    'user_address',
    'user_extra_supplier',
    'user_identification',
    'user_information',
    'user_payment_method',
    'user_role',
    'user_verification'
  ]::text[] LOOP
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name=t)
       AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='chiru_schema' AND table_name=t)
    THEN
      EXECUTE format('ALTER TABLE public.%I SET SCHEMA chiru_schema;', t);
    END IF;
  END LOOP;
END $$;

-- =========================
-- GA4 tables
-- =========================

CREATE TABLE IF NOT EXISTS ga4.event_daily (
  day date NOT NULL,
  event_name text NOT NULL,
  event_count bigint NOT NULL DEFAULT 0,
  total_users bigint NOT NULL DEFAULT 0,
  sessions bigint NOT NULL DEFAULT 0,
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT pk_ga4_event_daily PRIMARY KEY (day, event_name)
);

CREATE TABLE IF NOT EXISTS ga4.funnel_daily (
  day date PRIMARY KEY,
  sessions bigint NOT NULL DEFAULT 0,
  view_item bigint NOT NULL DEFAULT 0,
  add_to_cart bigint NOT NULL DEFAULT 0,
  begin_checkout bigint NOT NULL DEFAULT 0,
  purchase bigint NOT NULL DEFAULT 0,
  updated_at timestamptz NOT NULL DEFAULT now()
);

-- =========================
-- BI views
-- =========================

CREATE OR REPLACE VIEW analytics.v_sales_daily AS
SELECT
  (b.paid_at)::date AS day,
  COUNT(*) AS bills_paid,
  COALESCE(SUM(b.total_amount),0) AS revenue_total,
  COALESCE(SUM(b.total),0) AS base_total,
  COALESCE(SUM(b.vat),0) AS vat_total,
  COALESCE(SUM(b.discount),0) AS discount_total
FROM chiru_schema.bill b
WHERE b.paid_at IS NOT NULL
  AND (b.payment_status IS NULL OR b.payment_status='PAID')
GROUP BY 1;

-- v_product_daily: evita asumir nombre de columna revenue en bills_details (varía entre dumps)
DO $$
DECLARE
  revenue_expr text;
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema='chiru_schema' AND table_name='bills_details' AND column_name='total_amount'
  ) THEN
    revenue_expr := 'COALESCE(SUM(d.total_amount),0)';

  ELSIF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema='chiru_schema' AND table_name='bills_details' AND column_name='total'
  ) THEN
    revenue_expr := 'COALESCE(SUM(d.total),0)';

  ELSIF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema='chiru_schema' AND table_name='bills_details' AND column_name='line_total'
  ) THEN
    revenue_expr := 'COALESCE(SUM(d.line_total),0)';

  ELSIF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema='chiru_schema' AND table_name='bills_details' AND column_name='subtotal'
  ) THEN
    revenue_expr := 'COALESCE(SUM(d.subtotal),0)';

  ELSIF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema='chiru_schema' AND table_name='bills_details' AND column_name='amount'
  ) THEN
    revenue_expr := 'COALESCE(SUM(d.amount),0)';

  ELSIF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema='chiru_schema' AND table_name='bills_details' AND column_name='unit_price'
  ) THEN
    revenue_expr := 'COALESCE(SUM(d.quantity * d.unit_price),0)';

  ELSIF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema='chiru_schema' AND table_name='bills_details' AND column_name='price'
  ) THEN
    revenue_expr := 'COALESCE(SUM(d.quantity * d.price),0)';

  ELSE
    revenue_expr := '0::numeric';
  END IF;

  EXECUTE format($SQL$
    CREATE OR REPLACE VIEW analytics.v_product_daily AS
    SELECT
      (b.paid_at)::date AS day,
      p.id AS product_id,
      p.name AS product_name,
      COALESCE(SUM(d.quantity),0) AS qty_sold,
      %s AS revenue
    FROM chiru_schema.bill b
    JOIN chiru_schema.bills_details d
      ON d.bill_code = b.code
    LEFT JOIN chiru_schema.product p
      ON p.id = d.product
    WHERE b.paid_at IS NOT NULL
      AND (b.payment_status IS NULL OR b.payment_status='PAID')
    GROUP BY 1,2,3;
  $SQL$, revenue_expr);
END $$;

CREATE OR REPLACE VIEW analytics.v_funnel_daily AS
SELECT
  COALESCE(f.day, s.day) AS day,
  COALESCE(f.sessions,0) AS ga4_sessions,
  COALESCE(f.view_item,0) AS ga4_view_item,
  COALESCE(f.add_to_cart,0) AS ga4_add_to_cart,
  COALESCE(f.begin_checkout,0) AS ga4_begin_checkout,
  COALESCE(f.purchase,0) AS ga4_purchase,
  COALESCE(s.bills_paid,0) AS bills_paid,
  COALESCE(s.revenue_total,0) AS revenue_total,
  CASE WHEN COALESCE(f.sessions,0) > 0
       THEN ROUND(100.0*COALESCE(s.bills_paid,0)/f.sessions, 2)
       ELSE 0
  END AS purchase_rate_pct
FROM ga4.funnel_daily f
FULL OUTER JOIN analytics.v_sales_daily s ON s.day = f.day;

CREATE OR REPLACE VIEW analytics.v_ga4_event_daily AS
SELECT day, event_name, event_count, total_users, sessions, updated_at
FROM ga4.event_daily;

CREATE OR REPLACE VIEW analytics.v_ga4_funnel_daily AS
SELECT day, sessions, view_item, add_to_cart, begin_checkout, purchase, updated_at
FROM ga4.funnel_daily;

-- ============================================================
-- Seed mínimo (BEST-EFFORT e IDEMPOTENTE)
-- - No debe romper el restore aunque el schema cambie.
-- - Si ON CONFLICT (id) falla por falta de constraint, hace DELETE+INSERT.
-- ============================================================

DO $$
DECLARE
  v_cat_id uuid := '00000000-0000-0000-0000-000000000001'::uuid;
  v_sup_id uuid := '00000000-0000-0000-0000-000000000002'::uuid;
  v_prd_id uuid := '00000000-0000-0000-0000-000000000003'::uuid;

  cols text;
  vals text;
  upd  text;

  sql_on    text;
  sql_plain text;

  tmp_type text;
BEGIN
  -- Si no existen tablas, no seedeamos
  IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='chiru_schema' AND table_name='category') THEN
    RETURN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='chiru_schema' AND table_name='supplier') THEN
    RETURN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='chiru_schema' AND table_name='product') THEN
    RETURN;
  END IF;

  -- -------------------------
  -- CATEGORY (best-effort)
  -- -------------------------
  BEGIN
    cols := 'id, name';
    vals := format('%L::uuid, %L', v_cat_id::text, 'Seed Category');
    upd  := 'name = EXCLUDED.name';

    IF EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema='chiru_schema' AND table_name='category' AND column_name='description'
    ) THEN
      SELECT data_type INTO tmp_type
      FROM information_schema.columns
      WHERE table_schema='chiru_schema' AND table_name='category' AND column_name='description';

      cols := cols || ', description';
      IF tmp_type = 'jsonb' THEN
        vals := vals || ', to_jsonb(' || quote_literal('Seed Category (seed)') || '::text)';
      ELSE
        vals := vals || ', ' || quote_literal('Seed Category (seed)');
      END IF;
      upd := upd || ', description = EXCLUDED.description';
    END IF;

    IF EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema='chiru_schema' AND table_name='category' AND column_name='created_at'
    ) THEN
      cols := cols || ', created_at';
      vals := vals || ', now()';
    END IF;

    IF EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema='chiru_schema' AND table_name='category' AND column_name='updated_at'
    ) THEN
      cols := cols || ', updated_at';
      vals := vals || ', now()';
    END IF;

    sql_on    := format('INSERT INTO chiru_schema.category (%s) VALUES (%s) ON CONFLICT (id) DO UPDATE SET %s;', cols, vals, upd);
    sql_plain := format('DELETE FROM chiru_schema.category WHERE id=%L::uuid; INSERT INTO chiru_schema.category (%s) VALUES (%s);', v_cat_id::text, cols, vals);

    BEGIN
      EXECUTE sql_on;
    EXCEPTION
      WHEN invalid_column_reference THEN
        EXECUTE sql_plain;
      WHEN OTHERS THEN
        NULL;
    END;
  EXCEPTION WHEN OTHERS THEN
    NULL;
  END;

  -- -------------------------
  -- SUPPLIER (best-effort)
  -- -------------------------
  BEGIN
    cols := 'id, name';
    vals := format('%L::uuid, %L', v_sup_id::text, 'Seed Supplier');
    upd  := 'name = EXCLUDED.name';

    IF EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema='chiru_schema' AND table_name='supplier' AND column_name='description'
    ) THEN
      SELECT data_type INTO tmp_type
      FROM information_schema.columns
      WHERE table_schema='chiru_schema' AND table_name='supplier' AND column_name='description';

      cols := cols || ', description';
      IF tmp_type = 'jsonb' THEN
        vals := vals || ', to_jsonb(' || quote_literal('Seed Supplier (seed)') || '::text)';
      ELSE
        vals := vals || ', ' || quote_literal('Seed Supplier (seed)');
      END IF;
      upd := upd || ', description = EXCLUDED.description';
    END IF;

    IF EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema='chiru_schema' AND table_name='supplier' AND column_name='created_at'
    ) THEN
      cols := cols || ', created_at';
      vals := vals || ', now()';
    END IF;

    IF EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema='chiru_schema' AND table_name='supplier' AND column_name='updated_at'
    ) THEN
      cols := cols || ', updated_at';
      vals := vals || ', now()';
    END IF;

    sql_on    := format('INSERT INTO chiru_schema.supplier (%s) VALUES (%s) ON CONFLICT (id) DO UPDATE SET %s;', cols, vals, upd);
    sql_plain := format('DELETE FROM chiru_schema.supplier WHERE id=%L::uuid; INSERT INTO chiru_schema.supplier (%s) VALUES (%s);', v_sup_id::text, cols, vals);

    BEGIN
      EXECUTE sql_on;
    EXCEPTION
      WHEN invalid_column_reference THEN
        EXECUTE sql_plain;
      WHEN OTHERS THEN
        NULL;
    END;
  EXCEPTION WHEN OTHERS THEN
    NULL;
  END;

  -- -------------------------
  -- PRODUCT (best-effort)
  -- - incluye description para evitar NOT NULL
  -- - price jsonb/numeric/text
  -- -------------------------
  BEGIN
    cols := 'id, name';
    vals := format('%L::uuid, %L', v_prd_id::text, 'Seed Product');
    upd  := 'name = EXCLUDED.name';

    -- description (si existe, siempre la seteamos)
    IF EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema='chiru_schema' AND table_name='product' AND column_name='description'
    ) THEN
      SELECT data_type INTO tmp_type
      FROM information_schema.columns
      WHERE table_schema='chiru_schema' AND table_name='product' AND column_name='description';

      cols := cols || ', description';
      IF tmp_type = 'jsonb' THEN
        vals := vals || ', to_jsonb(' || quote_literal('Seed Product (seed)') || '::text)';
      ELSE
        vals := vals || ', ' || quote_literal('Seed Product (seed)');
      END IF;
      upd := upd || ', description = EXCLUDED.description';
    END IF;

    -- FK columns si existen
    IF EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema='chiru_schema' AND table_name='product' AND column_name='category_id'
    ) THEN
      cols := cols || ', category_id';
      vals := vals || format(', %L::uuid', v_cat_id::text);
    END IF;

    IF EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema='chiru_schema' AND table_name='product' AND column_name='supplier_id'
    ) THEN
      cols := cols || ', supplier_id';
      vals := vals || format(', %L::uuid', v_sup_id::text);
    END IF;

    -- price (si existe)
    IF EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema='chiru_schema' AND table_name='product' AND column_name='price'
    ) THEN
      SELECT data_type INTO tmp_type
      FROM information_schema.columns
      WHERE table_schema='chiru_schema' AND table_name='product' AND column_name='price';

      cols := cols || ', price';
      IF tmp_type = 'jsonb' THEN
        vals := vals || ', to_jsonb(10.00::numeric)';
      ELSIF tmp_type IN ('numeric','double precision','real','integer','bigint','smallint','decimal') THEN
        vals := vals || ', 10.00';
      ELSE
        vals := vals || ', ' || quote_literal('10.00');
      END IF;
      upd := upd || ', price = EXCLUDED.price';
    END IF;

    -- timestamps comunes
    IF EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema='chiru_schema' AND table_name='product' AND column_name='created_at'
    ) THEN
      cols := cols || ', created_at';
      vals := vals || ', now()';
    END IF;

    IF EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema='chiru_schema' AND table_name='product' AND column_name='updated_at'
    ) THEN
      cols := cols || ', updated_at';
      vals := vals || ', now()';
      upd := upd || ', updated_at = EXCLUDED.updated_at';
    END IF;

    sql_on    := format('INSERT INTO chiru_schema.product (%s) VALUES (%s) ON CONFLICT (id) DO UPDATE SET %s;', cols, vals, upd);
    sql_plain := format('DELETE FROM chiru_schema.product WHERE id=%L::uuid; INSERT INTO chiru_schema.product (%s) VALUES (%s);', v_prd_id::text, cols, vals);

    BEGIN
      EXECUTE sql_on;
    EXCEPTION
      WHEN invalid_column_reference THEN
        EXECUTE sql_plain;
      WHEN OTHERS THEN
        NULL;
    END;
  EXCEPTION WHEN OTHERS THEN
    NULL;
  END;

END $$;