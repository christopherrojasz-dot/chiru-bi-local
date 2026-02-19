-- 95_seed_minimal.sql (IDEMPOTENTE + schema-aware)
-- Seed MINIMO para BI:
--  - 1 category, 1 supplier, 1 product
--  - 1 bill pagado
--  - 1 bills_details (linea) para que v_sales_daily y v_product_daily tengan filas

DO $$
DECLARE
  v_cat_id uuid := '00000000-0000-0000-0000-000000000001'::uuid;
  v_sup_id uuid := '00000000-0000-0000-0000-000000000002'::uuid;
  v_prd_id uuid := '00000000-0000-0000-0000-000000000003'::uuid;

  v_bill_code text := 'SEED-BILL-0001';

  -- tipos detectados
  prod_price_type text;
  prod_desc_is_required boolean;

  d_price_type text;

  -- para inserts dinamicos
  cols text;
  vals text;
  upd  text;

  sql_on text;
  sql_plain text;

  -- bills_details revenue column pick
  detail_has_total_amount boolean;
  detail_has_total boolean;
  detail_has_line_total boolean;
  detail_has_subtotal boolean;
  detail_has_amount boolean;
  detail_has_unit_price boolean;
  detail_has_price boolean;
BEGIN
  -- Si no existen tablas base, no seedeamos (no romper restore)
  IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='chiru_schema' AND table_name='category') THEN
    RETURN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='chiru_schema' AND table_name='supplier') THEN
    RETURN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='chiru_schema' AND table_name='product') THEN
    RETURN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='chiru_schema' AND table_name='bill') THEN
    RETURN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='chiru_schema' AND table_name='bills_details') THEN
    RETURN;
  END IF;

  -- =========================
  -- CATEGORY (best-effort)
  -- =========================
  BEGIN
    cols := 'id, name';
    vals := format('%L::uuid, %L', v_cat_id::text, 'Seed Category');
    upd  := 'name = EXCLUDED.name';

    -- si hay description y es NOT NULL, la llenamos
    IF EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema='chiru_schema' AND table_name='category' AND column_name='description'
    ) THEN
      cols := cols || ', description';
      vals := vals || ', ' || quote_literal('Seed Category (seed)');
      upd  := upd  || ', description = EXCLUDED.description';
    END IF;

    sql_on    := format('INSERT INTO chiru_schema.category (%s) VALUES (%s) ON CONFLICT (id) DO UPDATE SET %s;', cols, vals, upd);
    sql_plain := format('DELETE FROM chiru_schema.category WHERE id=%L::uuid; INSERT INTO chiru_schema.category (%s) VALUES (%s);', v_cat_id::text, cols, vals);

    BEGIN
      EXECUTE sql_on;
    EXCEPTION WHEN invalid_column_reference THEN
      EXECUTE sql_plain;
    END;
  EXCEPTION WHEN OTHERS THEN
    NULL;
  END;

  -- =========================
  -- SUPPLIER (best-effort)
  -- =========================
  BEGIN
    cols := 'id, name';
    vals := format('%L::uuid, %L', v_sup_id::text, 'Seed Supplier');
    upd  := 'name = EXCLUDED.name';

    IF EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema='chiru_schema' AND table_name='supplier' AND column_name='description'
    ) THEN
      cols := cols || ', description';
      vals := vals || ', ' || quote_literal('Seed Supplier (seed)');
      upd  := upd  || ', description = EXCLUDED.description';
    END IF;

    sql_on    := format('INSERT INTO chiru_schema.supplier (%s) VALUES (%s) ON CONFLICT (id) DO UPDATE SET %s;', cols, vals, upd);
    sql_plain := format('DELETE FROM chiru_schema.supplier WHERE id=%L::uuid; INSERT INTO chiru_schema.supplier (%s) VALUES (%s);', v_sup_id::text, cols, vals);

    BEGIN
      EXECUTE sql_on;
    EXCEPTION WHEN invalid_column_reference THEN
      EXECUTE sql_plain;
    END;
  EXCEPTION WHEN OTHERS THEN
    NULL;
  END;

  -- =========================
  -- PRODUCT (schema-aware)
  -- =========================
  SELECT data_type
    INTO prod_price_type
  FROM information_schema.columns
  WHERE table_schema='chiru_schema' AND table_name='product' AND column_name='price';

  SELECT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema='chiru_schema' AND table_name='product' AND column_name='description' AND is_nullable='NO'
  ) INTO prod_desc_is_required;

  BEGIN
    cols := 'id, name';
    vals := format('%L::uuid, %L', v_prd_id::text, 'Seed Product');
    upd  := 'name = EXCLUDED.name';

    -- description: si existe (sobre todo si es NOT NULL), la seteamos
    IF EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema='chiru_schema' AND table_name='product' AND column_name='description'
    ) THEN
      cols := cols || ', description';
      vals := vals || ', ' || quote_literal('Seed Product (seed)');
      upd  := upd  || ', description = EXCLUDED.description';
    ELSIF prod_desc_is_required THEN
      -- si por alguna razón info_schema dice required pero no encontramos columna (ultra raro), igual no rompemos
      NULL;
    END IF;

    -- FKs si existen
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

    -- price si existe (jsonb vs numeric/text)
    IF prod_price_type IS NOT NULL THEN
      cols := cols || ', price';
      IF prod_price_type = 'jsonb' THEN
        vals := vals || ', to_jsonb(10.00::numeric)';
      ELSIF prod_price_type IN ('numeric','double precision','real','integer','bigint','smallint','decimal') THEN
        vals := vals || ', 10.00';
      ELSE
        vals := vals || ', ' || quote_literal('10.00');
      END IF;
      upd := upd || ', price = EXCLUDED.price';
    END IF;

    sql_on    := format('INSERT INTO chiru_schema.product (%s) VALUES (%s) ON CONFLICT (id) DO UPDATE SET %s;', cols, vals, upd);
    sql_plain := format('DELETE FROM chiru_schema.product WHERE id=%L::uuid; INSERT INTO chiru_schema.product (%s) VALUES (%s);', v_prd_id::text, cols, vals);

    BEGIN
      EXECUTE sql_on;
    EXCEPTION WHEN invalid_column_reference THEN
      EXECUTE sql_plain;
    END;
  EXCEPTION WHEN OTHERS THEN
    NULL;
  END;

  -- =========================
  -- BILL (seed pagado)
  -- =========================
  BEGIN
    -- OJO: asumimos que estas columnas existen en bill (normalmente sí en tu schema)
    -- Si no existen, no rompemos el restore.
    sql_on :=
      'INSERT INTO chiru_schema.bill (code, total_amount, total, vat, discount, payment_status, paid_at)
       VALUES ($1, 10.00, 10.00, 0.00, 0.00, ''PAID'', now())
       ON CONFLICT (code) DO UPDATE SET paid_at = EXCLUDED.paid_at;';
    sql_plain :=
      'DELETE FROM chiru_schema.bill WHERE code = $1;
       INSERT INTO chiru_schema.bill (code, total_amount, total, vat, discount, payment_status, paid_at)
       VALUES ($1, 10.00, 10.00, 0.00, 0.00, ''PAID'', now());';

    BEGIN
      EXECUTE sql_on USING v_bill_code;
    EXCEPTION WHEN invalid_column_reference THEN
      EXECUTE sql_plain USING v_bill_code;
    END;
  EXCEPTION WHEN OTHERS THEN
    NULL;
  END;

  -- =========================
  -- BILLS_DETAILS (1 línea)
  -- No asume total_amount; elige una columna de "revenue" disponible
  -- =========================

  SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='chiru_schema' AND table_name='bills_details' AND column_name='total_amount')
    INTO detail_has_total_amount;
  SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='chiru_schema' AND table_name='bills_details' AND column_name='total')
    INTO detail_has_total;
  SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='chiru_schema' AND table_name='bills_details' AND column_name='line_total')
    INTO detail_has_line_total;
  SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='chiru_schema' AND table_name='bills_details' AND column_name='subtotal')
    INTO detail_has_subtotal;
  SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='chiru_schema' AND table_name='bills_details' AND column_name='amount')
    INTO detail_has_amount;
  SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='chiru_schema' AND table_name='bills_details' AND column_name='unit_price')
    INTO detail_has_unit_price;
  SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='chiru_schema' AND table_name='bills_details' AND column_name='price')
    INTO detail_has_price;

  -- si existe bills_details.price, averigua tipo por si fuera jsonb
  IF detail_has_price THEN
    SELECT data_type INTO d_price_type
    FROM information_schema.columns
    WHERE table_schema='chiru_schema' AND table_name='bills_details' AND column_name='price';
  END IF;

  BEGIN
    cols := 'bill_code, product';
    vals := format('%L, %L::uuid', v_bill_code, v_prd_id::text);

    IF EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema='chiru_schema' AND table_name='bills_details' AND column_name='quantity'
    ) THEN
      cols := cols || ', quantity';
      vals := vals || ', 1';
    END IF;

    -- elegimos 1 columna de revenue a poblar (misma prioridad que la vista)
    IF detail_has_total_amount THEN
      cols := cols || ', total_amount';
      vals := vals || ', 10.00';
    ELSIF detail_has_total THEN
      cols := cols || ', total';
      vals := vals || ', 10.00';
    ELSIF detail_has_line_total THEN
      cols := cols || ', line_total';
      vals := vals || ', 10.00';
    ELSIF detail_has_subtotal THEN
      cols := cols || ', subtotal';
      vals := vals || ', 10.00';
    ELSIF detail_has_amount THEN
      cols := cols || ', amount';
      vals := vals || ', 10.00';
    ELSIF detail_has_unit_price THEN
      cols := cols || ', unit_price';
      vals := vals || ', 10.00';
    ELSIF detail_has_price THEN
      cols := cols || ', price';
      IF d_price_type = 'jsonb' THEN
        vals := vals || ', to_jsonb(10.00::numeric)';
      ELSIF d_price_type IN ('numeric','double precision','real','integer','bigint','smallint','decimal') THEN
        vals := vals || ', 10.00';
      ELSE
        vals := vals || ', ' || quote_literal('10.00');
      END IF;
    END IF;

    -- Idempotencia: intentamos ON CONFLICT(bill_code, product) y si no hay constraint, DELETE+INSERT
    sql_on := format(
      'INSERT INTO chiru_schema.bills_details (%s) VALUES (%s) ON CONFLICT (bill_code, product) DO NOTHING;',
      cols, vals
    );
    sql_plain := format(
      'DELETE FROM chiru_schema.bills_details WHERE bill_code=%L AND product=%L::uuid; INSERT INTO chiru_schema.bills_details (%s) VALUES (%s);',
      v_bill_code, v_prd_id::text, cols, vals
    );

    BEGIN
      EXECUTE sql_on;
    EXCEPTION WHEN invalid_column_reference THEN
      EXECUTE sql_plain;
    END;
  EXCEPTION WHEN OTHERS THEN
    NULL;
  END;

END $$;