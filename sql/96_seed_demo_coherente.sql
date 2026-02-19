BEGIN;

DO $$
DECLARE
  -- IDs fijos
  v_cat_id uuid := '00000000-0000-0000-0000-000000000001'::uuid;
  v_sup_id uuid := '00000000-0000-0000-0000-000000000002'::uuid;

  v_prd_a uuid := '10000000-0000-0000-0000-000000000001'::uuid;
  v_prd_b uuid := '10000000-0000-0000-0000-000000000002'::uuid;
  v_prd_c uuid := '10000000-0000-0000-0000-000000000003'::uuid;

  v_size chiru_schema.product_size_enum;

  i int := 0;
  d date;

  code1 int;
  code2 int;

  base1 numeric; vat1 numeric; disc1 numeric; tot1 numeric;
  base2 numeric; vat2 numeric; disc2 numeric; tot2 numeric;

BEGIN
  -- tomar 1 valor valido del enum de size (evita adivinar)
  SELECT (enum_range(NULL::chiru_schema.product_size_enum))[1] INTO v_size;

  -- =========================
  -- CATEGORY (requerida)
  -- =========================
  INSERT INTO chiru_schema.category (id, name)
  VALUES (v_cat_id, 'DEMO-Categoria')
  ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;

  -- =========================
  -- SUPPLIER (para FK de product)
  -- (en tu schema supplier.user_id NO es obligatorio sin default, asi que lo dejamos NULL)
  -- =========================
  INSERT INTO chiru_schema.supplier (id, name)
  VALUES (v_sup_id, 'DEMO-Supplier')
  ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;

  -- =========================
  -- PRODUCTS (tus NOT NULL obligatorios)
  -- =========================
  INSERT INTO chiru_schema.product
    (id, name, description, details, basic_info, size, supplier_id, category_id, price)
  VALUES
    (v_prd_a, 'DEMO-Producto A', 'Demo A',
      jsonb_build_object('brand','Demo','sku','DEMO-A','tags', jsonb_build_array('eco','popular')),
      jsonb_build_object('weight','1kg','material','cotton'),
      v_size, v_sup_id, v_cat_id, to_jsonb(10.00::numeric)
    ),
    (v_prd_b, 'DEMO-Producto B', 'Demo B',
      jsonb_build_object('brand','Demo','sku','DEMO-B','tags', jsonb_build_array('new')),
      jsonb_build_object('weight','0.5kg','material','leather'),
      v_size, v_sup_id, v_cat_id, to_jsonb(15.00::numeric)
    ),
    (v_prd_c, 'DEMO-Producto C', 'Demo C',
      jsonb_build_object('brand','Demo','sku','DEMO-C','tags', jsonb_build_array('sale')),
      jsonb_build_object('weight','2kg','material','denim'),
      v_size, v_sup_id, v_cat_id, to_jsonb(20.00::numeric)
    )
  ON CONFLICT (id) DO UPDATE SET
    name=EXCLUDED.name,
    description=EXCLUDED.description,
    details=EXCLUDED.details,
    basic_info=EXCLUDED.basic_info,
    size=EXCLUDED.size,
    supplier_id=EXCLUDED.supplier_id,
    category_id=EXCLUDED.category_id,
    price=EXCLUDED.price;

  -- =========================
  -- 14 dias, 2 bills por dia (coherente)
  -- bill.code es INTEGER en tu schema -> usamos rangos altos para no chocar
  -- =========================
  FOR d IN SELECT (current_date - gs.i) FROM generate_series(0,13) AS gs(i)
  LOOP
    code1 := 900000000 + (i*2) + 1;
    code2 := 900000000 + (i*2) + 2;
    i := i + 1;

    base1 := 100; vat1 := 18; disc1 := 0;  tot1 := base1 + vat1 - disc1; -- 118
    base2 := 150; vat2 := 27; disc2 := 10; tot2 := base2 + vat2 - disc2; -- 167

    -- idempotencia por code (sin LIKE)
    DELETE FROM chiru_schema.bills_details WHERE bill_code IN (code1, code2);
    DELETE FROM chiru_schema.bill WHERE code IN (code1, code2);

    -- BILL 1 (NOT NULL: user_id,total,discount,vat,total_amount)
    INSERT INTO chiru_schema.bill
      (code, user_id, total, discount, vat, total_amount, paid_at, payment_status)
    VALUES
      (code1, 1, base1, disc1, vat1, tot1, (d::timestamp + time '11:30'), 'PAID');

    -- BILL 2
    INSERT INTO chiru_schema.bill
      (code, user_id, total, discount, vat, total_amount, paid_at, payment_status)
    VALUES
      (code2, 1, base2, disc2, vat2, tot2, (d::timestamp + time '18:10'), 'PAID');

    -- DETAILS (NOT NULL: quantity numeric)
    -- Bill 1: A x2, B x1
    INSERT INTO chiru_schema.bills_details (bill_code, product, quantity)
    VALUES
      (code1, v_prd_a, 2::numeric),
      (code1, v_prd_b, 1::numeric);

    -- Bill 2: B x3, C x1
    INSERT INTO chiru_schema.bills_details (bill_code, product, quantity)
    VALUES
      (code2, v_prd_b, 3::numeric),
      (code2, v_prd_c, 1::numeric);

  END LOOP;

END $$;

COMMIT;