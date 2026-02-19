# LOCAL_SETUP.md (Windows CMD) - Paso a paso, reproducible

## Objetivo
Tener **una sola base de datos** local (`chiru_local`) con:
- `chiru_schema` (QA fake restaurado desde Neon)
- `analytics` (vistas para Power BI)
- `ga4` (tablas agregadas GA4)

---

## Paso 0: Conceptos mínimos (para no improvisar)

### Docker
- **Qué es:** un sistema para correr software “empaquetado” (contenedores).
- **Para qué sirve:** todos usan el mismo Postgres 17.
- **Ejemplo corto:** `docker run postgres:17`

### Schema (Postgres)
- **Qué es:** un “namespace” dentro de una BD (como una carpeta de tablas).
- **Para qué sirve:** separar app (`chiru_schema`) de BI (`analytics`) y eventos (`ga4`).
- **Ejemplo corto:** `SELECT * FROM analytics.v_sales_daily;`

---

## Paso 1: Levantar Postgres 17 en Docker (puerto 5433)

- **Qué es:** un contenedor con PostgreSQL 17.
- **Para qué sirve:** BD local reproducible.
- **Ejemplo corto:** `localhost:5433`

### Comando exacto (CMD)
```
cmd\01_docker_up_local.cmd
```

### Expected output
- `[OK] Postgres local listo en localhost:5433`

### Si falla: solución directa
- **Docker apagado:** abre Docker Desktop.
- **Puerto ocupado:** edita `cmd\01_docker_up_local.cmd` y cambia `PORT=5433`.

---

## Paso 2: Reset completo (drop/create DB + restore + patch + seed + checks)

- **Qué es:** script “todo en uno”.
- **Para qué sirve:** reproducibilidad total (sin pasos manuales).
- **Ejemplo corto:** crea DB `chiru_local` y deja vistas listas.

### Comando exacto (CMD)
```
cmd\02_reset_db_and_restore.cmd
```

### Expected output (mínimo)
En el output de `sql\99_sanity_checks.sql`:
- `sales_daily_rows` > 0
- `product_daily_rows` > 0

### Si falla: solución directa
- **Restore falla:** reemplaza `dumps\neon_schema.sql` por dump real.
- **Seed falla por NOT NULL:** ajusta `sql\95_seed_minimal.sql`.

---

## Paso 3: Sanity checks (cuando quieras)

- **Qué es:** un script que ejecuta `sql\99_sanity_checks.sql`.
- **Para qué sirve:** validar que todo está OK.
- **Ejemplo corto:** conteos y 5 filas de muestra.

### Comando exacto (CMD)
```
cmd\03_sanity_checks.cmd
```

### Expected output
- `[OK] Sanity OK.`

---

## Generar dump schema-only desde Neon (sin credenciales en repo)

- **Qué es:** `pg_dump --schema-only` contra Neon (QA fake).
- **Para qué sirve:** que el local quede “tal cual Neon”.
- **Ejemplo corto:** genera `dumps\neon_schema.sql`.

### Comando exacto (CMD) con placeholders
```
cmd\10_generate_neon_schema_dump.cmd
```

### Si falla
- `pg_dump` no existe: instala **PostgreSQL client tools** y agrega al PATH.

