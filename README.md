# CHIRU BI Local (Postgres + Analytics + GA4) - Repo listo para empresa (Windows CMD)

## Qué es (definición simple)
Un proyecto **reproducible** que levanta PostgreSQL local en Docker y prepara una **única base de datos** (`chiru_local`) con 3 schemas:

- `chiru_schema`: simula el esquema de QA (restaurado desde Neon “fake QA”).
- `analytics`: vistas/marts para Power BI.
- `ga4`: tablas agregadas diarias importadas desde GA4 (si no hay eventos, queda vacío sin fallar).

## Para qué sirve
- Trabajar localmente sin acceso a QA real.
- Entregar al equipo un paquete que pueden replicar en QA (mismo SQL + ETL + docs).
- Conectar Power BI a vistas listas para dashboard.

## Requisitos
- Windows 10/11
- Docker Desktop
- CMD (NO PowerShell)
- (Opcional GA4) Python 3.10+ en PATH

## Estructura
- `cmd/` scripts CMD para correr todo.
- `dumps/` dump de schema desde Neon (`neon_schema.sql`) + fallback demo (`minimal_schema.sql`).
- `sql/` patch analytics+ga4, seed mínimo, sanity checks.
- `etl/` ETL GA4 (Python) + wrapper CMD con logging.
- `docs/` documentación.

---

## Ejecución local (rápida)

### 1) Levantar Postgres (Docker)
**Comando CMD**
```
cmd\01_docker_up_local.cmd
```
**Expected output**
- `[OK] Postgres local listo en localhost:5433`

### 2) Reset completo (drop/create DB + restore + patch + seed + checks)
**Comando CMD**
```
cmd\02_reset_db_and_restore.cmd
```
**Expected output**
En el output de sanity checks:
- `sales_daily_rows` > 0
- `product_daily_rows` > 0

### 3) (Opcional) GA4 sync una vez
**Comando CMD**
```
cmd\00_install_python_deps.cmd
cmd\04_ga4_sync_once.cmd
```
**Expected output**
- Si GA4 vacío: `GA4 devolvio 0 filas ... Termina OK.`
- Si hay data: `Upsert completado ...`

---

## “Tal cual Neon”: dumps/neon_schema.sql (obligatorio en empresa)
Este repo trae `dumps/neon_schema.sql` como **PLACEHOLDER**.

Tienes 2 opciones:

A) Ya tienes el dump real:
- Copia tu dump schema-only real a: `dumps\neon_schema.sql`

B) Generarlo desde Neon (ejemplo con placeholders):
```
cmd\10_generate_neon_schema_dump.cmd
```

Si NO reemplazas `neon_schema.sql`, el reset usará `dumps\minimal_schema.sql` (DEMO) para que el repo corra igual.

---

## Documentación
- `docs/LOCAL_SETUP.md`
- `docs/GA4_SETUP.md`
- `docs/POWERBI.md`
- `docs/QA_HANDOFF.md`
- `docs/TROUBLESHOOTING.md`
