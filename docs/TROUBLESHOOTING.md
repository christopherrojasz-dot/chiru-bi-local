# TROUBLESHOOTING.md - Errores típicos y fixes directos

## Docker / Postgres

### Docker no disponible
**Causa:** Docker Desktop apagado.  
**Fix:** abrir Docker Desktop y reintentar `cmd\01_docker_up_local.cmd`.

### Puerto 5433 ocupado (Bind failed)
**Fix:** cambiar `PORT=5433` en `cmd\01_docker_up_local.cmd` (ej 5434) y actualizar:
- `etl\ga4_sync_daily.cmd` (Port)
- Power BI (Port)

---

## Restore / Schema

### relation "chiru_schema.bill" does not exist
**Causa:** el dump restauró tablas en `public` u otro schema.  
**Fix:** correr `cmd\02_reset_db_and_restore.cmd` (incluye patch que intenta mover `public -> chiru_schema`).

### Errores por OWNER/privileges
**Fix:** generar dump con `--no-owner --no-privileges`.

---

## Seed

### NOT NULL violation
**Causa:** el schema real tiene columnas NOT NULL sin default.  
**Fix:** editar `sql\95_seed_minimal.sql` para incluir esas columnas con valores dummy.

### FK violation (bills_details.product)
**Causa:** el product UUID no existe o el FK apunta a otra tabla/columna.  
**Fix:** verificar que `chiru_schema.product(id)` exista y coincida con FK.

---

## GA4

### GA4_PROPERTY_ID no configurado (placeholder)
**Fix:** setear en `etl\ga4_sync_daily.cmd` o como variable del job.

### invalid_grant
**Fix:** borrar `C:\Users\%USERNAME%\secrets\ga4-token.json` y reintentar.

### 403 permission denied
**Fix:** pedir acceso de lectura en GA4 Property (sin pedir credenciales).

