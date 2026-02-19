# QA_HANDOFF.md - Entrega a equipo (QA)

## Objetivo
Replicar en QA:
- `ga4.*` (tablas)
- `analytics.*` (vistas para Power BI)
- Job diario GA4 (si GA4 vacío, no falla)

Regla:
- No crear 2 BDs. Se usa la BD de QA con schemas.

---

## Qué correr en QA (SQL)
Ejecutar en la BD de QA:
- `sql/90_local_patch.sql`

---

## ETL GA4 en QA (job)
Script:
- `etl/ga4_to_local.py`

### Variables (placeholders)
Ejemplo (Windows CMD):
```
set "NEON_CONNECTION_STRING=Host=TU_HOST;Port=5432;Username=TU_USER;Password=TU_PASS;Database=TU_DB;Ssl Mode=Require;"
set "GA4_PROPERTY_ID=TU_PROPERTY_ID"
set "GA4_CLIENT=C:\ruta\segura\ga4-oauth-client.json"
set "GA4_TOKEN=C:\ruta\segura\ga4-token.json"
```

---

## Checklist
- [ ] Schema de app existe en QA (TypeORM).
- [ ] Ejecutar `sql/90_local_patch.sql`.
- [ ] Validar vistas:
  - [ ] `SELECT COUNT(*) FROM analytics.v_sales_daily;`
  - [ ] `SELECT COUNT(*) FROM analytics.v_product_daily;`
- [ ] Configurar OAuth GA4 (client + token) como secretos.
- [ ] Programar ETL diario.
- [ ] Validar tablas GA4:
  - [ ] `SELECT COUNT(*) FROM ga4.event_daily;`
  - [ ] `SELECT COUNT(*) FROM ga4.funnel_daily;`

