# POWERBI.md - Modelo mínimo (ventas + GA4) sin romper filtros

## 1) Conectar a PostgreSQL local

- **Qué es:** conector PostgreSQL en Power BI.
- **Para qué sirve:** importar vistas `analytics.*`.
- **Ejemplo corto:** server `localhost`, port `5433`.

### Parámetros
- Server: `localhost`
- Port: `5433`
- Database: `chiru_local`
- Username: `postgres`
- Password: `postgres`

---

## 2) Importar vistas (obligatorias)
- `analytics.v_sales_daily`
- `analytics.v_funnel_daily`
- `analytics.v_product_daily`
- `analytics.v_ga4_event_daily`
- `analytics.v_ga4_funnel_daily`

Regla clave:
- Si GA4 está vacío, estas vistas traerán 0 filas (es normal).

---

## 3) Calendar table (Date) y relaciones 1:*

- **Qué es:** tabla con una fila por fecha.
- **Para qué sirve:** filtros de fecha estables sin medidas raras.
- **Ejemplo corto:** relación 1:* Calendar[Date] -> v_sales_daily[day]

### DAX (ejemplo fijo)
Power BI -> Modeling -> New table:
```DAX
Calendar = CALENDAR(DATE(2026,1,1), DATE(2026,12,31))
```

### Relaciones (Model view)
- Calendar[Date] 1:*  -> v_sales_daily[day]
- Calendar[Date] 1:*  -> v_product_daily[day]
- Calendar[Date] 1:*  -> v_funnel_daily[day]
- Calendar[Date] 1:*  -> v_ga4_event_daily[day]
- Calendar[Date] 1:*  -> v_ga4_funnel_daily[day]

---

## Expected behavior
- Filtros por fecha no rompen.
- GA4 vacío: visuals GA4 vacíos (correcto), ventas siguen funcionando.

