# GA4_SETUP.md - Property ID + OAuth Desktop + Token + Automatización

## Objetivo
Cargar agregados diarios de GA4 en:
- `ga4.event_daily`
- `ga4.funnel_daily`

Regla clave:
- Si la propiedad está vacía (sin eventos), el ETL **NO falla**: deja 0 filas y termina OK.

---

## 1) Property ID

- **Qué es:** un número que identifica tu GA4 Property (ej: `123456789`).
- **Para qué sirve:** la API usa `properties/{PROPERTY_ID}`.
- **Ejemplo corto:** `GA4_PROPERTY_ID=123456789`

### Variable (CMD)
```
set "GA4_PROPERTY_ID=TU_PROPERTY_ID"
```

---

## 2) OAuth Desktop client (Google Cloud)

- **Qué es:** credencial OAuth tipo Desktop (archivo JSON).
- **Para qué sirve:** autenticación para leer GA4 con Data API.
- **Ejemplo corto:** `ga4-oauth-client.json`

### Ruta recomendada (local)
```
C:\Users\%USERNAME%\secrets\ga4-oauth-client.json
```

---

## 3) Token

- **Qué es:** JSON con refresh token.
- **Para qué sirve:** evitar login manual en cada corrida.
- **Ejemplo corto:** `ga4-token.json`

### Ruta recomendada
```
C:\Users\%USERNAME%\secrets\ga4-token.json
```

---

## 4) Ejecutar ETL una vez

- **Qué es:** corre `etl\ga4_to_local.py`.
- **Para qué sirve:** upsert en `ga4.*`.
- **Ejemplo corto:** lookback 30 días.

### Comando exacto CMD
```
cmd\00_install_python_deps.cmd
cmd\04_ga4_sync_once.cmd
```

### Expected output
- Si GA4 está vacío: `GA4 devolvio 0 filas ... Termina OK.`
- Si hay data: `Upsert completado ...`

### Si falla: solución directa
- `invalid_grant`: borrar token y reautenticar.
- 403: pedir acceso de lectura a la propiedad (no credenciales).

---

## Automatización

### A) Local gratis: Task Scheduler (Windows)

- **Qué es:** programador de tareas de Windows.
- **Para qué sirve:** ejecutar ETL diario sin correrlo a mano.
- **Ejemplo corto:** ejecutar `cmd.exe /c ...ga4_sync_daily.cmd`

**Pasos**
1) Abrir Task Scheduler -> Create Task...
2) Trigger: Daily (ej 07:30).
3) Action:
   - Program/script: `cmd.exe`
   - Add arguments:
     ```
     /c "C:\RUTA\DEL\REPO\etl\ga4_sync_daily.cmd --lookback 30"
     ```
   - Start in: `C:\RUTA\DEL\REPO`

**Expected output**
- Log en `etl\ga4_sync.log`.

### B) Empresa (sin PC prendida)

Opciones limpias:
- Cron en VM/servidor
- GitHub Actions schedule
- Cloud Run Jobs + Scheduler

Variables que deben configurar:
- `NEON_CONNECTION_STRING`
- `GA4_PROPERTY_ID`
- `GA4_CLIENT`
- `GA4_TOKEN`

