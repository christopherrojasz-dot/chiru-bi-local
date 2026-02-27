import argparse
import json
import os
import sys
from datetime import date, timedelta
from urllib.parse import urlencode
import urllib.request

import psycopg2

def normalize_conn_str(s: str) -> str:
    """
    Acepta:
    - 'Host=localhost;Port=5433;Username=postgres;Password=postgres;Database=chiru_local'
    - 'host=... port=... user=... password=... dbname=...'
    - 'postgresql://user:pass@host:port/db'
    Devuelve un connection string compatible con psycopg2.
    """
    if not s:
        return s

    s_strip = s.strip()

    # Si ya es URL, devolver tal cual
    if s_strip.startswith("postgresql://") or s_strip.startswith("postgres://"):
        return s_strip

    # Si parece DSN libpq (tiene 'host=' o 'dbname='), devolver tal cual
    if "host=" in s_strip.lower() or "dbname=" in s_strip.lower():
        return s_strip

    # Parse estilo 'Key=Value;Key=Value;'
    parts = [p for p in s_strip.split(";") if p.strip()]
    kv = {}
    for p in parts:
        if "=" not in p:
            continue
        k, v = p.split("=", 1)
        kv[k.strip().lower()] = v.strip()

    # Mapear a libpq
    host = kv.get("host")
    port = kv.get("port")
    db   = kv.get("database") or kv.get("dbname")
    user = kv.get("username") or kv.get("user")
    pwd  = kv.get("password")

    # Construir DSN libpq
    out = []
    if host: out.append(f"host={host}")
    if port: out.append(f"port={port}")
    if db:   out.append(f"dbname={db}")
    if user: out.append(f"user={user}")
    if pwd:  out.append(f"password={pwd}")

    return " ".join(out)


def monday_of(d: date) -> date:
    return d - timedelta(days=d.weekday())


def pct(values, p: float):
    if not values:
        return None
    values_sorted = sorted(values)
    k = (len(values_sorted) - 1) * p
    f = int(k)
    c = min(f + 1, len(values_sorted) - 1)
    if f == c:
        return float(values_sorted[f])
    return float(values_sorted[f] + (values_sorted[c] - values_sorted[f]) * (k - f))


def fetch_ml(site_id: str, query: str, limit: int = 50):
    params = {"q": query, "limit": str(limit)}
    url = f"https://api.mercadolibre.com/sites/{site_id}/search?{urlencode(params)}"

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "es-PE,es;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Connection": "close",
        "X-Requested-With": "XMLHttpRequest",
    }

    # Retry con backoff (403/429/5xx)
    import time
    from urllib.error import HTTPError, URLError

    last_err = None
    for attempt in range(1, 6):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
            return json.loads(raw)
        except HTTPError as e:
            last_err = e
            code = getattr(e, "code", None)
            if code in (403, 429, 500, 502, 503, 504):
                # Backoff: 1s,2s,4s,8s,16s + jitter
                sleep_s = min(16, 2 ** (attempt - 1))
                time.sleep(sleep_s)
                continue
            raise
        except URLError as e:
            last_err = e
            time.sleep(min(16, 2 ** (attempt - 1)))
            continue

    raise last_err


def load_keywords_from_db(conn, limit: int):
    # Tomamos canónicas con mejor prioridad (1 primero), y que existan en dim
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT keyword_canonica
            FROM analytics.dim_keyword_categoria
            GROUP BY 1
            ORDER BY MIN(prioridad) ASC, COUNT(*) DESC, keyword_canonica ASC
            LIMIT %s
            """,
            (limit,),
        )
        return [r[0] for r in cur.fetchall()]


def upsert_weekly(conn, rows):
    if not rows:
        print("[OK] ML: no hay filas para upsert (OK).")
        return
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO analytics.competitor_ml_weekly
              (week_start, keyword_canonica, site_id, results_total,
               price_median, price_p25, price_p75, top_category_id, top_category_name)
            VALUES
              (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (week_start, keyword_canonica, site_id)
            DO UPDATE SET
              results_total = EXCLUDED.results_total,
              price_median = EXCLUDED.price_median,
              price_p25 = EXCLUDED.price_p25,
              price_p75 = EXCLUDED.price_p75,
              top_category_id = EXCLUDED.top_category_id,
              top_category_name = EXCLUDED.top_category_name,
              loaded_at = now();
            """,
            rows,
        )
    conn.commit()
    print(f"[OK] ML: upsert semanal OK. filas={len(rows)}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--site", default="MPE", help="Site ID Mercado Libre (Peru=MPE)")
    ap.add_argument("--limit-items", type=int, default=50, help="Items por keyword para muestreo de precios")
    ap.add_argument("--kw-limit", type=int, default=10, help="Cantidad de keywords a procesar (desde dim_keyword_categoria)")
    ap.add_argument("--week-start", default=None, help="YYYY-MM-DD (lunes). Si no, usa lunes de la semana actual")
    args = ap.parse_args()

    conn_str = os.environ.get("NEON_CONNECTION_STRING") or os.environ.get("LOCAL_CONNECTION_STRING")
    if not conn_str:
        print("Falta variable NEON_CONNECTION_STRING (conexion a Postgres local).", file=sys.stderr)
        sys.exit(2)

    if args.week_start:
        wk = date.fromisoformat(args.week_start)
    else:
        wk = monday_of(date.today())

    conn = psycopg2.connect(normalize_conn_str(conn_str))
    try:
        keywords = load_keywords_from_db(conn, args.kw_limit)
        if not keywords:
            print("[OK] ML: no hay keywords en dim_keyword_categoria.")
            return

        out_rows = []
        for kw in keywords:
            try:
                data = fetch_ml(args.site, kw, limit=args.limit_items)
                results_total = int(data.get("paging", {}).get("total", 0))
                items = data.get("results", []) or []

                prices = []
                top_cat_id = None
                top_cat_name = None

                # Tomamos precios de items que tengan price
                for it in items:
                    p = it.get("price", None)
                    if isinstance(p, (int, float)):
                        prices.append(float(p))

                # categoría top (si viene)
                if items:
                    top_cat_id = items[0].get("category_id")
                # El endpoint puede incluir "filters" con categorías; intentamos inferir
                filters = data.get("filters", []) or []
                for f in filters:
                    if f.get("id") == "category":
                        vals = f.get("values") or []
                        if vals:
                            top_cat_id = vals[0].get("id") or top_cat_id
                            top_cat_name = vals[0].get("name") or top_cat_name

                p25 = pct(prices, 0.25)
                p50 = pct(prices, 0.50)
                p75 = pct(prices, 0.75)

                out_rows.append(
                    (
                        wk.isoformat(),
                        kw,
                        args.site,
                        results_total,
                        p50,
                        p25,
                        p75,
                        top_cat_id,
                        top_cat_name,
                    )
                )

                print(f"ML OK: kw='{kw}' total={results_total} n_prices={len(prices)}")
            except Exception as e:
                print(f"[WARN] ML fallo kw='{kw}': {e}")

        upsert_weekly(conn, out_rows)
    finally:
        conn.close()


if __name__ == "__main__":
    main()