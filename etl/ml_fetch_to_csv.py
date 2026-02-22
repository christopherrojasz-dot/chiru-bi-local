import csv
import json
import sys
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import urlencode
import urllib.request


KW_SOURCE = Path("data/trends_weekly.csv")   # usamos tus keywords ya consolidadas
OUT_CSV = Path("data/ml_weekly.csv")


def monday_of(d: date) -> date:
    return d - timedelta(days=d.weekday())


def pct(values, p: float):
    if not values:
        return None
    v = sorted(values)
    k = (len(v) - 1) * p
    f = int(k)
    c = min(f + 1, len(v) - 1)
    if f == c:
        return float(v[f])
    return float(v[f] + (v[c] - v[f]) * (k - f))


def read_keywords_from_trends(limit: int = 10):
    if not KW_SOURCE.exists():
        raise SystemExit("Falta data/trends_weekly.csv (fuente de keywords)")
    kws = []
    with KW_SOURCE.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            kw = (row.get("keyword_canonica") or "").strip()
            if kw and kw.lower() not in [k.lower() for k in kws]:
                kws.append(kw)
            if len(kws) >= limit:
                break
    return kws


def fetch_ml(site_id: str, query: str, limit: int = 50):
    url = f"https://api.mercadolibre.com/sites/{site_id}/search?{urlencode({'q': query, 'limit': str(limit)})}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "es-PE,es;q=0.9,en;q=0.8",
        "X-Requested-With": "XMLHttpRequest",
    }
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    return json.loads(raw)


def main():
    site_id = "MPE"
    wk = monday_of(date.today()).isoformat()

    keywords = read_keywords_from_trends(limit=10)
    if not keywords:
        print("[OK] No hay keywords para ML.")
        return 0

    rows = []
    for kw in keywords:
        data = fetch_ml(site_id, kw, limit=50)
        total = int(data.get("paging", {}).get("total", 0))
        items = data.get("results") or []

        prices = []
        for it in items:
            p = it.get("price")
            if isinstance(p, (int, float)):
                prices.append(float(p))

        p25 = pct(prices, 0.25)
        p50 = pct(prices, 0.50)
        p75 = pct(prices, 0.75)

        top_cat_id = None
        top_cat_name = None
        filters = data.get("filters") or []
        for f in filters:
            if f.get("id") == "category":
                vals = f.get("values") or []
                if vals:
                    top_cat_id = vals[0].get("id")
                    top_cat_name = vals[0].get("name")
                    break

        rows.append([wk, kw, site_id, total, p50, p25, p75, top_cat_id, top_cat_name])
        print(f"ML OK: kw='{kw}' total={total} n_prices={len(prices)}")

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "week_start","keyword_canonica","site_id","results_total",
            "price_median","price_p25","price_p75","top_category_id","top_category_name"
        ])
        for r in rows:
            w.writerow(r)

    print(f"[OK] CSV generado: {OUT_CSV} filas={len(rows)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())