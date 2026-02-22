import csv
import json
import sys
from datetime import date, timedelta
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


OUT_CSV = Path("data/ml_trends_weekly.csv")


def monday_of(d: date) -> date:
    return d - timedelta(days=d.weekday())


def fetch_json(url: str):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "es-PE,es;q=0.9,en;q=0.8",
    }
    req = Request(url, headers=headers)
    with urlopen(req, timeout=30) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    return json.loads(raw)


def write_csv(rows):
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["week_start", "site_id", "category_id", "rank", "keyword", "url"])
        for r in rows:
            w.writerow(r)


def main():
    site_id = "MPE"
    wk = monday_of(date.today()).isoformat()

    # Endpoint principal (Top trends del sitio)
    # Ejemplos públicos usan: https://api.mercadolibre.com/trends/MLA :contentReference[oaicite:2]{index=2}
    url_primary = f"https://api.mercadolibre.com/trends/{site_id}"

    rows = []
    try:
        data = fetch_json(url_primary)

        # Esperado: lista de objetos con "keyword" (y a veces "url")
        # Si el formato cambia, igual no rompemos: dejamos CSV vacio.
        if isinstance(data, list):
            for i, it in enumerate(data[:50], start=1):
                kw = (it.get("keyword") or "").strip() if isinstance(it, dict) else ""
                u = (it.get("url") or "").strip() if isinstance(it, dict) else ""
                if kw:
                    rows.append([wk, site_id, "", i, kw, u])

        print(f"[OK] ML trends fetched. rows={len(rows)}")
        write_csv(rows)
        return 0

    except (HTTPError, URLError) as e:
        # No rompemos pipeline: generamos CSV vacio y salimos OK.
        print(f"[WARN] ML trends fetch failed: {e}. Generando CSV vacio.")
        write_csv([])
        return 0
    except Exception as e:
        print(f"[WARN] ML trends unexpected error: {e}. Generando CSV vacio.")
        write_csv([])
        return 0


if __name__ == "__main__":
    sys.exit(main())