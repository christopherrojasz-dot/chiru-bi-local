import csv
import json
import sys
from datetime import date, timedelta
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


OUT_CSV = Path("data/ml_trends_weekly.csv")
DEBUG_JSON = Path("data/ml_trends_debug.json")


def monday_of(d: date) -> date:
    return d - timedelta(days=d.weekday())


def write_csv(rows):
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["week_start", "site_id", "category_id", "rank", "keyword", "url"])
        for r in rows:
            w.writerow(r)


def write_debug(obj):
    DEBUG_JSON.parent.mkdir(parents=True, exist_ok=True)
    # recorte por seguridad de tamaño
    s = json.dumps(obj, ensure_ascii=False)
    if len(s) > 200000:
        s = s[:200000]
    DEBUG_JSON.write_text(s, encoding="utf-8")


def fetch_json(url: str):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "es-PE,es;q=0.9,en;q=0.8",
        "Connection": "close",
    }
    req = Request(url, headers=headers)
    with urlopen(req, timeout=30) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    return json.loads(raw), {"url": url, "status": 200}


def main():
    site_id = "MPE"
    wk = monday_of(date.today()).isoformat()

    url_primary = f"https://api.mercadolibre.com/trends/{site_id}"

    rows = []

    try:
        data, meta = fetch_json(url_primary)
        write_debug({"meta": meta, "response": data})
        print(f"[OK] Debug guardado: {DEBUG_JSON}")

        # Formato esperado: lista de dicts con keyword (y a veces url)
        if isinstance(data, list):
            for i, it in enumerate(data[:50], start=1):
                if not isinstance(it, dict):
                    continue
                kw = (it.get("keyword") or "").strip()
                u = (it.get("url") or "").strip()
                if kw:
                    rows.append([wk, site_id, "", i, kw, u])

        write_csv(rows)
        print(f"[OK] ML trends fetched. rows={len(rows)} csv={OUT_CSV}")
        return 0

    except HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            body = ""
        write_debug({
            "meta": {"url": url_primary, "status": int(getattr(e, "code", 0))},
            "error": {"type": "HTTPError", "message": str(e), "body": body[:200000]}
        })
        write_csv([])  # CSV vacío pero válido
        print(f"[WARN] ML trends HTTPError: {e}. CSV vacío generado. Debug: {DEBUG_JSON}")
        return 0

    except URLError as e:
        write_debug({
            "meta": {"url": url_primary, "status": None},
            "error": {"type": "URLError", "message": str(e)}
        })
        write_csv([])
        print(f"[WARN] ML trends URLError: {e}. CSV vacío generado. Debug: {DEBUG_JSON}")
        return 0

    except Exception as e:
        write_debug({
            "meta": {"url": url_primary, "status": None},
            "error": {"type": "Exception", "message": str(e)}
        })
        write_csv([])
        print(f"[WARN] ML trends error inesperado: {e}. CSV vacío generado. Debug: {DEBUG_JSON}")
        return 0


if __name__ == "__main__":
    sys.exit(main())