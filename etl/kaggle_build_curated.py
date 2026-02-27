import csv
import re
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta, date
from collections import defaultdict

import psycopg2


RAW_ROOT = Path("data/kaggle_raw")

# Heurísticas de columnas
PRICE_KEYS = ["price", "totalamount", "total_amount", "amount", "total", "payment", "value", "cost"]
DATE_KEYS  = ["event_time", "orderdate", "order_date", "purchase_date", "date", "datetime", "timestamp", "time"]
# Categoría en prioridad: category_code > category > category_name > category_id
CAT_PRIORITY_KEYS = ["category_code", "category", "category_name", "department", "type", "category_id"]

TEXT_KEYS = ["description", "product"]  # para extraer términos útiles

STOPWORDS_EN = set("""
a an and are as at be been by for from has have he her his i in is it its of on or that the to was were with you your
""".split())

# Palabras muy genéricas que te ensucian el diccionario (ajustable)
DOMAIN_STOPWORDS = set("""
details made fabric weight program recycled recyclable common threads recycling polyester cotton organic
""".split())

MAX_ROWS_PER_FILE = 300000          # cap global por archivo grande (mkechinov)
MAX_PRICE_SAMPLES_PER_CAT = 50000   # cap por categoría
TEXT_SAMPLE_ROWS = 2000             # para texto (cclark)

def sniff_delimiter(sample: str) -> str:
    candidates = [",", ";", "\t", "|"]
    counts = {c: sample.count(c) for c in candidates}
    return max(counts, key=counts.get)

def normalize_conn_str(s: str) -> str:
    """
    Acepta:
    - ODBC/ADO: Host=...;Port=...;Username=...;Password=...;Database=...
    - libpq DSN: host=... port=... dbname=... user=... password=...
    - URL: postgresql://user:pass@host:port/db
    Devuelve un string válido para psycopg2.
    """
    if not s:
        return s
    s_strip = s.strip()

    if s_strip.startswith("postgresql://") or s_strip.startswith("postgres://"):
        return s_strip

    if ";" in s_strip:
        parts = [p for p in s_strip.split(";") if p.strip()]
        kv = {}
        for p in parts:
            if "=" not in p:
                continue
            k, v = p.split("=", 1)
            kv[k.strip().lower()] = v.strip()

        host = kv.get("host")
        port = kv.get("port")
        db   = kv.get("database") or kv.get("dbname")
        user = kv.get("username") or kv.get("user")
        pwd  = kv.get("password")

        out = []
        if host: out.append(f"host={host}")
        if port: out.append(f"port={port}")
        if db:   out.append(f"dbname={db}")
        if user: out.append(f"user={user}")
        if pwd:  out.append(f"password={pwd}")
        return " ".join(out)

    return s_strip

def monday_of(d: date) -> date:
    return d - timedelta(days=d.weekday())

def safe_float(x):
    try:
        s = str(x).strip()
        if s == "":
            return None
        s = s.replace(",", ".")
        return float(s)
    except:
        return None

def parse_date_any(x):
    """
    Soporta:
    - 2024-11-11
    - 2020-04-24 11:50:39 UTC
    - 2020-04-24 11:50:39
    - ISO con T/Z
    """
    s = str(x).strip()
    if not s:
        return None

    # Si trae " UTC", lo quitamos
    if s.endswith(" UTC"):
        s = s[:-4].strip()

    # ISO date directo
    if len(s) >= 10 and re.match(r"^\d{4}-\d{2}-\d{2}", s):
        try:
            return datetime.strptime(s[:10], "%Y-%m-%d").date()
        except:
            pass

    # datetime común
    fmts = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
    ]
    for fmt in fmts:
        try:
            return datetime.strptime(s, fmt).date()
        except:
            continue

    # último intento: extrae YYYY-MM-DD
    m = re.search(r"\d{4}-\d{2}-\d{2}", s)
    if m:
        try:
            return datetime.strptime(m.group(0), "%Y-%m-%d").date()
        except:
            return None
    return None

def tokenize(text: str):
    t = re.sub(r"<[^>]+>", " ", text)  # quita html
    t = re.sub(r"[^a-zA-Z0-9 ]+", " ", t).lower()
    words = []
    for w in t.split():
        if 3 <= len(w) <= 20 and w not in STOPWORDS_EN and w not in DOMAIN_STOPWORDS:
            # evita tokens full numéricos tipo "100"
            if w.isdigit():
                continue
            words.append(w)
    return words

def percentile(sorted_vals, p):
    if not sorted_vals:
        return None
    k = (len(sorted_vals) - 1) * p
    f = int(k)
    c = min(f + 1, len(sorted_vals) - 1)
    if f == c:
        return float(sorted_vals[f])
    return float(sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f))

def find_idx(header, key_candidates):
    header_l = [h.lower().strip() for h in header]
    for key in key_candidates:
        key_l = key.lower()
        for i, h in enumerate(header_l):
            if h == key_l or key_l in h:
                return i
    return None

def choose_category_idx(header):
    """
    Devuelve (idx, kind) donde kind indica qué columna ganó:
    - category_code
    - category
    - category_id
    """
    idx_code = find_idx(header, ["category_code"])
    if idx_code is not None:
        return idx_code, "category_code"

    idx_cat = find_idx(header, ["category", "category_name", "department", "type"])
    if idx_cat is not None:
        return idx_cat, "category"

    idx_id = find_idx(header, ["category_id"])
    if idx_id is not None:
        return idx_id, "category_id"

    return None, None

def normalize_category(val: str, kind: str):
    v = (val or "").strip()
    if not v:
        return ""
    if kind == "category_code":
        # ej: electronics.audio.headphone
        return v.lower()
    if kind == "category":
        # ej: Electronics, Fashion
        return v.strip().lower()
    if kind == "category_id":
        return "id:" + v.strip()
    return v.strip().lower()

def upsert_text_terms(conn, rows):
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO analytics.kaggle_text_terms (source, term, term_count)
            VALUES (%s,%s,%s)
            ON CONFLICT (source, term)
            DO UPDATE SET term_count=EXCLUDED.term_count, loaded_at=now();
            """,
            rows,
        )
    conn.commit()

def upsert_price_benchmark(conn, rows):
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO analytics.kaggle_price_benchmark
              (source, category_norm, currency, n_prices, price_p25, price_p50, price_p75)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (source, category_norm, currency)
            DO UPDATE SET
              n_prices=EXCLUDED.n_prices,
              price_p25=EXCLUDED.price_p25,
              price_p50=EXCLUDED.price_p50,
              price_p75=EXCLUDED.price_p75,
              loaded_at=now();
            """,
            rows,
        )
    conn.commit()

def upsert_category_weekly(conn, rows):
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO analytics.kaggle_category_weekly
              (source, week_start, category_norm, n_events, revenue_proxy)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (source, week_start, category_norm)
            DO UPDATE SET
              n_events=EXCLUDED.n_events,
              revenue_proxy=EXCLUDED.revenue_proxy,
              loaded_at=now();
            """,
            rows,
        )
    conn.commit()

def profile_delim_and_header(path: Path):
    text = path.read_text(encoding="utf-8", errors="replace")
    lines = [ln for ln in text.splitlines() if ln.strip()][:50]
    if not lines:
        return ",", []
    delim = sniff_delimiter("\n".join(lines))
    reader = csv.reader(lines, delimiter=delim)
    header = next(reader, None) or []
    header = [h.strip().strip("\ufeff") for h in header]
    return delim, header

def process_file(path: Path, source: str, conn):
    delim, header = profile_delim_and_header(path)
    if not header:
        print(f"[WARN] {source}: sin header -> {path.name}")
        return

    # indices
    i_price = find_idx(header, ["totalamount", "total_amount"])
    if i_price is None:
        i_price = find_idx(header, ["price"])  # fallback

    i_date = find_idx(header, DATE_KEYS)

    i_cat, cat_kind = choose_category_idx(header)

    # Texto: description o product
    i_desc = find_idx(header, ["description"])
    i_prod = find_idx(header, ["product"])

    # 1) Extraer términos (si aplica)
    if i_desc is not None or i_prod is not None:
        idx_text = i_desc if i_desc is not None else i_prod
        term_counts = defaultdict(int)

        with path.open("r", encoding="utf-8", errors="replace", newline="") as f:
            r = csv.reader(f, delimiter=delim)
            _ = next(r, None)  # skip header
            n = 0
            for row in r:
                if len(row) <= idx_text:
                    continue
                words = tokenize(row[idx_text])
                for w in words:
                    term_counts[w] += 1
                n += 1
                if n >= TEXT_SAMPLE_ROWS:
                    break

        top_terms = sorted(term_counts.items(), key=lambda x: x[1], reverse=True)[:200]
        upsert_text_terms(conn, [(source, t, c) for t, c in top_terms])
        print(f"[OK] {source}: text_terms upsert {len(top_terms)}")

        # cclark solo tiene texto; no aporta category/price/date
        if source == "cclark_product_item_data":
            return

    # 2) Si no hay categoría, no podemos hacer benchmark
    if i_cat is None:
        print(f"[WARN] {source}: no encontre categoria -> skip benchmark ({path.name})")
        return

    prices_by_cat = defaultdict(list)
    weekly_counts = defaultdict(int)
    weekly_rev = defaultdict(float)

    with path.open("r", encoding="utf-8", errors="replace", newline="") as f:
        r = csv.reader(f, delimiter=delim)
        _ = next(r, None)  # header

        n_seen = 0
        for row in r:
            n_seen += 1
            # normaliza largo
            if len(row) < len(header):
                row = row + [""] * (len(header) - len(row))

            cat_val = normalize_category(row[i_cat], cat_kind)
            if not cat_val:
                continue

            # price benchmark
            if i_price is not None:
                pv = safe_float(row[i_price])
                if pv is not None:
                    lst = prices_by_cat[cat_val]
                    if len(lst) < MAX_PRICE_SAMPLES_PER_CAT:
                        lst.append(pv)

            # weekly demand
            if i_date is not None:
                d = parse_date_any(row[i_date])
                if d is not None:
                    wk = monday_of(d).isoformat()
                    key = (wk, cat_val)
                    weekly_counts[key] += 1
                    if i_price is not None:
                        pv = safe_float(row[i_price])
                        if pv is not None:
                            weekly_rev[key] += pv

            if n_seen >= MAX_ROWS_PER_FILE:
                break

    # upsert price benchmark
    bench_rows = []
    for cat, vals in prices_by_cat.items():
        if len(vals) >= 30:
            svals = sorted(vals)
            p25 = percentile(svals, 0.25)
            p50 = percentile(svals, 0.50)
            p75 = percentile(svals, 0.75)
            bench_rows.append((source, cat, "", len(vals), p25, p50, p75))

    upsert_price_benchmark(conn, bench_rows)
    print(f"[OK] {source}: price_benchmark upsert {len(bench_rows)}")

    # upsert weekly demand
    weekly_rows = []
    for (wk, cat), cnt in weekly_counts.items():
        rev = weekly_rev.get((wk, cat), None)
        weekly_rows.append((source, wk, cat, cnt, rev if rev != 0.0 else None))

    upsert_category_weekly(conn, weekly_rows)
    print(f"[OK] {source}: category_weekly upsert {len(weekly_rows)}")

def main():
    conn_str = os.environ.get("NEON_CONNECTION_STRING")
    if not conn_str:
        print("Falta NEON_CONNECTION_STRING", file=sys.stderr)
        sys.exit(2)

    csvs = sorted(RAW_ROOT.rglob("*.csv"))
    if not csvs:
        print("No hay CSV en data/kaggle_raw", file=sys.stderr)
        sys.exit(1)

    conn = psycopg2.connect(normalize_conn_str(conn_str))
    try:
        for p in csvs:
            # source = nombre de carpeta del dataset (como tus rutas reales)
            source = p.parent.name.lower()
            process_file(p, source, conn)
    finally:
        conn.close()

    print("[OK] Kaggle curated build completo.")

if __name__ == "__main__":
    main()