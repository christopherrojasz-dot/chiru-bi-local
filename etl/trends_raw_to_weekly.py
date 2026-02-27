import csv
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from collections import defaultdict

RAW_DIR = Path("data/trends_raw")
OUT_CSV = Path("data/trends_weekly.csv")

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

def to_monday(d) -> str:
    monday = d - timedelta(days=d.weekday())
    return monday.isoformat()

def parse_any_datetime(s: str):
    s = s.strip().strip('"').lstrip("\ufeff")
    if not s:
        return None

    # Caso 1: YYYY-MM-DD
    if DATE_RE.match(s):
        return datetime.strptime(s, "%Y-%m-%d").date()

    # Caso 2: ISO timestamp con Z (UTC) o con offset
    # Ej: 2026-02-21T16:32:00Z
    # Ej: 2026-02-21T16:32:00+00:00
    if "T" in s:
        try:
            if s.endswith("Z"):
                s2 = s[:-1] + "+00:00"
            else:
                s2 = s
            dt = datetime.fromisoformat(s2)
            return dt.date()
        except:
            return None

    return None

def detect_header_line(lines):
    """
    Devuelve (start_idx, delimiter, kind)
    kind: 'week' si header es Semana/Week/Date, 'time' si header es Time/Hora
    """
    for i, line in enumerate(lines):
        raw = line.strip()
        if not raw:
            continue

        # Detect delimiter
        delim = None
        if ";" in raw and raw.count(";") >= 1:
            delim = ";"
        elif "," in raw and raw.count(",") >= 1:
            delim = ","
        else:
            continue

        first_cell = raw.split(delim, 1)[0].strip().strip('"').lstrip("\ufeff").lower()

        if first_cell in ("semana", "week", "date"):
            return i, delim, "week"
        if first_cell in ("time", "hora"):
            return i, delim, "time"

    return None, None, None

def normalize_keyword(header_cell: str) -> str:
    s = header_cell.strip().strip('"').lstrip("\ufeff")
    # A veces viene como "celular: (Perú)" o similar
    if ":" in s:
        s = s.split(":", 1)[0].strip()
    return s

def parse_trends_file(path: Path):
    text = path.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()

    start_idx, delim, kind = detect_header_line(lines)
    if start_idx is None:
        raise ValueError("No pude encontrar header (Semana/Week/Date o Time/Hora).")

    reader = csv.reader(lines[start_idx:], delimiter=delim)
    header = next(reader, None)
    if not header or len(header) < 2:
        raise ValueError(f"Header inválido: {header}")

    kw = normalize_keyword(header[1])

    points = []  # (date, interest)
    for row in reader:
        if not row or len(row) < 2:
            continue

        t_raw = row[0].strip()
        v_raw = row[1].strip()

        d = parse_any_datetime(t_raw)
        if d is None:
            # si ya había empezado a leer puntos y ahora no, cortamos bloque
            if points:
                break
            continue

        if v_raw.strip('"') == "<1":
            val = 0
        else:
            try:
                val = int(float(v_raw.strip().strip('"')))
            except:
                continue

        # clamp 0..100
        if val < 0:
            val = 0
        if val > 100:
            val = 100

        points.append((d, val))

    if not points:
        return []

    # Agregación semanal: promedio por semana
    agg = defaultdict(lambda: [0, 0])  # key -> [sum, count]
    for d, val in points:
        wk = to_monday(d)
        key = (wk, kw, "PE", "")
        agg[key][0] += val
        agg[key][1] += 1

    rows = []
    for (wk, kw, geo, region), (s, c) in agg.items():
        interest = int(round(s / c))
        if interest < 0:
            interest = 0
        if interest > 100:
            interest = 100
        rows.append((wk, kw, geo, region, interest))

    return rows

def main():
    if not RAW_DIR.exists():
        raise SystemExit("Falta carpeta data/trends_raw")

    files = sorted(RAW_DIR.glob("*.csv"))
    if not files:
        raise SystemExit("No hay CSV en data/trends_raw")

    all_rows = []
    warns = 0

    for f in files:
        try:
            all_rows.extend(parse_trends_file(f))
        except Exception as e:
            warns += 1
            print(f"[WARN] {f.name}: {e}")

    if not all_rows:
        raise SystemExit("No se pudo extraer ninguna fila de Trends")

    # Dedup por PK lógica
    dedup = {}
    for r in all_rows:
        key = (r[0], r[1].lower(), r[2], r[3])
        dedup[key] = r

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", newline="", encoding="utf-8") as out:
        w = csv.writer(out)
        w.writerow(["week_start", "keyword_canonica", "geo", "region", "interest"])
        for _, r in sorted(dedup.items(), key=lambda x: (x[0][0], x[0][1])):
            w.writerow(r)

    print(f"[OK] trends_weekly.csv generado. filas={len(dedup)} archivos={len(files)} warns={warns}")

if __name__ == "__main__":
    main()