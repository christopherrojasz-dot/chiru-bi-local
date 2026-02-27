import csv
from pathlib import Path

RAW_ROOT = Path("data/kaggle_raw")
OUT_MD = Path("docs/KAGGLE_SCHEMA_SNAPSHOT.md")

def sniff_delimiter(sample: str) -> str:
    # detecta separador común
    candidates = [",", ";", "\t", "|"]
    counts = {c: sample.count(c) for c in candidates}
    return max(counts, key=counts.get)

def read_header_and_rows(path: Path, nrows: int = 5):
    text = path.read_text(encoding="utf-8", errors="replace")
    lines = [ln for ln in text.splitlines() if ln.strip()][:200]
    if not lines:
        return None, None, None

    delim = sniff_delimiter("\n".join(lines[:20]))
    reader = csv.reader(lines, delimiter=delim)
    header = next(reader, None)
    if not header:
        return delim, None, None

    header = [h.strip().strip("\ufeff") for h in header]
    rows = []
    for row in reader:
        if row and any(str(x).strip() for x in row):
            # normaliza largo
            if len(row) < len(header):
                row = row + [""] * (len(header) - len(row))
            rows.append([str(x).strip() for x in row[:len(header)]])
        if len(rows) >= nrows:
            break
    return delim, header, rows

def md_escape(s: str) -> str:
    return s.replace("|", "\\|").replace("\n", " ").replace("\r", " ")

def main():
    if not RAW_ROOT.exists():
        raise SystemExit("Falta carpeta data/kaggle_raw")

    files = sorted(RAW_ROOT.rglob("*.csv"))
    if not files:
        raise SystemExit("No encontre CSV en data/kaggle_raw")

    out = []
    out.append("# Kaggle - Schema Snapshot (auto)\n\n")
    out.append(f"Archivos detectados: {len(files)}\n\n")

    for p in files:
        delim, header, rows = read_header_and_rows(p, nrows=5)
        out.append(f"## {p.name}\n\n")
        out.append(f"- Ruta: `{p.as_posix()}`\n")
        out.append(f"- Delimitador detectado: `{delim}`\n")
        if not header:
            out.append("- ERROR: no se detecto header\n\n")
            continue

        out.append(f"- Columnas ({len(header)}):\n")
        for c in header:
            out.append(f"  - {md_escape(c)}\n")
        out.append("\n")

        # tabla con muestra
        out.append("### Muestra (primeras 5 filas)\n\n")
        out.append("| " + " | ".join(md_escape(c) for c in header[:8]) + " |\n")
        out.append("|" + "|".join(["---"] * min(8, len(header))) + "|\n")
        for r in rows:
            out.append("| " + " | ".join(md_escape(x) for x in r[:8]) + " |\n")
        if len(header) > 8:
            out.append("\nNota: se muestran solo las primeras 8 columnas para no saturar.\n")
        out.append("\n---\n\n")

    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_MD.write_text("".join(out), encoding="utf-8")
    print(f"[OK] Generado: {OUT_MD}")

if __name__ == "__main__":
    main()