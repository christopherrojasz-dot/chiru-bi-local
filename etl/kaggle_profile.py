import os
import csv
from pathlib import Path

RAW_ROOT = Path("data/kaggle_raw")
OUT_MD = Path("docs/KAGGLE_INVENTORY.md")

def human_size(n):
    for u in ["B","KB","MB","GB"]:
        if n < 1024:
            return f"{n:.1f} {u}"
        n /= 1024
    return f"{n:.1f} TB"

def sniff_delimiter(sample_line: str):
    if sample_line.count(";") > sample_line.count(","):
        return ";"
    return ","

def profile_csv(path: Path, max_rows: int = 2000):
    size = path.stat().st_size
    with path.open("r", encoding="utf-8", errors="replace", newline="") as f:
        first = f.readline()
        if not first:
            return {"file": str(path), "size": size, "error": "archivo vacio"}
        delim = sniff_delimiter(first)
        f.seek(0)
        reader = csv.reader(f, delimiter=delim)
        header = next(reader, None)
        if not header:
            return {"file": str(path), "size": size, "error": "sin header"}
        header = [h.strip().strip("\ufeff") for h in header]

        n = 0
        nulls = [0] * len(header)
        for row in reader:
            if not row:
                continue
            n += 1
            # normalizar largo
            if len(row) < len(header):
                row = row + [""] * (len(header) - len(row))
            for i, v in enumerate(row[:len(header)]):
                if (v is None) or (str(v).strip() == ""):
                    nulls[i] += 1
            if n >= max_rows:
                break

        null_pct = [(header[i], round(100.0 * nulls[i] / max(n, 1), 2)) for i in range(len(header))]
        null_pct.sort(key=lambda x: x[1], reverse=True)

    return {
        "file": str(path),
        "size": size,
        "delimiter": delim,
        "columns": header,
        "sampled_rows": n,
        "null_pct_top": null_pct[:10],
    }

def main():
    if not RAW_ROOT.exists():
        raise SystemExit("Falta data/kaggle_raw. Crea carpetas y coloca los CSV.")

    csvs = sorted(RAW_ROOT.rglob("*.csv"))
    if not csvs:
        raise SystemExit("No encontre ningun .csv dentro de data/kaggle_raw.")

    lines = []
    lines.append("# KAGGLE INVENTORY (AUTO)\n")
    lines.append(f"CSV detectados: {len(csvs)}\n")

    for p in csvs:
        info = profile_csv(p)
        lines.append(f"## {p.name}\n")
        lines.append(f"- Ruta: `{info['file']}`\n")
        lines.append(f"- Tamaño: {human_size(info['size'])}\n")
        if "error" in info:
            lines.append(f"- ERROR: {info['error']}\n\n")
            continue
        lines.append(f"- Delimitador detectado: `{info['delimiter']}`\n")
        lines.append(f"- Filas muestreadas: {info['sampled_rows']}\n")
        lines.append(f"- Columnas ({len(info['columns'])}):\n")
        for c in info["columns"]:
            lines.append(f"  - {c}\n")
        lines.append("- Top 10 columnas con más nulos (en muestra):\n")
        for c, pct in info["null_pct_top"]:
            lines.append(f"  - {c}: {pct}%\n")
        lines.append("\n")

    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_MD.write_text("".join(lines), encoding="utf-8")
    print(f"[OK] Reporte generado: {OUT_MD}")

if __name__ == "__main__":
    main()