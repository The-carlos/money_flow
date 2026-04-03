"""
Pipeline principal de MoneyFlow.

Detecta PDFs nuevos en data/raw/, los procesa (débito o crédito),
y los agrega al CSV consolidado con su periodo.

Uso:
    python src/extractor/pipeline.py          # procesa todos los PDFs nuevos
    python src/extractor/pipeline.py --all    # reprocesa todo desde cero
"""

import csv
import hashlib
import json
import re
import sys
from datetime import datetime
from pathlib import Path

import pdfplumber

from pdf_parser    import extract_movements
from credit_parser import extract_credit_data, save_msi_csv

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "categorizer"))
from rules import auto_category

# ---------------------------------------------------------------------------
# Rutas
# ---------------------------------------------------------------------------
PROJECT_ROOT   = Path(__file__).resolve().parents[2]
RAW_DIR        = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR  = PROJECT_ROOT / "data" / "processed"
CONSOLIDATED   = PROCESSED_DIR / "movimientos_consolidados.csv"
MANIFEST_PATH  = PROCESSED_DIR / "manifest.json"

FIELDS = [
    "fecha_oper", "fecha_liq", "producto", "descripcion", "referencia",
    "tipo", "cargo", "abono", "saldo_acumulado", "categoria", "periodo",
]

MESES_ES = {
    1: "Ene", 2: "Feb", 3: "Mar", 4: "Abr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Ago", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dic",
}

# ---------------------------------------------------------------------------
# Detección de tipo de PDF
# ---------------------------------------------------------------------------

def _detect_type(pdf_path: Path) -> str:
    """Devuelve 'credito' o 'debito' según el contenido del PDF."""
    with pdfplumber.open(str(pdf_path)) as pdf:
        text = " ".join(
            p.extract_text() or "" for p in pdf.pages[:3]
        )
    credit_keywords = [
        "CARGOS, COMPRAS y ABONOS REGULARES",
        "Tarjeta ORO",
        "Tarjeta Oro",
        "TARJETA DE CREDITO",
        "Pago para no generar intereses",
        "Saldo cargo a meses",
    ]
    return "credito" if any(k in text for k in credit_keywords) else "debito"


# ---------------------------------------------------------------------------
# Período
# ---------------------------------------------------------------------------

def _periodo_key(fechas: list[str]) -> str:
    """Devuelve 'YYYY-MM' basado en la fecha máxima del lote."""
    parsed = [datetime.strptime(f, "%Y-%m-%d") for f in fechas if f]
    if not parsed:
        raise ValueError("No hay fechas validas para construir el periodo.")
    hi = max(parsed)
    return f"{hi.year}-{hi.month:02d}"


def _periodo_label(fechas: list[str], tipo: str = "") -> str:
    """Devuelve 'Débito · Mmm YYYY' o 'Crédito · Mmm YYYY'."""
    parsed = [datetime.strptime(f, "%Y-%m-%d") for f in fechas if f]
    if not parsed:
        raise ValueError("No hay fechas validas para construir la etiqueta de periodo.")
    lo, hi = min(parsed), max(parsed)
    fmt = lambda d: f"{d.day:02d} {MESES_ES[d.month]}"
    rango = f"{fmt(lo)} – {fmt(hi)} {hi.year}"
    prefix = f"{tipo.capitalize()} · " if tipo else ""
    return f"{prefix}{rango}"


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------

def _load_manifest() -> dict:
    if MANIFEST_PATH.exists():
        with open(MANIFEST_PATH, encoding="utf-8") as f:
            manifest = json.load(f)
            return _normalize_manifest(manifest)
    return {"processed": []}


def _save_manifest(manifest: dict) -> None:
    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)


def _normalize_manifest(manifest: dict) -> dict:
    deduped: list[dict] = []
    seen: set[tuple] = set()
    for entry in manifest.get("processed", []):
        key = (
            entry.get("filename", ""),
            entry.get("sha256", ""),
            entry.get("size", ""),
            entry.get("mtime", ""),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(entry)
    return {"processed": deduped}


def _file_signature(pdf_path: Path) -> dict:
    stat = pdf_path.stat()
    sha256 = hashlib.sha256(pdf_path.read_bytes()).hexdigest()
    return {
        "filename": pdf_path.name,
        "size": stat.st_size,
        "mtime": int(stat.st_mtime),
        "sha256": sha256,
    }


def _processed_signatures(manifest: dict) -> set[tuple]:
    signatures = set()
    for entry in manifest.get("processed", []):
        signatures.add((
            entry.get("filename", ""),
            entry.get("sha256", ""),
            entry.get("size", ""),
            entry.get("mtime", ""),
        ))
    return signatures


# ---------------------------------------------------------------------------
# Consolidated CSV helpers
# ---------------------------------------------------------------------------

def _load_consolidated() -> list[dict]:
    if not CONSOLIDATED.exists():
        return []
    with open(CONSOLIDATED, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _row_key(row: dict) -> tuple:
    return (
        row.get("fecha_oper", ""),
        row.get("fecha_liq", ""),
        row.get("producto", ""),
        row.get("tipo", ""),
        row.get("descripcion", ""),
        row.get("referencia", ""),
        row.get("cargo", ""),
        row.get("abono", ""),
    )


def _save_consolidated(rows: list[dict]) -> None:
    with open(CONSOLIDATED, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# Métricas de crédito
# ---------------------------------------------------------------------------

def _extract_metricas(pdf_path: Path, adeudo: float) -> dict:
    with pdfplumber.open(str(pdf_path)) as pdf:
        if len(pdf.pages) < 2:
            raise ValueError(f"{pdf_path.name}: el PDF no tiene suficientes paginas para leer metricas.")
        page2 = pdf.pages[1].extract_text() or ""

    def _find(pattern):
        m = re.search(pattern, page2)
        if not m:
            return None
        return float(m.group(1).replace(",", ""))

    metricas = {
        "pago_sin_intereses":     _find(r"Pago para no generar intereses:\d?\s*\$([\d,]+\.\d{2})"),
        "saldo_cargos_regulares": _find(r"Saldo cargos regulares:\s*\$([\d,]+\.\d{2})"),
        "saldo_msi":              _find(r"Saldo cargo a meses:\s*\$([\d,]+\.\d{2})"),
        "saldo_deudor_total":     _find(r"Saldo deudor total:\d*\s*\$([\d,]+\.\d{2})"),
        "limite_credito":         _find(r"Límite de crédito:\s*\$([\d,]+\.\d{2})"),
        "credito_disponible":     _find(r"Crédito disponible:\s*\$([\d,]+\.\d{2})"),
        "pago_minimo":            _find(r"Pago mínimo:\d?\s*\$([\d,]+\.\d{2})"),
        "adeudo_anterior":        adeudo,
    }
    required = ("pago_sin_intereses", "saldo_deudor_total", "limite_credito")
    missing = [key for key in required if metricas.get(key) is None]
    if missing:
        raise ValueError(
            f"{pdf_path.name}: faltan metricas criticas en el PDF ({', '.join(missing)})."
        )
    return metricas




# ---------------------------------------------------------------------------
# Procesamiento de un PDF
# ---------------------------------------------------------------------------

def _process_debito(pdf_path: Path) -> list[dict]:
    movs, saldo_inicial = extract_movements(str(pdf_path))
    if not movs:
        return []

    fechas = [m.fecha_oper for m in movs]
    label  = _periodo_label(fechas, "débito")

    # Calcular saldo acumulado
    saldo = saldo_inicial
    rows = []
    for m in movs:
        if m.cargo:
            saldo -= m.cargo
        elif m.abono:
            saldo += m.abono
        rows.append({
            "fecha_oper":      m.fecha_oper,
            "fecha_liq":       m.fecha_liq,
            "producto":        "débito",
            "descripcion":     m.descripcion,
            "referencia":      m.referencia,
            "tipo":            m.tipo,
            "cargo":           str(m.cargo) if m.cargo else "",
            "abono":           str(m.abono) if m.abono else "",
            "saldo_acumulado": f"{saldo:.2f}",
            "categoria":       auto_category(m.descripcion, m.referencia),
            "periodo":         label,
        })
    return rows


def _process_credito(pdf_path: Path) -> list[dict]:
    movs, plans, adeudo = extract_credit_data(str(pdf_path))
    if not movs:
        return []

    fechas = [m.fecha_oper for m in movs]
    label  = _periodo_label(fechas, "crédito")
    key    = _periodo_key(fechas)

    # Guardar MSI y métricas indexados por periodo
    msi_out = PROCESSED_DIR / f"msi_activos_{key}.csv"
    save_msi_csv(plans, str(msi_out))

    metricas = _extract_metricas(pdf_path, adeudo)
    metro_out = PROCESSED_DIR / f"metricas_credito_{key}.json"
    with open(metro_out, "w", encoding="utf-8") as f:
        json.dump(metricas, f, indent=2)

    # Calcular saldo acumulado (deuda acumulada)
    saldo = adeudo
    rows = []
    for m in movs:
        if m.cargo:
            saldo += m.cargo
        elif m.abono:
            saldo -= m.abono
        rows.append({
            "fecha_oper":      m.fecha_oper,
            "fecha_liq":       m.fecha_liq,
            "producto":        "crédito",
            "descripcion":     m.descripcion,
            "referencia":      m.referencia,
            "tipo":            m.tipo,
            "cargo":           str(m.cargo) if m.cargo else "",
            "abono":           str(m.abono) if m.abono else "",
            "saldo_acumulado": f"{saldo:.2f}",
            "categoria":       auto_category(m.descripcion, m.referencia),
            "periodo":         label,
        })

    print(f"  MSI:     {msi_out.name}  ({len(plans)} planes)")
    print(f"  Métricas: {metro_out.name}")
    return rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(force_all: bool = False) -> None:
    manifest = {"processed": []} if force_all else _load_manifest()
    done_signatures = set() if force_all else _processed_signatures(manifest)

    pdf_files = sorted(RAW_DIR.glob("*.pdf"))
    pdf_signatures = {p: _file_signature(p) for p in pdf_files}
    new_pdfs = [
        p for p in pdf_files
        if (
            pdf_signatures[p]["filename"],
            pdf_signatures[p]["sha256"],
            pdf_signatures[p]["size"],
            pdf_signatures[p]["mtime"],
        ) not in done_signatures
    ]

    if not new_pdfs:
        print("No hay PDFs nuevos para procesar.")
        return

    existing_rows = [] if force_all else _load_consolidated()
    existing_keys = {_row_key(r) for r in existing_rows}

    total_added = 0

    for pdf_path in new_pdfs:
        print(f"\n📄 {pdf_path.name}")
        kind = _detect_type(pdf_path)
        print(f"  Tipo detectado: {kind}")

        try:
            if kind == "credito":
                new_rows = _process_credito(pdf_path)
            else:
                new_rows = _process_debito(pdf_path)
        except Exception as e:
            print(f"  ⚠ Error al procesar: {e}")
            continue

        # Deduplicar
        added = []
        for row in new_rows:
            k = _row_key(row)
            if k not in existing_keys:
                existing_keys.add(k)
                added.append(row)

        existing_rows.extend(added)
        total_added += len(added)
        print(f"  Movimientos nuevos: {len(added)}  (duplicados omitidos: {len(new_rows) - len(added)})")

        if new_rows:
            fechas = [r["fecha_oper"] for r in new_rows if r["fecha_oper"]]
            periodo_label = new_rows[0]["periodo"] if new_rows else "?"
            key = _periodo_key(fechas)
            manifest["processed"].append({
                **pdf_signatures[pdf_path],
                "type":       kind,
                "periodo_key": key,
                "periodo":    periodo_label,
                "processed_at": datetime.now().isoformat(timespec="seconds"),
            })

    # Ordenar y guardar
    existing_rows.sort(key=lambda r: (r.get("periodo", ""), r.get("fecha_oper", ""), r.get("producto", "")))
    _save_consolidated(existing_rows)
    _save_manifest(_normalize_manifest(manifest))

    print(f"\n✅ Total movimientos agregados: {total_added}")
    print(f"   Consolidado: {CONSOLIDATED} ({len(existing_rows)} filas)")
    print(f"   Manifest: {MANIFEST_PATH}")


if __name__ == "__main__":
    force = "--all" in sys.argv
    run(force_all=force)
