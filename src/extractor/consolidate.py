"""
Consolida movimientos de débito y crédito en un único CSV.

Agrega columna 'categoria' vacía lista para ser llenada por el categorizador.
"""

import csv
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEBIT_CSV  = PROJECT_ROOT / "data" / "processed" / "movimientos.csv"
CREDIT_CSV = PROJECT_ROOT / "data" / "processed" / "movimientos_credito.csv"
OUT_CSV    = PROJECT_ROOT / "data" / "processed" / "movimientos_consolidados.csv"

FIELDS = [
    "fecha_oper", "fecha_liq", "producto", "descripcion", "referencia",
    "tipo", "cargo", "abono", "saldo_acumulado", "categoria", "periodo",
]

MESES_ES = {
    1: "Ene", 2: "Feb", 3: "Mar", 4: "Abr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Ago", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dic",
}

def _periodo_label(fechas: list[str]) -> str:
    """Genera etiqueta 'DD Mmm YYYY – DD Mmm YYYY' a partir de lista de fechas ISO."""
    parsed = [datetime.strptime(f, "%Y-%m-%d") for f in fechas if f]
    if not parsed:
        return ""
    lo, hi = min(parsed), max(parsed)
    fmt = lambda d: f"{d.day:02d} {MESES_ES[d.month]} {d.year}"
    return f"{fmt(lo)} – {fmt(hi)}"


def consolidate() -> None:
    rows = []

    for path in (DEBIT_CSV, CREDIT_CSV):
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                row.setdefault("categoria", "")
                row.setdefault("periodo", "")
                rows.append({k: row.get(k, "") for k in FIELDS})

    rows.sort(key=lambda r: (r["fecha_oper"], r["producto"]))

    # Asigna periodo a filas que aún no tienen uno (primera consolidación)
    fechas_sin_periodo = [r["fecha_oper"] for r in rows if not r["periodo"]]
    if fechas_sin_periodo:
        label = _periodo_label(fechas_sin_periodo)
        for r in rows:
            if not r["periodo"]:
                r["periodo"] = label

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    debito  = sum(1 for r in rows if r["producto"] == "débito")
    credito = sum(1 for r in rows if r["producto"] == "crédito")
    print(f"Consolidado: {len(rows)} movimientos  ({debito} débito, {credito} crédito)")
    print(f"Guardado en: {OUT_CSV}")


if __name__ == "__main__":
    consolidate()
