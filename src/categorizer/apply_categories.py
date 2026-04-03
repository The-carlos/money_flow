"""
Categoriza movimientos_consolidados.csv con reglas y overrides opcionales.

Los overrides ya no dependen del indice de la fila. Si quieres forzar una
categoria manual, crea `data/processed/manual_categories.json` con este formato:

{
  "2026-03-01|2026-03-02|crédito|egreso|UBER| |119.93|": "Transporte"
}
"""

import csv
import json
from pathlib import Path

from rules import auto_category

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CSV_PATH = PROJECT_ROOT / "data" / "processed" / "movimientos_consolidados.csv"
OVERRIDES_PATH = PROJECT_ROOT / "data" / "processed" / "manual_categories.json"


def movement_signature(row: dict) -> str:
    fields = (
        "fecha_oper",
        "fecha_liq",
        "producto",
        "tipo",
        "descripcion",
        "referencia",
        "cargo",
        "abono",
    )
    return "|".join((row.get(field, "") or "").strip() for field in fields)


def load_overrides() -> dict[str, str]:
    if not OVERRIDES_PATH.exists():
        return {}
    with open(OVERRIDES_PATH, encoding="utf-8") as f:
        data = json.load(f)
    return {str(k): str(v) for k, v in data.items()}


def run() -> None:
    overrides = load_overrides()

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
        fieldnames = list(rows[0].keys()) if rows else []

    if "categoria" not in fieldnames:
        fieldnames.append("categoria")

    for row in rows:
        current = (row.get("categoria") or "").strip()
        if current == "Indefinido":
            current = "No identificado"

        override = overrides.get(movement_signature(row))
        if override:
            row["categoria"] = override
            continue

        if current not in ("", "No identificado"):
            row["categoria"] = current
            continue

        row["categoria"] = auto_category(
            row.get("descripcion", ""),
            row.get("referencia", ""),
        )

    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    from collections import Counter

    conteo = Counter(r["categoria"] for r in rows)
    print(f"Categorizado: {len(rows)} movimientos\n")
    print("Distribucion:")
    for cat, n in conteo.most_common():
        print(f"  {n:3d}  {cat}")


if __name__ == "__main__":
    run()
