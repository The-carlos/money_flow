"""
Categoriza movimientos bancarios usando OpenAI API.

Lee movimientos_consolidados.csv, llena la columna 'categoria' con IA
y sobreescribe el archivo. Los movimientos ya categorizados se omiten.

Variables de entorno:
  OPENAI_API_KEY         clave de API
  OPENAI_MODEL           modelo a usar (default: gpt-5-mini)
  OPENAI_API_BASE        base URL opcional (default: https://api.openai.com/v1)
  OPENAI_CATEGORIZE_BATCH tamaño de lote (default: 50)
"""

import csv
import os
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - dependencia opcional
    load_dotenv = None

from openai_classifier import categorize_rows, normalize_category


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CSV_PATH = PROJECT_ROOT / "data" / "processed" / "movimientos_consolidados.csv"
def categorize_movements(rows: list[dict]) -> dict[int, str]:
    """Llama a OpenAI API y regresa un dict index -> categoria."""
    pending = [
        row for row in rows
        if normalize_category(row.get("categoria", "")) == "No identificado"
    ]
    if not pending:
        print("Todos los movimientos ya estan categorizados.")
        return {}

    print(f"Categorizando {len(pending)} movimientos con OpenAI...")
    return categorize_rows(
        rows,
        description_key="descripcion",
        reference_key="referencia",
        type_key="tipo",
        amount_key="monto",
        category_key="categoria",
    )


def run() -> None:
    if load_dotenv is not None:
        load_dotenv()

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
        fieldnames = list(rows[0].keys()) if rows else []

    if "categoria" not in fieldnames:
        fieldnames.append("categoria")
        for row in rows:
            row["categoria"] = ""

    categoria_map = categorize_movements(rows)

    for i, row in enumerate(rows):
        row["categoria"] = normalize_category(row.get("categoria", ""))
        if i in categoria_map:
            row["categoria"] = categoria_map[i]

    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    categorizados = sum(1 for r in rows if r.get("categoria"))
    print(f"Guardado: {CSV_PATH} ({categorizados}/{len(rows)} categorizados)")

    from collections import Counter

    conteo = Counter(r["categoria"] for r in rows if r.get("categoria"))
    print("\nDistribucion de categorias:")
    for cat, n in conteo.most_common():
        print(f"  {n:3d}  {cat}")


if __name__ == "__main__":
    run()
