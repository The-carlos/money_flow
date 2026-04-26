"""
Helpers de categorizacion para gastos del tracker.
"""

from categorizer.openai_classifier import categorize_rows, normalize_category


def backfill_tracker_categories(state: dict) -> tuple[dict, bool, int]:
    gastos = state.get("gastos", [])
    if not gastos:
        return state, False, 0

    changed = False
    for gasto in gastos:
        normalized = normalize_category(gasto.get("categoria", ""))
        if gasto.get("categoria") != normalized:
            gasto["categoria"] = normalized
            changed = True

    categoria_map = categorize_rows(
        gastos,
        description_key="descripcion",
        reference_key="categoria_contexto",
        type_key="tipo",
        amount_key="monto",
        category_key="categoria",
    )
    if not categoria_map:
        return state, changed, 0

    updated = 0
    for i, categoria in categoria_map.items():
        if gastos[i].get("categoria") != categoria:
            gastos[i]["categoria"] = categoria
            updated += 1
            changed = True

    return state, changed, updated


def classify_tracker_expense(
    *,
    descripcion: str,
    monto: float,
    categoria_contexto: str = "",
) -> str:
    rows = [{
        "descripcion": descripcion,
        "categoria_contexto": categoria_contexto,
        "tipo": "tracker",
        "monto": monto,
        "categoria": "No identificado",
    }]
    categoria_map = categorize_rows(
        rows,
        description_key="descripcion",
        reference_key="categoria_contexto",
        type_key="tipo",
        amount_key="monto",
        category_key="categoria",
        batch_size_env="OPENAI_CATEGORIZE_BATCH",
    )
    return categoria_map.get(0, "No identificado")
