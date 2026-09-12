"""
Diff helpers for comparing credit-card statement expenses against tracker rows.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from math import inf
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class DiffItem:
    source: str
    fecha: date
    monto: float
    descripcion: str
    categoria: str
    periodo: str
    row_id: int


def credit_expenses_for_period(df: pd.DataFrame, periodo: str) -> list[DiffItem]:
    credit_df = df[
        (df["periodo"] == periodo) &
        (df["producto"] == "crédito") &
        (df["tipo"] == "egreso")
    ].copy()

    items: list[DiffItem] = []
    for row_id, row in credit_df.sort_values(["fecha_oper", "cargo", "descripcion"]).iterrows():
        amount = pd.to_numeric(row.get("cargo"), errors="coerce")
        if pd.isna(amount):
            continue
        fecha = pd.to_datetime(row["fecha_oper"]).date()
        items.append(DiffItem(
            source="estado",
            fecha=fecha,
            monto=round(float(amount), 2),
            descripcion=str(row.get("descripcion", "") or ""),
            categoria=str(row.get("categoria", "") or "No identificado"),
            periodo=str(row.get("periodo", "") or ""),
            row_id=int(row_id),
        ))
    return items


def tracker_expenses_for_period(period: dict[str, Any], label: str) -> list[DiffItem]:
    state = period["state"]
    items: list[DiffItem] = []
    for row_id, row in enumerate(state.get("gastos", [])):
        amount = pd.to_numeric(row.get("monto"), errors="coerce")
        if pd.isna(amount):
            continue
        fecha = pd.to_datetime(row.get("fecha")).date()
        items.append(DiffItem(
            source="tracker",
            fecha=fecha,
            monto=round(float(amount), 2),
            descripcion=str(row.get("descripcion", "") or ""),
            categoria=str(row.get("categoria", "") or "No identificado"),
            periodo=label,
            row_id=row_id,
        ))
    return sorted(items, key=lambda item: (item.fecha, item.monto, item.descripcion))


def recurrente_items_for_cycle(recurrentes_state: dict[str, Any], ciclo_id: str, label: str) -> list[DiffItem]:
    recurrentes_by_id = {
        item.get("id"): item
        for item in recurrentes_state.get("recurrentes", [])
    }
    items: list[DiffItem] = []
    for row_id, row in enumerate(recurrentes_state.get("confirmaciones", [])):
        if row.get("ciclo_id") != ciclo_id or not row.get("confirmado"):
            continue
        recurrente = recurrentes_by_id.get(row.get("recurrente_id"), {})
        amount = pd.to_numeric(row.get("monto_real"), errors="coerce")
        if pd.isna(amount):
            continue
        fecha = pd.to_datetime(row.get("fecha"), errors="coerce")
        if pd.isna(fecha):
            continue
        items.append(DiffItem(
            source="recurrente",
            fecha=fecha.date(),
            monto=round(float(amount), 2),
            descripcion=str(recurrente.get("descripcion", "") or ""),
            categoria=str(recurrente.get("categoria", "") or "No identificado"),
            periodo=label,
            row_id=row_id,
        ))
    return sorted(items, key=lambda item: (item.fecha, item.monto, item.descripcion))


def pending_recurrente_items_for_cycle(recurrentes_state: dict[str, Any], ciclo_id: str, label: str) -> list[DiffItem]:
    confirmed_ids = {
        row.get("recurrente_id")
        for row in recurrentes_state.get("confirmaciones", [])
        if row.get("ciclo_id") == ciclo_id and row.get("confirmado")
    }
    items: list[DiffItem] = []
    for row_id, row in enumerate(recurrentes_state.get("recurrentes", [])):
        if not row.get("activo", True) or row.get("id") in confirmed_ids:
            continue
        amount = pd.to_numeric(row.get("monto_estimado"), errors="coerce")
        if pd.isna(amount):
            continue
        dia = int(row.get("dia_estimado") or 1)
        start_text = ciclo_id.split("__", 1)[0]
        start = pd.to_datetime(start_text, errors="coerce")
        if pd.isna(start):
            continue
        year = start.year if dia >= 10 else (start + pd.DateOffset(months=1)).year
        month = start.month if dia >= 10 else (start + pd.DateOffset(months=1)).month
        try:
            fecha = date(year, month, min(max(dia, 1), 28))
        except ValueError:
            fecha = start.date()
        items.append(DiffItem(
            source="recurrente_pendiente",
            fecha=fecha,
            monto=round(float(amount), 2),
            descripcion=str(row.get("descripcion", "") or ""),
            categoria=str(row.get("categoria", "") or "No identificado"),
            periodo=label,
            row_id=row_id,
        ))
    return sorted(items, key=lambda item: (item.fecha, item.monto, item.descripcion))


def _match_score(statement: DiffItem, tracker: DiffItem) -> tuple[int, float, int]:
    date_delta = abs((statement.fecha - tracker.fecha).days)
    amount_delta = abs(statement.monto - tracker.monto)
    same_sign_description = 0 if statement.descripcion[:8].lower() == tracker.descripcion[:8].lower() else 1
    return date_delta, amount_delta, same_sign_description


def match_expenses(
    statement_items: list[DiffItem],
    tracker_items: list[DiffItem],
    *,
    date_tolerance_days: int = 1,
    amount_tolerance: float = 0.01,
) -> list[dict[str, DiffItem | None]]:
    unused_tracker = set(range(len(tracker_items)))
    matches: list[dict[str, DiffItem | None]] = []

    for statement in sorted(statement_items, key=lambda item: (item.fecha, item.monto, item.descripcion)):
        best_idx = None
        best_score = (inf, inf, inf)

        for idx in unused_tracker:
            tracker = tracker_items[idx]
            date_delta, amount_delta, description_score = _match_score(statement, tracker)
            if date_delta > date_tolerance_days:
                continue
            if amount_delta > amount_tolerance:
                continue
            score = (date_delta, amount_delta, description_score)
            if score < best_score:
                best_idx = idx
                best_score = score

        if best_idx is None:
            matches.append({"statement": statement, "tracker": None})
        else:
            unused_tracker.remove(best_idx)
            matches.append({"statement": statement, "tracker": tracker_items[best_idx]})

    for idx in sorted(unused_tracker, key=lambda i: (tracker_items[i].fecha, tracker_items[i].monto, tracker_items[i].descripcion)):
        matches.append({"statement": None, "tracker": tracker_items[idx]})

    return sorted(
        matches,
        key=lambda pair: (
            (pair["statement"] or pair["tracker"]).fecha,  # type: ignore[union-attr]
            (pair["statement"] or pair["tracker"]).monto,  # type: ignore[union-attr]
        ),
    )


def match_expenses_with_recurrentes(
    statement_items: list[DiffItem],
    tracker_items: list[DiffItem],
    recurrente_items: list[DiffItem],
    pending_recurrentes: list[DiffItem],
    *,
    date_tolerance_days: int = 1,
    amount_tolerance: float = 0.01,
) -> list[dict[str, DiffItem | None]]:
    first_pass = match_expenses(
        statement_items,
        tracker_items,
        date_tolerance_days=date_tolerance_days,
        amount_tolerance=amount_tolerance,
    )
    matched = [pair for pair in first_pass if pair["statement"] and pair["tracker"]]
    statement_only = [pair["statement"] for pair in first_pass if pair["statement"] and not pair["tracker"]]
    tracker_only = [pair for pair in first_pass if pair["tracker"] and not pair["statement"]]

    second_pass = match_expenses(
        statement_only,
        recurrente_items,
        date_tolerance_days=date_tolerance_days,
        amount_tolerance=amount_tolerance,
    )
    pending_pairs = [{"statement": None, "tracker": item} for item in pending_recurrentes]

    return sorted(
        matched + second_pass + tracker_only + pending_pairs,
        key=lambda pair: (
            (pair["statement"] or pair["tracker"]).fecha,  # type: ignore[union-attr]
            (pair["statement"] or pair["tracker"]).monto,  # type: ignore[union-attr]
        ),
    )


def build_diff_frame(matches: list[dict[str, DiffItem | None]]) -> pd.DataFrame:
    rows = []
    for pair in matches:
        statement = pair["statement"]
        tracker = pair["tracker"]
        if statement and tracker:
            status = "match_recurrente" if tracker.source == "recurrente" else "match_tracker"
            diff = statement.monto - tracker.monto
            fecha_ref = min(statement.fecha, tracker.fecha)
        elif statement:
            status = "estado_only"
            diff = statement.monto
            fecha_ref = statement.fecha
        elif tracker and tracker.source in ("recurrente", "recurrente_pendiente"):
            status = "recurrente_pendiente"
            diff = -tracker.monto
            fecha_ref = tracker.fecha
        else:
            status = "tracker_only"
            diff = -(tracker.monto if tracker else 0)
            fecha_ref = tracker.fecha if tracker else None

        rows.append({
            "status": status,
            "Fecha": fecha_ref.isoformat() if fecha_ref else "",
            "Estado Fecha": statement.fecha.isoformat() if statement else "",
            "Estado Monto": statement.monto if statement else None,
            "Estado Descripción": statement.descripcion if statement else "",
            "Estado Categoría": statement.categoria if statement else "",
            "Esperado Fuente": tracker.source if tracker else "",
            "Esperado Fecha": tracker.fecha.isoformat() if tracker else "",
            "Esperado Monto": tracker.monto if tracker else None,
            "Esperado Descripción": tracker.descripcion if tracker else "",
            "Esperado Categoría": tracker.categoria if tracker else "",
            "Diferencia": round(diff, 2),
        })
    return pd.DataFrame(rows)


def summarize_diff(diff_df: pd.DataFrame) -> dict[str, float | int]:
    if diff_df.empty:
        return {
            "matched_count": 0,
            "matched_tracker_count": 0,
            "matched_recurrente_count": 0,
            "statement_only_count": 0,
            "tracker_only_count": 0,
            "recurrente_pending_count": 0,
            "matched_amount": 0.0,
            "matched_tracker_amount": 0.0,
            "matched_recurrente_amount": 0.0,
            "statement_only_amount": 0.0,
            "tracker_only_amount": 0.0,
            "recurrente_pending_amount": 0.0,
            "net_difference": 0.0,
        }

    matched = diff_df[diff_df["status"].isin(["match_tracker", "match_recurrente"])]
    matched_tracker = diff_df[diff_df["status"] == "match_tracker"]
    matched_recurrente = diff_df[diff_df["status"] == "match_recurrente"]
    statement_only = diff_df[diff_df["status"] == "estado_only"]
    tracker_only = diff_df[diff_df["status"] == "tracker_only"]
    recurrente_pending = diff_df[diff_df["status"] == "recurrente_pendiente"]
    return {
        "matched_count": len(matched),
        "matched_tracker_count": len(matched_tracker),
        "matched_recurrente_count": len(matched_recurrente),
        "statement_only_count": len(statement_only),
        "tracker_only_count": len(tracker_only),
        "recurrente_pending_count": len(recurrente_pending),
        "matched_amount": float(matched["Estado Monto"].fillna(0).sum()),
        "matched_tracker_amount": float(matched_tracker["Estado Monto"].fillna(0).sum()),
        "matched_recurrente_amount": float(matched_recurrente["Estado Monto"].fillna(0).sum()),
        "statement_only_amount": float(statement_only["Estado Monto"].fillna(0).sum()),
        "tracker_only_amount": float(tracker_only["Esperado Monto"].fillna(0).sum()),
        "recurrente_pending_amount": float(recurrente_pending["Esperado Monto"].fillna(0).sum()),
        "net_difference": float(diff_df["Diferencia"].fillna(0).sum()),
    }


def style_diff_frame(diff_df: pd.DataFrame) -> pd.io.formats.style.Styler:
    def row_style(row: pd.Series) -> list[str]:
        if row["status"] in ("match_tracker", "match_recurrente"):
            style = "background-color: #B7E4C7; color: #0B2E13; border-left: 5px solid #2D6A4F"
        elif row["status"] == "recurrente_pendiente":
            style = "background-color: #FEF3C7; color: #422006; border-left: 5px solid #D97706"
        elif row["status"] == "estado_only":
            style = "background-color: #F8B4B4; color: #3B0A0A; border-left: 5px solid #B91C1C"
        else:
            style = "background-color: #F8B4B4; color: #3B0A0A; border-left: 5px solid #B91C1C"
        return [style] * len(row)

    return (
        diff_df.style
        .apply(row_style, axis=1)
        .hide(axis="columns", subset=["status"])
        .format({
            "Estado Monto": "${:,.2f}",
            "Esperado Monto": "${:,.2f}",
            "Diferencia": "${:,.2f}",
        }, na_rep="")
    )
