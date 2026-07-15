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


def build_diff_frame(matches: list[dict[str, DiffItem | None]]) -> pd.DataFrame:
    rows = []
    for pair in matches:
        statement = pair["statement"]
        tracker = pair["tracker"]
        if statement and tracker:
            status = "match"
            diff = statement.monto - tracker.monto
            fecha_ref = min(statement.fecha, tracker.fecha)
        elif statement:
            status = "estado_only"
            diff = statement.monto
            fecha_ref = statement.fecha
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
            "Tracker Fecha": tracker.fecha.isoformat() if tracker else "",
            "Tracker Monto": tracker.monto if tracker else None,
            "Tracker Descripción": tracker.descripcion if tracker else "",
            "Tracker Categoría": tracker.categoria if tracker else "",
            "Diferencia": round(diff, 2),
        })
    return pd.DataFrame(rows)


def summarize_diff(diff_df: pd.DataFrame) -> dict[str, float | int]:
    if diff_df.empty:
        return {
            "matched_count": 0,
            "statement_only_count": 0,
            "tracker_only_count": 0,
            "matched_amount": 0.0,
            "statement_only_amount": 0.0,
            "tracker_only_amount": 0.0,
            "net_difference": 0.0,
        }

    matched = diff_df[diff_df["status"] == "match"]
    statement_only = diff_df[diff_df["status"] == "estado_only"]
    tracker_only = diff_df[diff_df["status"] == "tracker_only"]
    return {
        "matched_count": len(matched),
        "statement_only_count": len(statement_only),
        "tracker_only_count": len(tracker_only),
        "matched_amount": float(matched["Estado Monto"].fillna(0).sum()),
        "statement_only_amount": float(statement_only["Estado Monto"].fillna(0).sum()),
        "tracker_only_amount": float(tracker_only["Tracker Monto"].fillna(0).sum()),
        "net_difference": float(diff_df["Diferencia"].fillna(0).sum()),
    }


def style_diff_frame(diff_df: pd.DataFrame) -> pd.io.formats.style.Styler:
    def row_style(row: pd.Series) -> list[str]:
        if row["status"] == "match":
            color = "background-color: #E8F5E9"
        elif row["status"] == "estado_only":
            color = "background-color: #FFEBEE"
        else:
            color = "background-color: #FFEBEE"
        return [color] * len(row)

    return (
        diff_df.style
        .apply(row_style, axis=1)
        .hide(axis="columns", subset=["status"])
        .format({
            "Estado Monto": "${:,.2f}",
            "Tracker Monto": "${:,.2f}",
            "Diferencia": "${:,.2f}",
        }, na_rep="")
    )
