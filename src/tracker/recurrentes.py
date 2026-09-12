"""
Helpers para compromisos recurrentes manuales.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from tracker.storage import load_json, save_json


DEFAULT_STATE = {
    "version": 1,
    "recurrentes": [],
    "confirmaciones": [],
}


def month_delta(year: int, month: int, delta: int) -> tuple[int, int]:
    month += delta
    while month > 12:
        year += 1
        month -= 12
    while month < 1:
        year -= 1
        month += 12
    return year, month


def cycle_bounds(today: datetime | None = None) -> tuple[datetime, datetime]:
    today = today or datetime.now()
    if today.day >= 10:
        start = datetime(today.year, today.month, 10)
        end_year, end_month = month_delta(today.year, today.month, 1)
        end = datetime(end_year, end_month, 9, 23, 59, 59)
    else:
        start_year, start_month = month_delta(today.year, today.month, -1)
        start = datetime(start_year, start_month, 10)
        end = datetime(today.year, today.month, 9, 23, 59, 59)
    return start, end


def cycle_id(today: datetime | None = None) -> str:
    start, end = cycle_bounds(today)
    return f"{start.date().isoformat()}__{end.date().isoformat()}"


def _to_float(value: Any) -> float:
    try:
        if pd.isna(value):
            return 0.0
        return round(float(value), 2)
    except (TypeError, ValueError):
        return 0.0


def _to_int(value: Any, default: int = 1) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def normalize_state(state: dict) -> dict:
    normalized = {
        "version": 1,
        "recurrentes": [],
        "confirmaciones": [],
    }

    for item in state.get("recurrentes", []):
        descripcion = str(item.get("descripcion", "")).strip()
        if not descripcion:
            continue
        normalized["recurrentes"].append({
            "id": str(item.get("id") or uuid.uuid4()),
            "activo": bool(item.get("activo", True)),
            "descripcion": descripcion,
            "monto_estimado": _to_float(item.get("monto_estimado")),
            "categoria": str(item.get("categoria", "") or "No identificado"),
            "dia_estimado": min(max(_to_int(item.get("dia_estimado"), 1), 1), 31),
        })

    recurrente_ids = {item["id"] for item in normalized["recurrentes"]}
    for item in state.get("confirmaciones", []):
        recurrente_id = str(item.get("recurrente_id", ""))
        if recurrente_id not in recurrente_ids:
            continue
        fecha = str(item.get("fecha", "") or "")
        normalized["confirmaciones"].append({
            "id": str(item.get("id") or uuid.uuid4()),
            "recurrente_id": recurrente_id,
            "ciclo_id": str(item.get("ciclo_id", "") or ""),
            "confirmado": bool(item.get("confirmado", False)),
            "fecha": fecha,
            "monto_real": _to_float(item.get("monto_real")),
        })

    return normalized


def load_recurrentes(path: Path) -> dict:
    return normalize_state(load_json(path, DEFAULT_STATE.copy()))


def save_recurrentes(path: Path, state: dict) -> dict:
    normalized = normalize_state(state)
    save_json(path, normalized)
    return normalized


def active_recurrentes(state: dict) -> list[dict]:
    return [item for item in state.get("recurrentes", []) if item.get("activo", True)]


def recurrentes_total(state: dict) -> float:
    return round(sum(_to_float(item.get("monto_estimado")) for item in active_recurrentes(state)), 2)


def confirmations_for_cycle(state: dict, ciclo_id: str) -> list[dict]:
    return [
        item for item in state.get("confirmaciones", [])
        if item.get("ciclo_id") == ciclo_id
    ]


def confirmed_total(state: dict, ciclo_id: str) -> float:
    return round(sum(
        _to_float(item.get("monto_real"))
        for item in confirmations_for_cycle(state, ciclo_id)
        if item.get("confirmado")
    ), 2)


def pending_recurrentes(state: dict, ciclo_id: str) -> list[dict]:
    confirmed_ids = {
        item.get("recurrente_id")
        for item in confirmations_for_cycle(state, ciclo_id)
        if item.get("confirmado")
    }
    return [item for item in active_recurrentes(state) if item.get("id") not in confirmed_ids]


def latest_msi_payment_total(data_dir: Path) -> float:
    candidates = sorted(data_dir.glob("msi_activos_*.csv"), reverse=True)
    if not candidates:
        return 0.0
    try:
        df = pd.read_csv(candidates[0])
    except (OSError, pd.errors.ParserError):
        return 0.0
    if "pago_requerido" not in df:
        return 0.0
    return round(pd.to_numeric(df["pago_requerido"], errors="coerce").fillna(0).sum(), 2)
