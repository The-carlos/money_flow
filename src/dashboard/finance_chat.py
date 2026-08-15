"""
Asistente financiero de solo lectura para el dashboard.

El modelo no recibe todos los datos crudos: puede pedir herramientas internas
que calculan agregados acotados sobre los archivos locales de MoneyFlow.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import pandas as pd

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - dependencia opcional
    load_dotenv = None


SYSTEM_PROMPT = """Eres el asistente financiero personal de MoneyFlow.
Responde en espanol, con tono directo y prudente.

Tienes acceso solo a herramientas internas de lectura. Usa herramientas antes de
responder preguntas sobre datos financieros. No inventes cifras: si falta data,
dilo claramente. Explica causas probables separando hechos calculados de
interpretaciones. No des asesoria financiera profesional; ayuda a entender
patrones, deuda, gasto, pagos, tracker y MSI.
"""


@dataclass
class FinanceChatContext:
    data_dir: Path
    movements: pd.DataFrame
    credit_metrics: pd.DataFrame
    tracker_state: dict
    tracker_history: list[dict]


def _currency(value: Any) -> float:
    try:
        if pd.isna(value):
            return 0.0
        return round(float(value), 2)
    except (TypeError, ValueError):
        return 0.0


def _period_key_series(df: pd.DataFrame) -> pd.Series:
    return pd.to_datetime(df["fecha_oper"], errors="coerce").dt.strftime("%Y-%m")


def _top_records(df: pd.DataFrame, columns: list[str], limit: int = 12) -> list[dict]:
    if df.empty:
        return []
    available_columns = [col for col in columns if col in df.columns]
    rows = df[available_columns].head(limit).copy()
    for col in rows.columns:
        if pd.api.types.is_datetime64_any_dtype(rows[col]):
            rows[col] = rows[col].dt.strftime("%Y-%m-%d")
    return rows.fillna("").to_dict(orient="records")


def _load_msi_history(data_dir: Path) -> pd.DataFrame:
    frames = []
    for path in sorted(data_dir.glob("msi_activos_*.csv")):
        periodo_key = path.stem.replace("msi_activos_", "")
        try:
            frame = pd.read_csv(path)
        except (OSError, pd.errors.ParserError):
            continue
        frame["periodo_key"] = periodo_key
        frames.append(frame)

    if not frames:
        return pd.DataFrame()

    msi = pd.concat(frames, ignore_index=True)
    for col in ("monto_original", "saldo_pendiente", "pago_requerido"):
        if col in msi:
            msi[col] = pd.to_numeric(msi[col], errors="coerce").fillna(0)
    if "progreso" not in msi and {"pago_num", "total_pagos"}.issubset(msi.columns):
        msi["progreso"] = msi["pago_num"].astype(str) + " / " + msi["total_pagos"].astype(str)
    return msi


def _tool_credit_debt_history(ctx: FinanceChatContext, months: int = 8) -> dict:
    metrics = ctx.credit_metrics.copy()
    if metrics.empty:
        return {"periods": [], "note": "No hay metricas historicas de credito."}

    months = max(1, min(int(months or 8), 18))
    metrics = metrics.sort_values("periodo_key").tail(months)

    credit = ctx.movements[ctx.movements["producto"] == "crédito"].copy()
    movement_rows = []
    if not credit.empty:
        credit["periodo_key"] = _period_key_series(credit)
        grouped = credit.groupby("periodo_key", dropna=True).agg(
            compras=("cargo", "sum"),
            pagos=("abono", "sum"),
        )
        movement_rows = grouped.to_dict(orient="index")

    periods = []
    previous_total = None
    for _, row in metrics.iterrows():
        key = row["periodo_key"]
        movimientos = movement_rows.get(key, {})
        total = _currency(row.get("Deuda total"))
        periods.append({
            "periodo": key,
            "deuda_regular": _currency(row.get("Deuda TDC Regular")),
            "deuda_msi": _currency(row.get("Deuda MSI")),
            "deuda_total": total,
            "limite_credito": _currency(row.get("Límite de crédito")),
            "uso_tdc_pct": _currency(row.get("Uso TDC")),
            "compras_del_periodo": _currency(movimientos.get("compras")),
            "pagos_del_periodo": _currency(movimientos.get("pagos")),
            "cambio_deuda_total": None if previous_total is None else _currency(total - previous_total),
        })
        previous_total = total

    return {
        "periods": periods,
        "interpretation_hint": (
            "Si compras_del_periodo supera pagos_del_periodo, o deuda_msi sube, "
            "la deuda total puede no bajar aunque hagas pagos."
        ),
    }


def _tool_spending_by_category(ctx: FinanceChatContext, product: str = "crédito", months: int = 4) -> dict:
    df = ctx.movements.copy()
    if product in ("débito", "crédito"):
        df = df[df["producto"] == product]
    df = df[df["tipo"] == "egreso"].copy()
    if df.empty:
        return {"categories": [], "note": "No hay egresos para esos filtros."}

    months = max(1, min(int(months or 4), 12))
    df["periodo_key"] = _period_key_series(df)
    latest_periods = sorted(df["periodo_key"].dropna().unique())[-months:]
    df = df[df["periodo_key"].isin(latest_periods)]

    grouped = (
        df.groupby(["periodo_key", "categoria"], dropna=False)["cargo"]
        .sum()
        .reset_index()
        .sort_values(["periodo_key", "cargo"], ascending=[True, False])
    )
    totals = (
        df.groupby("categoria", dropna=False)["cargo"]
        .sum()
        .sort_values(ascending=False)
        .head(12)
        .reset_index()
    )
    return {
        "periods": latest_periods,
        "top_categories_total": [
            {"categoria": r["categoria"], "monto": _currency(r["cargo"])}
            for _, r in totals.iterrows()
        ],
        "by_period_category": [
            {"periodo": r["periodo_key"], "categoria": r["categoria"], "monto": _currency(r["cargo"])}
            for _, r in grouped.head(40).iterrows()
        ],
    }


def _tool_recent_large_movements(ctx: FinanceChatContext, product: str = "crédito", limit: int = 12) -> dict:
    df = ctx.movements.copy()
    if product in ("débito", "crédito"):
        df = df[df["producto"] == product]
    df = df[df["tipo"] == "egreso"].copy()
    if df.empty:
        return {"movements": []}

    df = df.sort_values("cargo", ascending=False)
    return {
        "movements": _top_records(
            df,
            ["fecha_oper", "producto", "descripcion", "categoria", "cargo", "periodo"],
            max(1, min(int(limit or 12), 25)),
        )
    }


def _tool_msi_summary(ctx: FinanceChatContext, months: int = 6) -> dict:
    msi = _load_msi_history(ctx.data_dir)
    if msi.empty:
        return {"periods": [], "plans": [], "note": "No hay archivos de MSI activos."}

    months = max(1, min(int(months or 6), 18))
    latest_periods = sorted(msi["periodo_key"].dropna().unique())[-months:]
    msi = msi[msi["periodo_key"].isin(latest_periods)].copy()

    summary = (
        msi.groupby("periodo_key")
        .agg(
            planes=("descripcion", "count"),
            saldo_pendiente=("saldo_pendiente", "sum"),
            pago_requerido=("pago_requerido", "sum"),
        )
        .reset_index()
        .sort_values("periodo_key")
    )
    plans = msi.sort_values("saldo_pendiente", ascending=False)
    return {
        "periods": [
            {
                "periodo": r["periodo_key"],
                "planes": int(r["planes"]),
                "saldo_pendiente": _currency(r["saldo_pendiente"]),
                "pago_requerido": _currency(r["pago_requerido"]),
            }
            for _, r in summary.iterrows()
        ],
        "largest_active_plans": _top_records(
            plans,
            ["periodo_key", "descripcion", "fecha_compra", "monto_original", "saldo_pendiente", "pago_requerido", "progreso"],
            12,
        ),
    }


def _tool_tracker_summary(ctx: FinanceChatContext) -> dict:
    current = ctx.tracker_state or {}
    gastos = current.get("gastos", [])
    presupuesto = _currency(current.get("presupuesto"))
    gastado = _currency(sum(float(g.get("monto", 0) or 0) for g in gastos))
    categories: dict[str, float] = {}
    for gasto in gastos:
        category = (gasto.get("categoria") or "No identificado").strip() or "No identificado"
        categories[category] = categories.get(category, 0.0) + float(gasto.get("monto", 0) or 0)

    history = []
    for archive in ctx.tracker_history[:8]:
        history.append({
            "id": archive.get("id"),
            "label": archive.get("label"),
            "primer_gasto": archive.get("primer_gasto"),
            "ultimo_gasto": archive.get("ultimo_gasto"),
            "total_gastado": _currency(archive.get("total_gastado")),
            "presupuesto": _currency(archive.get("presupuesto")),
            "movimientos": len(archive.get("gastos", [])),
        })

    return {
        "current": {
            "ciclo_inicio": current.get("ciclo_inicio"),
            "presupuesto": presupuesto,
            "gastado": gastado,
            "restante": _currency(presupuesto - gastado),
            "movimientos": len(gastos),
            "top_categories": [
                {"categoria": k, "monto": _currency(v)}
                for k, v in sorted(categories.items(), key=lambda item: item[1], reverse=True)[:10]
            ],
        },
        "history": history,
    }


TOOL_SPECS = [
    {
        "type": "function",
        "function": {
            "name": "get_credit_debt_history",
            "description": "Historial de deuda de tarjeta, deuda MSI, compras y pagos por periodo.",
            "parameters": {
                "type": "object",
                "properties": {
                    "months": {"type": "integer", "minimum": 1, "maximum": 18, "default": 8}
                },
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_spending_by_category",
            "description": "Egresos por categoria y periodo para debito, credito o ambos.",
            "parameters": {
                "type": "object",
                "properties": {
                    "product": {"type": "string", "enum": ["débito", "crédito", "todos"], "default": "crédito"},
                    "months": {"type": "integer", "minimum": 1, "maximum": 12, "default": 4},
                },
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_recent_large_movements",
            "description": "Movimientos de egreso mas grandes para detectar compras que explican cambios.",
            "parameters": {
                "type": "object",
                "properties": {
                    "product": {"type": "string", "enum": ["débito", "crédito", "todos"], "default": "crédito"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 25, "default": 12},
                },
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_msi_summary",
            "description": "Resumen historico de MSI activos y planes con mayor saldo pendiente.",
            "parameters": {
                "type": "object",
                "properties": {
                    "months": {"type": "integer", "minimum": 1, "maximum": 18, "default": 6}
                },
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_tracker_summary",
            "description": "Resumen del tracker actual e historial archivado.",
            "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
        },
    },
]


TOOL_HANDLERS: dict[str, Callable[..., dict]] = {
    "get_credit_debt_history": _tool_credit_debt_history,
    "get_spending_by_category": _tool_spending_by_category,
    "get_recent_large_movements": _tool_recent_large_movements,
    "get_msi_summary": _tool_msi_summary,
    "get_tracker_summary": _tool_tracker_summary,
}


def _extract_message(response_json: dict) -> dict:
    choices = response_json.get("choices") or []
    if not choices:
        raise RuntimeError("OpenAI API no devolvio choices.")
    return choices[0].get("message") or {}


def _call_openai(messages: list[dict], *, tools: list[dict] | None = None) -> dict:
    if load_dotenv is not None:
        load_dotenv()

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("Define OPENAI_API_KEY para usar el chat financiero.")

    api_base = os.environ.get("OPENAI_API_BASE", "https://api.openai.com/v1").rstrip("/")
    model = os.environ.get("OPENAI_MODEL", "gpt-5-mini")
    body = {
        "model": model,
        "messages": messages,
    }
    if tools:
        body["tools"] = tools
        body["tool_choice"] = "auto"

    req = urllib.request.Request(
        url=f"{api_base}/chat/completions",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        response_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"OpenAI API devolvio HTTP {exc.code}: {response_body}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"No se pudo conectar a OpenAI API: {exc}") from exc


def answer_finance_question(
    ctx: FinanceChatContext,
    chat_history: list[dict],
    question: str,
) -> str:
    """Responde una pregunta usando tool calling de solo lectura."""
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    messages.extend(chat_history[-8:])
    messages.append({"role": "user", "content": question})

    first = _extract_message(_call_openai(messages, tools=TOOL_SPECS))
    tool_calls = first.get("tool_calls") or []

    if not tool_calls:
        return (first.get("content") or "").strip() or "No pude generar una respuesta."

    messages.append(first)
    for tool_call in tool_calls:
        function = tool_call.get("function") or {}
        name = function.get("name", "")
        handler = TOOL_HANDLERS.get(name)
        try:
            args = json.loads(function.get("arguments") or "{}")
        except json.JSONDecodeError:
            args = {}

        if handler is None:
            result = {"error": f"Herramienta no disponible: {name}"}
        else:
            result = handler(ctx, **args)

        messages.append({
            "role": "tool",
            "tool_call_id": tool_call.get("id"),
            "name": name,
            "content": json.dumps(result, ensure_ascii=False),
        })

    final = _extract_message(_call_openai(messages))
    return (final.get("content") or "").strip() or "No pude generar una respuesta."
