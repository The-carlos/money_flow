"""
Persistencia y archivado de ciclos del tracker.
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path


MESES_ES = {
    1: "ene", 2: "feb", 3: "mar", 4: "abr", 5: "may", 6: "jun",
    7: "jul", 8: "ago", 9: "sep", 10: "oct", 11: "nov", 12: "dic",
}


def ensure_history_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_json(path: Path, default: dict) -> dict:
    if path.exists():
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return default


def save_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def cycle_bounds(gastos: list[dict]) -> tuple[datetime, datetime]:
    fechas = sorted(datetime.fromisoformat(g["fecha"]) for g in gastos)
    return fechas[0], fechas[-1]


def cycle_label(start: datetime, end: datetime) -> str:
    return f"{start.day:02d}{MESES_ES[start.month]}-{end.day:02d}{MESES_ES[end.month]}"


def cycle_id(start: datetime, end: datetime) -> str:
    return f"{start.date().isoformat()}__{end.date().isoformat()}"


def category_summary(gastos: list[dict]) -> dict[str, float]:
    summary: dict[str, float] = defaultdict(float)
    for gasto in gastos:
        categoria = (gasto.get("categoria") or "No identificado").strip() or "No identificado"
        summary[categoria] += float(gasto.get("monto", 0) or 0)
    return dict(sorted(summary.items(), key=lambda item: item[0].lower()))


def archive_cycle(state: dict, history_dir: Path) -> dict | None:
    gastos = state.get("gastos", [])
    if not gastos:
        return None

    ensure_history_dir(history_dir)
    start, end = cycle_bounds(gastos)
    archive = {
        "id": cycle_id(start, end),
        "label": cycle_label(start, end),
        "closed_at": datetime.now().isoformat(timespec="seconds"),
        "ciclo_inicio": state.get("ciclo_inicio"),
        "primer_gasto": start.isoformat(timespec="seconds"),
        "ultimo_gasto": end.isoformat(timespec="seconds"),
        "presupuesto": state.get("presupuesto", 0),
        "total_gastado": sum(float(g.get("monto", 0) or 0) for g in gastos),
        "restante": float(state.get("presupuesto", 0) or 0) - sum(float(g.get("monto", 0) or 0) for g in gastos),
        "gastos": gastos,
        "resumen_por_categoria": category_summary(gastos),
    }

    archive_path = history_dir / f"{archive['id']}.json"
    save_json(archive_path, archive)
    return archive
