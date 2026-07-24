"""
Repara fechas mal capturadas en track_ciclo.json.

Uso seguro:
    python3 src/tracker/repair_dates.py

Aplicar cambios:
    python3 src/tracker/repair_dates.py --apply
"""

from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TRACK_PATH = PROJECT_ROOT / "data" / "processed" / "track_ciclo.json"


def _parse_date(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d")


def _month_delta(year: int, month: int, delta: int) -> tuple[int, int]:
    month += delta
    while month > 12:
        year += 1
        month -= 12
    while month < 1:
        year -= 1
        month += 12
    return year, month


def _shift_month(value: datetime, month_offset: int) -> datetime:
    year, month = _month_delta(value.year, value.month, month_offset)
    return value.replace(year=year, month=month)


def _load_state(path: Path) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _save_state(path: Path, state: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)


def repair_dates(
    state: dict,
    *,
    bad_start: datetime,
    bad_end: datetime,
    month_offset: int,
) -> list[dict]:
    changes = []
    for idx, gasto in enumerate(state.get("gastos", [])):
        raw_fecha = str(gasto.get("fecha", ""))
        try:
            fecha = datetime.fromisoformat(raw_fecha)
        except ValueError:
            continue

        if not (bad_start.date() <= fecha.date() <= bad_end.date()):
            continue

        fixed_fecha = _shift_month(fecha, month_offset)
        changes.append({
            "index": idx,
            "old": raw_fecha,
            "new": fixed_fecha.isoformat(timespec="seconds"),
            "monto": float(gasto.get("monto", 0) or 0),
            "descripcion": str(gasto.get("descripcion", "")),
            "categoria": str(gasto.get("categoria", "")),
        })
        gasto["fecha"] = fixed_fecha.isoformat(timespec="seconds")
    return changes


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Corrige fechas del tracker moviendo un rango de fechas por meses completos."
    )
    parser.add_argument("--path", type=Path, default=DEFAULT_TRACK_PATH)
    parser.add_argument("--bad-start", default="2026-06-13")
    parser.add_argument("--bad-end", default="2026-06-21")
    parser.add_argument("--month-offset", type=int, default=1)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    if not args.path.exists():
        raise SystemExit(f"No existe el archivo: {args.path}")

    state = _load_state(args.path)
    changes = repair_dates(
        state,
        bad_start=_parse_date(args.bad_start),
        bad_end=_parse_date(args.bad_end),
        month_offset=args.month_offset,
    )

    if not changes:
        print("No se encontraron gastos dentro del rango indicado.")
        return

    print(f"Gastos a corregir: {len(changes)}")
    for change in changes:
        print(
            f"- #{change['index']:03d} {change['old'][:10]} -> {change['new'][:10]} "
            f"${change['monto']:,.2f} {change['descripcion']} [{change['categoria']}]"
        )

    if not args.apply:
        print("\nDry-run solamente. Vuelve a ejecutar con --apply para escribir cambios.")
        return

    backup = args.path.with_suffix(args.path.suffix + f".bak-{datetime.now().strftime('%Y%m%d%H%M%S')}")
    shutil.copy2(args.path, backup)
    _save_state(args.path, state)
    print(f"\nBackup: {backup}")
    print(f"Actualizado: {args.path}")


if __name__ == "__main__":
    main()
