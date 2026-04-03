"""
Parser de estado de cuenta Tarjeta de Crédito BBVA México (Oro).

Extrae dos fuentes de datos:
1. Movimientos regulares (no a meses) → mismo esquema que movimientos.csv
2. Compras diferidas a meses sin intereses → msi_activos.csv
"""

import re
import csv
import sys
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

import pdfplumber

# ---------------------------------------------------------------------------
# Límites de columnas — sección REGULAR (x0 en puntos PDF)
# ---------------------------------------------------------------------------
REG_LIQ_MAX  = 120   # fecha operación
REG_DESC_MAX = 200   # fecha cargo/liquidación
REG_SIGN_MIN = 510   # signo (+/-) y monto

# Límites de columnas — sección MSI
MSI_FECHA_MAX = 75   # fecha operación (x0 ≈ 21-24)
MSI_DESC_MAX  = 305  # descripción (empieza en x0 ≈ 82)
MSI_ORIG_MAX  = 365  # monto original
MSI_PEND_MAX  = 425  # saldo pendiente
MSI_PAY_MAX   = 490  # pago requerido
MSI_NUM_MAX   = 550  # "N de M"
# >= 550 → tasa

CC_DATE_RE = re.compile(r"^\d{2}-[a-záéíóú]{3}-\d{4}$", re.IGNORECASE)
AMOUNT_RE  = re.compile(r"^\$?[\d,]+\.\d{2}$")

MONTH_MAP = {
    "ene": "01", "feb": "02", "mar": "03", "abr": "04",
    "may": "05", "jun": "06", "jul": "07", "ago": "08",
    "sep": "09", "oct": "10", "nov": "11", "dic": "12",
}


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class CreditMovement:
    fecha_oper: str = ""
    fecha_liq: str = ""
    producto: str = "crédito"
    descripcion: str = ""
    referencia: str = ""
    tipo: str = ""          # "egreso" | "ingreso"
    cargo: Optional[float] = None
    abono: Optional[float] = None


@dataclass
class MsiPlan:
    fecha_compra: str = ""
    descripcion: str = ""
    monto_original: Optional[float] = None
    saldo_pendiente: Optional[float] = None
    pago_requerido: Optional[float] = None
    pago_num: int = 0
    total_pagos: int = 0
    tasa: float = 0.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_date(raw: str) -> str:
    """DD-mes-YYYY → YYYY-MM-DD"""
    day, mon, year = raw.split("-")
    return f"{year}-{MONTH_MAP[mon.lower()]}-{day.zfill(2)}"


def _parse_amount(text: str) -> float:
    return float(text.lstrip("$").replace(",", ""))


def _words_to_lines(words: list[dict], tolerance: float = 3.0) -> list[dict]:
    if not words:
        return []
    lines, current_line = [], []
    current_top = words[0]["top"]
    for w in sorted(words, key=lambda x: (x["top"], x["x0"])):
        if abs(w["top"] - current_top) <= tolerance:
            current_line.append(w)
        else:
            lines.append({"top": current_top, "words": current_line})
            current_top, current_line = w["top"], [w]
    if current_line:
        lines.append({"top": current_top, "words": current_line})
    return lines


def _line_cols_regular(words: list[dict]) -> dict:
    cols = {"oper": [], "liq": [], "desc": [], "sign_amount": []}
    for w in words:
        x = w["x0"]
        if x < REG_LIQ_MAX:
            cols["oper"].append(w["text"])
        elif x < REG_DESC_MAX:
            cols["liq"].append(w["text"])
        elif x < REG_SIGN_MIN:
            cols["desc"].append(w["text"])
        else:
            cols["sign_amount"].append(w["text"])
    return {k: " ".join(v).strip() for k, v in cols.items()}


def _line_cols_msi(words: list[dict]) -> dict:
    cols = {"fecha": [], "desc": [], "orig": [], "pend": [], "pay": [], "num": [], "tasa": []}
    for w in words:
        x = w["x0"]
        if x < MSI_FECHA_MAX:
            cols["fecha"].append(w["text"])
        elif x < MSI_DESC_MAX:
            cols["desc"].append(w["text"])
        elif x < MSI_ORIG_MAX:
            cols["orig"].append(w["text"])
        elif x < MSI_PEND_MAX:
            cols["pend"].append(w["text"])
        elif x < MSI_PAY_MAX:
            cols["pay"].append(w["text"])
        elif x < MSI_NUM_MAX:
            cols["num"].append(w["text"])
        else:
            cols["tasa"].append(w["text"])
    return {k: " ".join(v).strip() for k, v in cols.items()}


def _parse_sign_amount(raw: str) -> tuple[str, float]:
    """'+  $600.00' → ('+', 600.0)"""
    parts = raw.split()
    sign = "+"
    amount_str = ""
    for p in parts:
        if p in ("+", "-"):
            sign = p
        elif AMOUNT_RE.match(p):
            amount_str = p
    if not amount_str:
        return sign, 0.0
    return sign, _parse_amount(amount_str)


def _is_continuation(cols: dict) -> bool:
    """Líneas de desglose IVA, tipo de cambio, pies de página, etc."""
    desc = cols.get("desc", "")
    skip_starts = ("IVA", "MXP", "USD", "Notas:", "Número de cuenta", "CARGOS,COMPRAS",
                   "Fecha", "de la", "operación", "de cargo", "Descripción", "COMPRAS Y CARGOS",
                   "Tarjeta titular", "Tasa de", "interés", "aplicable", "pendiente",
                   "requerido", "pago", "TOTAL", "Página", "BBVA", "Av.")
    return any(desc.startswith(k) for k in skip_starts)


# ---------------------------------------------------------------------------
# Extracción principal
# ---------------------------------------------------------------------------

def extract_credit_data(pdf_path: str) -> tuple[list[CreditMovement], list[MsiPlan], float]:
    movements: list[CreditMovement] = []
    msi_plans: list[MsiPlan] = []
    adeudo_anterior = 0.0
    current: Optional[CreditMovement] = None

    # Estados del parser
    IN_NONE = 0
    IN_MSI = 1
    IN_REGULAR = 2
    state = IN_NONE

    with pdfplumber.open(pdf_path) as pdf:
        if len(pdf.pages) < 2:
            raise ValueError("El estado de cuenta de credito no tiene suficientes paginas.")
        # Extraer adeudo anterior desde página 2
        page2_text = pdf.pages[1].extract_text() or ""
        m = re.search(r"Adeudo del periodo anterior\s+\$([\d,]+\.\d{2})", page2_text)
        if not m:
            raise ValueError("No se pudo detectar el adeudo anterior del estado de cuenta.")
        adeudo_anterior = _parse_amount(m.group(1))

        for page in pdf.pages:
            words = page.extract_words()
            lines = _words_to_lines(words)

            for line in lines:
                full_text = " ".join(w["text"] for w in line["words"])

                # Detectar inicio de secciones
                if "COMPRAS Y CARGOS DIFERIDOS A MESES SIN INTERESES" in full_text:
                    state = IN_MSI
                    continue
                if "CARGOS,COMPRAS Y ABONOS REGULARES" in full_text:
                    state = IN_REGULAR
                    continue
                if "TOTAL CARGOS" in full_text or "TOTAL ABONOS" in full_text:
                    state = IN_NONE
                    if current:
                        movements.append(current)
                        current = None
                    continue

                if state == IN_NONE:
                    continue

                # --- Sección MSI ---
                if state == IN_MSI:
                    cols = _line_cols_msi(line["words"])
                    fecha = cols["fecha"]
                    if not CC_DATE_RE.match(fecha):
                        continue
                    orig  = cols["orig"].lstrip("$")
                    pend  = cols["pend"].lstrip("$")
                    pay   = cols["pay"].lstrip("$")
                    num_raw = cols["num"]          # e.g. "8 de 20"
                    tasa_raw = cols["tasa"].rstrip("%")

                    # Parsear "N de M"
                    num_match = re.match(r"(\d+)\s+de\s+(\d+)", num_raw, re.IGNORECASE)
                    pago_num = int(num_match.group(1)) if num_match else 0
                    total_pagos = int(num_match.group(2)) if num_match else 0

                    plan = MsiPlan(
                        fecha_compra    = _parse_date(fecha),
                        descripcion     = cols["desc"],
                        monto_original  = _parse_amount(orig) if orig else None,
                        saldo_pendiente = _parse_amount(pend) if pend else None,
                        pago_requerido  = _parse_amount(pay) if pay else None,
                        pago_num        = pago_num,
                        total_pagos     = total_pagos,
                        tasa            = float(tasa_raw) if tasa_raw else 0.0,
                    )
                    msi_plans.append(plan)

                # --- Sección Regular ---
                elif state == IN_REGULAR:
                    cols = _line_cols_regular(line["words"])

                    if CC_DATE_RE.match(cols["oper"]):
                        # Guardar movimiento anterior
                        if current:
                            movements.append(current)

                        sign_amount = cols["sign_amount"]
                        if not sign_amount:
                            # Línea de fecha sin monto todavía (raro) — init vacío
                            current = CreditMovement(
                                fecha_oper = _parse_date(cols["oper"]),
                                fecha_liq  = _parse_date(cols["liq"]) if CC_DATE_RE.match(cols["liq"]) else "",
                                descripcion = cols["desc"],
                            )
                            continue

                        sign, amount = _parse_sign_amount(sign_amount)
                        current = CreditMovement(
                            fecha_oper  = _parse_date(cols["oper"]),
                            fecha_liq   = _parse_date(cols["liq"]) if CC_DATE_RE.match(cols["liq"]) else "",
                            descripcion = cols["desc"],
                        )
                        if sign == "+":
                            current.tipo  = "egreso"
                            current.cargo = amount
                        else:
                            current.tipo  = "ingreso"
                            current.abono = amount

                    elif current is not None:
                        # Línea de continuación (IVA, tipo de cambio, etc.)
                        if _is_continuation(cols):
                            continue
                        extra = cols["desc"].strip()
                        if extra and not current.referencia:
                            current.referencia = extra

        # Guardar último si quedó pendiente
        if current:
            movements.append(current)

    return movements, msi_plans, adeudo_anterior


# ---------------------------------------------------------------------------
# Guardado CSV
# ---------------------------------------------------------------------------

MOVEMENT_FIELDS = [
    "fecha_oper", "fecha_liq", "producto", "descripcion", "referencia",
    "tipo", "cargo", "abono", "saldo_acumulado",
]

MSI_FIELDS = [
    "fecha_compra", "descripcion", "monto_original", "saldo_pendiente",
    "pago_requerido", "pago_num", "total_pagos", "tasa",
]


def save_movements_csv(movements: list[CreditMovement], output_path: str, saldo_inicial: float) -> None:
    saldo = saldo_inicial
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=MOVEMENT_FIELDS)
        writer.writeheader()
        for m in movements:
            if m.cargo is not None:
                saldo += m.cargo
            elif m.abono is not None:
                saldo -= m.abono
            writer.writerow({
                "fecha_oper":     m.fecha_oper,
                "fecha_liq":      m.fecha_liq,
                "producto":       m.producto,
                "descripcion":    m.descripcion,
                "referencia":     m.referencia,
                "tipo":           m.tipo,
                "cargo":          m.cargo if m.cargo is not None else "",
                "abono":          m.abono if m.abono is not None else "",
                "saldo_acumulado": round(saldo, 2),
            })
    print(f"Movimientos crédito guardados: {output_path} ({len(movements)} movimientos)")


def save_msi_csv(plans: list[MsiPlan], output_path: str) -> None:
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=MSI_FIELDS)
        writer.writeheader()
        for p in plans:
            writer.writerow({
                "fecha_compra":    p.fecha_compra,
                "descripcion":     p.descripcion,
                "monto_original":  p.monto_original if p.monto_original is not None else "",
                "saldo_pendiente": p.saldo_pendiente if p.saldo_pendiente is not None else "",
                "pago_requerido":  p.pago_requerido if p.pago_requerido is not None else "",
                "pago_num":        p.pago_num,
                "total_pagos":     p.total_pagos,
                "tasa":            p.tasa,
            })
    print(f"MSI activos guardados: {output_path} ({len(plans)} planes)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    project_root = Path(__file__).resolve().parents[2]
    pdf_path = sys.argv[1] if len(sys.argv) > 1 else str(
        project_root / "data" / "raw" / "Estado de Cuenta - YEXET3QM.pdf"
    )
    mov_out = str(project_root / "data" / "processed" / "movimientos_credito.csv")
    msi_out = str(project_root / "data" / "processed" / "msi_activos.csv")

    print(f"Procesando: {pdf_path}")
    movs, plans, adeudo = extract_credit_data(pdf_path)

    print(f"Adeudo anterior: ${adeudo:,.2f}")
    print(f"Movimientos encontrados: {len(movs)}")
    for m in movs:
        tipo = f"CARGO  ${m.cargo:>10.2f}" if m.cargo else f"ABONO  ${m.abono:>10.2f}"
        print(f"  {m.fecha_oper}  {tipo}  {m.descripcion[:50]}")

    print(f"\nPlanes MSI encontrados: {len(plans)}")
    for p in plans:
        print(f"  {p.fecha_compra}  saldo=${p.saldo_pendiente:>10,.2f}  {p.pago_num}/{p.total_pagos}  {p.descripcion[:40]}")

    save_movements_csv(movs, mov_out, adeudo)
    save_msi_csv(plans, msi_out)

    # Extraer y guardar métricas clave del estado de cuenta
    metricas_out = str(project_root / "data" / "processed" / "metricas_credito.json")
    with pdfplumber.open(pdf_path) as pdf:
        page2 = pdf.pages[1].extract_text() or ""

    def _find(pattern):
        m = re.search(pattern, page2)
        return _parse_amount(m.group(1)) if m else None

    metricas = {
        "pago_sin_intereses":   _find(r"Pago para no generar intereses:\d?\s*\$([\d,]+\.\d{2})"),
        "saldo_cargos_regulares": _find(r"Saldo cargos regulares:\s*\$([\d,]+\.\d{2})"),
        "saldo_msi":            _find(r"Saldo cargo a meses:\s*\$([\d,]+\.\d{2})"),
        "saldo_deudor_total":   _find(r"Saldo deudor total:\d*\s*\$([\d,]+\.\d{2})"),
        "limite_credito":       _find(r"Límite de crédito:\s*\$([\d,]+\.\d{2})"),
        "credito_disponible":   _find(r"Crédito disponible:\s*\$([\d,]+\.\d{2})"),
        "pago_minimo":          _find(r"Pago mínimo:\d?\s*\$([\d,]+\.\d{2})"),
        "adeudo_anterior":      adeudo,
    }
    with open(metricas_out, "w", encoding="utf-8") as f:
        json.dump(metricas, f, indent=2)
    print(f"Métricas guardadas: {metricas_out}")
    print(f"  Saldo deudor total: ${metricas['saldo_deudor_total']:,.2f}")
