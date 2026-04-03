"""
Parser de estados de cuenta BBVA México.

Extrae los movimientos de la sección "Detalle de Movimientos Realizados"
usando coordenadas de palabras para mapear columnas correctamente.
"""

import re
import csv
import sys
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

import pdfplumber

# ---------------------------------------------------------------------------
# Límites de columnas (x0 en puntos PDF, calibrados desde el estado de cuenta)
# ---------------------------------------------------------------------------
COL_OPER_MAX     = 55     # Fecha operación
COL_LIQ_MAX      = 100    # Fecha liquidación
COL_DESC_MAX     = 370    # Descripción + referencia
COL_CARGOS_MAX   = 420    # Cargos (débitos)
COL_ABONOS_MAX   = 460    # Abonos (créditos)
COL_SALDO_OP_MAX = 540    # Saldo operación

DATE_RE = re.compile(r"^\d{2}/(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)$")
AMOUNT_RE = re.compile(r"^\d{1,3}(?:,\d{3})*\.\d{2}$")

MONTH_MAP = {
    "ENE": "01", "FEB": "02", "MAR": "03", "ABR": "04",
    "MAY": "05", "JUN": "06", "JUL": "07", "AGO": "08",
    "SEP": "09", "OCT": "10", "NOV": "11", "DIC": "12",
}


@dataclass
class Movement:
    fecha_oper: str = ""
    fecha_liq: str = ""
    producto: str = "débito"   # "débito" | "crédito"
    descripcion: str = ""
    referencia: str = ""
    tipo: str = ""             # "egreso" | "ingreso"
    cargo: Optional[float] = None
    abono: Optional[float] = None


def _parse_amount(text: str) -> float:
    return float(text.replace(",", ""))


def _normalize_date(raw: str, year: int) -> str:
    """Convierte '27/FEB' → '2026-02-27'"""
    day, mon = raw.split("/")
    return f"{year}-{MONTH_MAP[mon]}-{day.zfill(2)}"


def _assign_column(x0: float, text: str) -> str:
    if x0 < COL_OPER_MAX:
        return "oper"
    if x0 < COL_LIQ_MAX:
        return "liq"
    if x0 < COL_DESC_MAX:
        return "desc"
    if x0 < COL_CARGOS_MAX:
        return "cargo"
    if x0 < COL_ABONOS_MAX:
        return "abono"
    if x0 < COL_SALDO_OP_MAX:
        return "saldo_op"
    return "saldo_liq"


def _words_to_lines(words: list[dict], top_tolerance: float = 3.0) -> list[dict]:
    """Agrupa palabras por línea usando coordenada 'top' con tolerancia."""
    if not words:
        return []

    lines: list[dict] = []
    current_top = words[0]["top"]
    current_line: list[dict] = []

    for w in sorted(words, key=lambda x: (x["top"], x["x0"])):
        if abs(w["top"] - current_top) <= top_tolerance:
            current_line.append(w)
        else:
            lines.append({"top": current_top, "words": current_line})
            current_top = w["top"]
            current_line = [w]

    if current_line:
        lines.append({"top": current_top, "words": current_line})

    return lines


def _line_columns(line_words: list[dict]) -> dict:
    """Devuelve un dict con el texto de cada columna en la línea."""
    cols: dict[str, list[str]] = {
        "oper": [], "liq": [], "desc": [], "cargo": [],
        "abono": [], "saldo_op": [], "saldo_liq": [],
    }
    for w in line_words:
        col = _assign_column(w["x0"], w["text"])
        cols[col].append(w["text"])

    return {k: " ".join(v).strip() for k, v in cols.items()}


def _is_transaction_line(cols: dict) -> bool:
    return bool(DATE_RE.match(cols.get("oper", "")))


def _is_footer_line(cols: dict) -> bool:
    """Líneas de pie de página o cabecera de página que deben ignorarse."""
    desc = cols.get("desc", "")
    oper = cols.get("oper", "")
    all_text = desc + " " + oper
    skip_keywords = [
        "BBVA MEXICO", "Av. Paseo", "La GAT", "PAGINA",
        # Cabeceras de página internas
        "No. de Cuenta", "No. de Cliente",
        "Estado de Cuenta", "Libretón Básico",
    ]
    return any(k in all_text for k in skip_keywords)


def extract_movements(pdf_path: str) -> tuple[list[Movement], float]:
    year = None
    saldo_inicial = 0.0
    movements: list[Movement] = []
    current: Optional[Movement] = None
    in_section = False

    with pdfplumber.open(pdf_path) as pdf:
        if len(pdf.pages) < 2:
            raise ValueError("El estado de cuenta de debito no tiene suficientes paginas.")
        # Detectar año y saldo inicial en página 2
        page2_text = pdf.pages[1].extract_text() or ""
        year_match = re.search(r"AL \d{2}/\d{2}/(\d{4})", page2_text)
        if not year_match:
            raise ValueError("No se pudo detectar el anio del estado de cuenta de debito.")
        year = int(year_match.group(1))
        saldo_match = re.search(r"Saldo Anterior\s+([\d,]+\.\d{2})", page2_text)
        if saldo_match:
            saldo_inicial = _parse_amount(saldo_match.group(1))

        for page in pdf.pages:
            words = page.extract_words()
            lines = _words_to_lines(words)

            for line in lines:
                cols = _line_columns(line["words"])
                full_text = " ".join(w["text"] for w in line["words"])

                # Activar extracción al encontrar la sección
                if "Detalle de Movimientos Realizados" in full_text:
                    in_section = True
                    continue

                # Desactivar al llegar al total de movimientos
                if "TOTAL IMPORTE CARGOS" in full_text or "TOTAL MOVIMIENTOS" in full_text:
                    in_section = False

                if not in_section:
                    continue

                # Ignorar cabecera de columnas y pies de página
                if "DESCRIPCION" in full_text or "REFERENCIA" in full_text:
                    continue
                if _is_footer_line(cols):
                    continue

                if _is_transaction_line(cols):
                    # Guardar movimiento anterior
                    if current is not None:
                        movements.append(current)

                    current = Movement()
                    current.fecha_oper = _normalize_date(cols["oper"], year)

                    if DATE_RE.match(cols.get("liq", "")):
                        current.fecha_liq = _normalize_date(cols["liq"], year)

                    current.descripcion = cols["desc"]

                    if AMOUNT_RE.match(cols.get("cargo", "")):
                        current.cargo = _parse_amount(cols["cargo"])
                        current.tipo = "egreso"
                    if AMOUNT_RE.match(cols.get("abono", "")):
                        current.abono = _parse_amount(cols["abono"])
                        current.tipo = "ingreso"

                elif current is not None:
                    # Línea de continuación: referencia o descripción extra
                    extra = cols["desc"].strip()
                    if extra:
                        if not current.referencia and ("Referencia" in extra or extra.startswith("BNET")):
                            current.referencia = extra
                        else:
                            # Puede ser descripción extra (ej. nombre del beneficiario)
                            current.descripcion = (current.descripcion + " " + extra).strip()


        # Guardar último movimiento
        if current is not None:
            movements.append(current)

    return movements, saldo_inicial


def save_to_csv(movements: list[Movement], output_path: str, saldo_inicial: float = 0.0) -> None:
    fieldnames = [
        "fecha_oper", "fecha_liq", "producto", "descripcion", "referencia",
        "tipo", "cargo", "abono", "saldo_acumulado",
    ]
    saldo = saldo_inicial
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for m in movements:
            if m.cargo is not None:
                saldo -= m.cargo
            elif m.abono is not None:
                saldo += m.abono
            writer.writerow({
                "fecha_oper": m.fecha_oper,
                "fecha_liq": m.fecha_liq,
                "producto": m.producto,
                "descripcion": m.descripcion,
                "referencia": m.referencia,
                "tipo": m.tipo,
                "cargo": m.cargo if m.cargo is not None else "",
                "abono": m.abono if m.abono is not None else "",
                "saldo_acumulado": round(saldo, 2),
            })
    print(f"CSV guardado en: {output_path} ({len(movements)} movimientos)")


if __name__ == "__main__":
    project_root = Path(__file__).resolve().parents[2]
    pdf_path = sys.argv[1] if len(sys.argv) > 1 else str(
        project_root / "data" / "raw" / "Estado de Cuenta - RJ8XR6CV.pdf"
    )
    output_path = sys.argv[2] if len(sys.argv) > 2 else str(
        project_root / "data" / "processed" / "movimientos.csv"
    )

    print(f"Procesando: {pdf_path}")
    movs, saldo_inicial = extract_movements(pdf_path)
    print(f"Saldo inicial: ${saldo_inicial:,.2f}")
    print(f"Movimientos encontrados: {len(movs)}")
    for m in movs:
        tipo = f"CARGO  ${m.cargo:>10.2f}" if m.cargo else f"ABONO  ${m.abono:>10.2f}"
        print(f"  {m.fecha_oper}  {tipo}  {m.descripcion[:50]}")

    save_to_csv(movs, output_path, saldo_inicial)
