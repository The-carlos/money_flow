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
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - dependencia opcional
    load_dotenv = None


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CSV_PATH = PROJECT_ROOT / "data" / "processed" / "movimientos_consolidados.csv"
API_BASE = os.environ.get("OPENAI_API_BASE", "https://api.openai.com/v1").rstrip("/")

CATEGORIAS = [
    "Alimentación",
    "Supermercado y Farmacia",
    "Transporte",
    "Entretenimiento",
    "Salud y Bienestar",
    "Tecnología",
    "Streaming y Suscripciones Digitales",
    "Educación",
    "Ropa y Deporte",
    "Servicios del Hogar y Telecomunicaciones",
    "Mascotas",
    "Transferencias Personales",
    "Ahorro e Inversiones",
    "Nómina e Ingresos",
    "Depósito en Efectivo",
    "Retiro en Efectivo",
    "Pago de Tarjeta de Crédito",
    "Pago MSI",
    "Comisiones Bancarias",
    "No identificado",
]

SYSTEM_PROMPT = f"""Eres un categorizador experto de movimientos bancarios del mercado mexicano.
Debes clasificar cada movimiento usando la descripcion, la referencia y el contexto bancario.

Categorias permitidas:
{chr(10).join(f"- {c}" for c in CATEGORIAS)}

Reglas:
- Usa solo una categoria de la lista.
- Si la descripcion o referencia no son suficientemente descriptivas, responde "No identificado".
- "PAGO CUENTA DE TERCERO", "SPEI ENVIADO" y "SPEI RECIBIDO" deben clasificarse como Transferencias Personales.
- SPEI es una transferencia interbancaria; no lo clasifiques como Ahorro e Inversiones solo por mencionar una institucion bancaria o fintech.
- Usa Ahorro e Inversiones solo cuando el texto indique claramente ahorro, inversion, fondeo de cuenta propia o movimiento patrimonial y no sea simplemente un SPEI generico.
- "RETIRO SIN TARJETA" -> Retiro en Efectivo.
- "SU PAGO EN EFECTIVO EN COMERCIO" -> Depósito en Efectivo.
- "PAGO DE NOMINA" -> Nómina e Ingresos.
- "BMOVIL.PAGO TDC" o "PAGO TARJETA DE CREDITO" -> Pago de Tarjeta de Crédito.
- Movimientos del estilo "06 DE 12" o "A 03 MSI" -> Pago MSI.
- Uber y viajes similares -> Transporte.
- Streaming, suscripciones, música, video -> Streaming y Suscripciones Digitales.
- OpenAI, ChatGPT, Platzi, IPN y similares -> Educación.

Responde solo JSON valido con este formato:
{{
  "items": [
    {{"index": 12, "categoria": "Alimentación"}}
  ]
}}
"""

SCHEMA = {
    "name": "movement_categories",
    "strict": True,
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "items": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "index": {"type": "integer"},
                        "categoria": {
                            "type": "string",
                            "enum": CATEGORIAS,
                        },
                    },
                    "required": ["index", "categoria"],
                },
            },
        },
        "required": ["items"],
    },
}


def _chunk(items: list[dict], size: int) -> list[list[dict]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def _extract_content(response_json: dict) -> str:
    for choice in response_json.get("choices", []):
        message = choice.get("message", {})
        content = message.get("content")
        if isinstance(content, str):
            return content
    raise ValueError("La respuesta de OpenAI no trajo contenido de texto.")


def _call_openai(payload_items: list[dict]) -> dict[int, str]:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("Define OPENAI_API_KEY antes de categorizar.")

    model = os.environ.get("OPENAI_MODEL", "gpt-5-mini")
    request_body = {
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": (
                    "Clasifica estos movimientos. Si no hay contexto suficiente, usa "
                    '"No identificado".\n\n'
                    + json.dumps(payload_items, ensure_ascii=False, indent=2)
                ),
            },
        ],
        "response_format": {
            "type": "json_schema",
            "json_schema": SCHEMA,
        },
    }

    req = urllib.request.Request(
        url=f"{API_BASE}/chat/completions",
        data=json.dumps(request_body).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            response_json = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"OpenAI API devolvio HTTP {exc.code}: {body}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"No se pudo conectar a OpenAI API: {exc}") from exc

    content = _extract_content(response_json)
    parsed = json.loads(content)
    return {item["index"]: item["categoria"] for item in parsed["items"]}


def categorize_movements(rows: list[dict]) -> dict[int, str]:
    """Llama a OpenAI API y regresa un dict index -> categoria."""
    to_categorize = [
        {
            "index": i,
            "descripcion": r.get("descripcion", ""),
            "referencia": r.get("referencia", ""),
            "tipo": r.get("tipo", ""),
            "monto": r.get("cargo") or r.get("abono") or "",
        }
        for i, r in enumerate(rows)
        if r.get("categoria", "").strip() in ("", "Indefinido", "No identificado")
    ]

    if not to_categorize:
        print("Todos los movimientos ya estan categorizados.")
        return {}

    batch_size = int(os.environ.get("OPENAI_CATEGORIZE_BATCH", "50"))
    print(f"Categorizando {len(to_categorize)} movimientos con OpenAI...")

    categoria_map: dict[int, str] = {}
    for batch_num, batch in enumerate(_chunk(to_categorize, batch_size), start=1):
        print(f"  Lote {batch_num}: {len(batch)} movimientos")
        batch_map = _call_openai(batch)
        categoria_map.update(batch_map)
        for item in batch:
            categoria_map.setdefault(item["index"], "No identificado")

    return categoria_map


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
        if row.get("categoria") == "Indefinido":
            row["categoria"] = "No identificado"
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
