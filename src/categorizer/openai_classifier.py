"""
Helpers compartidos para clasificar movimientos con OpenAI API.
"""

import json
import os
import urllib.error
import urllib.request


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

SYSTEM_PROMPT = f"""Eres un categorizador experto de movimientos financieros del mercado mexicano.
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


def normalize_category(value: str) -> str:
    category = (value or "").strip()
    if category in ("", "Indefinido"):
        return "No identificado"
    return category


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

    api_base = os.environ.get("OPENAI_API_BASE", "https://api.openai.com/v1").rstrip("/")
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
        url=f"{api_base}/chat/completions",
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


def categorize_rows(
    rows: list[dict],
    *,
    description_key: str = "descripcion",
    reference_key: str = "referencia",
    type_key: str = "tipo",
    amount_key: str = "monto",
    category_key: str = "categoria",
    batch_size_env: str = "OPENAI_CATEGORIZE_BATCH",
    recategorize_all: bool = False,
) -> dict[int, str]:
    """Clasifica filas que no tengan categoria util o, opcionalmente, todas."""
    payload = []
    for i, row in enumerate(rows):
        current = normalize_category(row.get(category_key, ""))
        if not recategorize_all and current != "No identificado":
            continue
        payload.append({
            "index": i,
            "descripcion": row.get(description_key, ""),
            "referencia": row.get(reference_key, ""),
            "tipo": row.get(type_key, ""),
            "monto": row.get(amount_key, "") or "",
        })

    if not payload:
        return {}

    batch_size = int(os.environ.get(batch_size_env, "50"))
    categoria_map: dict[int, str] = {}
    for batch in _chunk(payload, batch_size):
        batch_map = _call_openai(batch)
        categoria_map.update(batch_map)
        for item in batch:
            categoria_map.setdefault(item["index"], "No identificado")
    return categoria_map
