"""
Reglas de categorización automática compartidas entre pipeline y apply_categories.
"""

import re


def auto_category(descripcion: str, referencia: str = "") -> str:
    text = (descripcion + " " + referencia).upper()

    rules = [
        # MSI — antes que MercadoPago genérico
        (r"\d+ DE \d+|\bA \d+ MSI\b|\bA \d+ MESES\b",         "Pago MSI"),
        # Pagos bancarios
        (r"PAGO TARJETA DE CREDITO|BMOVIL\.PAGO TDC",          "Pago de Tarjeta de Crédito"),
        (r"PAGO DE NOMINA",                                     "Nómina e Ingresos"),
        (r"COMISION DE OP|SEGBBVA",                             "Comisiones Bancarias"),
        (r"RETIRO SIN TARJETA",                                 "Retiro en Efectivo"),
        (r"SU PAGO EN EFECTIVO",                                "Depósito en Efectivo"),
        # Transferencias interbancarias
        (r"PAGO CUENTA DE TERCERO|SPEI ",                       "Transferencias Personales"),
        # Ahorro e Inversiones sin SPEI explicito
        (r"\b(albo|FONDEADORA|MERCADO.?PAGO|INBURSA|NU.?MEXICO|STORI)\b",
                                                                "Ahorro e Inversiones"),
        # Alimentación
        (r"TACO|REST\b|RESTAUR|UDON|SANBORNS|OXXO|MERPAGO\*COMIDA|TAQUER"
         r"|CAFE\b|CAFETERIA|LATTE|COMIDA|COMFYFOODS|RESTITACATLAN"
         r"|CHEPE|PATIO|LUMEN|ESPERANZA|CIHUACOATL|MINIBOX|APECCAFE"
         r"|HELADERIA|DESAYUNO",                                "Alimentación"),
        # Farmacia / Super
        (r"FARMACIA|FARM\b|SIMILARES",                          "Supermercado y Farmacia"),
        # Mascotas
        (r"PETSHOP|VET\b|FARM ANIMALS",                         "Mascotas"),
        # Transporte
        (r"\bUBER\b|DLO\*TDA UBER",                             "Transporte"),
        # Streaming
        (r"NETFLIX|SPOTIFY|HBO|EBW\*SPOTIFY",                   "Streaming y Suscripciones Digitales"),
        # Tecnología
        (r"GOOGLE ONE|APPLE\.COM|AMAZON|DAYLIO|MERCADOLIBRE|LIVERPOOL",
                                                                "Tecnología"),
        # Educación
        (r"OPENAI|CHATGPT|PLATZI|IPN|ESCOLAR",                  "Educación"),
        # Salud
        (r"GYMPASS|BARBERS|SPA\b|HOSPITAL",                     "Salud y Bienestar"),
        # Deporte / Ropa
        (r"DECATHLON",                                          "Ropa y Deporte"),
        # Telecomunicaciones
        (r"TOTAL.?PLAY|TELCEL",                                 "Servicios del Hogar y Telecomunicaciones"),
        # Entretenimiento
        (r"HOTEL|LIV\b|CLUB|LOVE.?SHOP",                       "Entretenimiento"),
    ]

    for pattern, category in rules:
        if re.search(pattern, text, re.IGNORECASE):
            return category

    return "No identificado"
