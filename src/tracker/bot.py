"""
Bot de Telegram para trackear gastos del ciclo de TDC.

Comandos:
  /gasto 350 Uber          → registra un gasto
  /status                  → resumen del ciclo actual
  /update-presupuesto 14000 → cambia el presupuesto disponible
  /reset                   → reinicia el ciclo (borra gastos)

Configuración (variables de entorno o archivo .env):
  TELEGRAM_TOKEN   → token del bot (obtenido de @BotFather)
  TELEGRAM_CHAT_ID → tu chat ID personal (obtenido de @userinfobot)
"""

import json
import logging
import os
from datetime import datetime
from pathlib import Path

from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - dependencia opcional
    load_dotenv = None

# ---------------------------------------------------------------------------
# Rutas
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
TRACK_PATH   = PROJECT_ROOT / "data" / "processed" / "track_ciclo.json"

PRESUPUESTO_DEFAULT = 13168.0

# Estado en memoria para confirmación de reset
_pending_reset = False

logging.basicConfig(
    format="%(asctime)s — %(levelname)s — %(message)s",
    level=logging.INFO,
)

# ---------------------------------------------------------------------------
# Estado del ciclo
# ---------------------------------------------------------------------------

def _load() -> dict:
    if TRACK_PATH.exists():
        with open(TRACK_PATH, encoding="utf-8") as f:
            return json.load(f)
    return {
        "presupuesto": PRESUPUESTO_DEFAULT,
        "gastos": [],
        "ciclo_inicio": datetime.now().isoformat(timespec="seconds"),
    }


def _save(state: dict) -> None:
    with open(TRACK_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)


def _total_gastado(state: dict) -> float:
    return sum(g["monto"] for g in state["gastos"])


def _barra(gastado: float, presupuesto: float, width: int = 10) -> str:
    pct = min(gastado / presupuesto, 1.0) if presupuesto else 0
    filled = round(pct * width)
    bar = "█" * filled + "░" * (width - filled)
    return f"[{bar}] {pct*100:.1f}%"


# ---------------------------------------------------------------------------
# Guard: solo responde al dueño
# ---------------------------------------------------------------------------

def _authorized(update: Update) -> bool:
    chat_id = str(update.effective_chat.id)
    allowed = os.getenv("TELEGRAM_CHAT_ID", "")
    return not allowed or chat_id == allowed


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------

async def cmd_gasto(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update):
        return

    args = context.args  # [monto, resto...]
    if not args:
        await update.message.reply_text("Uso: /gasto 350 concepto_del_gasto\nOpcional: /gasto 350 concepto_del_gasto - Categoria")
        return

    try:
        monto = float(args[0].replace(",", ""))
    except ValueError:
        await update.message.reply_text("El monto debe ser un número. Ejemplo: /gasto 350 uber_viaje")
        return

    # Resto del mensaje: "concepto_del_gasto - Categoria"
    resto = " ".join(args[1:]) if len(args) > 1 else "Sin descripción"

    # Separar descripción y categoría por " - "
    if " - " in resto:
        partes = resto.split(" - ", 1)
        descripcion = partes[0].replace("_", " ").strip()
        categoria   = partes[1].strip()
    else:
        descripcion = resto.replace("_", " ").strip()
        categoria   = ""

    state = _load()
    state["gastos"].append({
        "fecha":       datetime.now().isoformat(timespec="seconds"),
        "monto":       monto,
        "descripcion": descripcion,
        "categoria":   categoria,
    })
    _save(state)

    gastado     = _total_gastado(state)
    presupuesto = state["presupuesto"]
    restante    = presupuesto - gastado
    barra       = _barra(gastado, presupuesto)
    cat_txt     = f" [{categoria}]" if categoria else ""
    uso_pct     = (gastado / presupuesto) if presupuesto > 0 else 0
    alerta      = "\n⚠️ Mas del 80% usado — considera frenar." if uso_pct >= 0.8 else ""
    excedido    = "\n🚨 PRESUPUESTO EXCEDIDO" if gastado > presupuesto else ""

    texto = (
        f"✅ ${monto:,.2f} registrado — {descripcion}{cat_txt}\n\n"
        f"{barra}\n"
        f"Gastado:     ${gastado:,.2f}\n"
        f"Presupuesto: ${presupuesto:,.2f}\n"
        f"Restante:    ${restante:,.2f}"
        f"{alerta}{excedido}"
    )
    await update.message.reply_text(texto)


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update):
        return

    state      = _load()
    gastado    = _total_gastado(state)
    presupuesto = state["presupuesto"]
    restante   = presupuesto - gastado
    barra      = _barra(gastado, presupuesto)
    inicio     = state.get("ciclo_inicio", "—")[:10]
    n_gastos   = len(state["gastos"])

    ultimos = ""
    if state["gastos"]:
        ultimos = "\n\nÚltimos 5 gastos:\n"
        for g in reversed(state["gastos"][-5:]):
            fecha = g["fecha"][5:10]  # MM-DD
            ultimos += f"  • {fecha} ${g['monto']:,.2f} — {g['descripcion']}\n"

    texto = (
        f"📊 Estado del ciclo (desde {inicio})\n\n"
        f"{barra}\n"
        f"Gastado:     ${gastado:,.2f}\n"
        f"Presupuesto: ${presupuesto:,.2f}\n"
        f"Restante:    ${restante:,.2f}\n"
        f"Movimientos: {n_gastos}"
        f"{ultimos}"
    )
    await update.message.reply_text(texto)


async def cmd_update_presupuesto(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update):
        return

    if not context.args:
        await update.message.reply_text("Uso: /update-presupuesto 14000")
        return

    try:
        nuevo = float(context.args[0].replace(",", ""))
    except ValueError:
        await update.message.reply_text("El monto debe ser un número.")
        return
    if nuevo <= 0:
        await update.message.reply_text("El presupuesto debe ser mayor a 0.")
        return

    state = _load()
    anterior = state["presupuesto"]
    state["presupuesto"] = nuevo
    _save(state)

    await update.message.reply_text(
        f"💰 Presupuesto actualizado: *${anterior:,.2f}* → *${nuevo:,.2f}*",
        parse_mode="Markdown",
    )


async def cmd_info(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update):
        return

    texto = (
        "📖 Comandos disponibles\n\n"
        "/gasto 350 concepto_del_gasto — Registra un gasto\n"
        "/gasto 350 concepto_del_gasto - Categoria — Con categoría opcional\n"
        "/status — Resumen: gastado, disponible y últimos 5 gastos\n"
        "/update_presupuesto 14000 — Cambia el presupuesto del ciclo\n"
        "/reset — Reinicia el ciclo (pide confirmación)\n"
        "/info — Muestra esta ayuda\n"
    )
    await update.message.reply_text(texto)


async def cmd_reset(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global _pending_reset
    if not _authorized(update):
        return

    state = _load()
    n_gastos = len(state["gastos"])
    total    = _total_gastado(state)
    _pending_reset = True

    await update.message.reply_text(
        f"⚠️ Vas a borrar {n_gastos} gastos (${total:,.2f} trackeados).\n"
        f"¿Confirmas el reset? Responde y o n"
    )


async def handle_confirmacion(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global _pending_reset
    if not _authorized(update):
        return
    if not _pending_reset:
        return

    respuesta = update.message.text.strip().lower()
    if respuesta == "y":
        state = _load()
        presupuesto_actual = state["presupuesto"]
        _save({
            "presupuesto":  presupuesto_actual,
            "gastos":       [],
            "ciclo_inicio": datetime.now().isoformat(timespec="seconds"),
        })
        _pending_reset = False
        await update.message.reply_text(
            f"🔄 Ciclo reiniciado. Presupuesto: ${presupuesto_actual:,.2f}"
        )
    elif respuesta == "n":
        _pending_reset = False
        await update.message.reply_text("Cancelado. El ciclo sigue igual.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if load_dotenv is not None:
        load_dotenv()

    token = os.getenv("TELEGRAM_TOKEN")
    if not token:
        raise ValueError(
            "Define la variable de entorno TELEGRAM_TOKEN con el token de tu bot."
        )

    app = ApplicationBuilder().token(token).build()
    app.add_handler(CommandHandler("gasto",               cmd_gasto))
    app.add_handler(CommandHandler("status",              cmd_status))
    app.add_handler(CommandHandler("update_presupuesto",  cmd_update_presupuesto))
    app.add_handler(CommandHandler("updatepresupuesto",   cmd_update_presupuesto))
    app.add_handler(CommandHandler("reset",               cmd_reset))
    app.add_handler(CommandHandler("info",                cmd_info))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_confirmacion))

    logging.info("Bot corriendo. Ctrl+C para detener.")
    app.run_polling()


if __name__ == "__main__":
    main()
