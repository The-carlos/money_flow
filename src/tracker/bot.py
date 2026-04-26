"""
Bot de Telegram para trackear gastos del ciclo de TDC.

Comandos:
  /gasto                   → inicia el flujo guiado de registro
  /status                  → resumen del ciclo actual
  /update-presupuesto 14000 → cambia el presupuesto disponible
  /reset                   → cierra y archiva el ciclo actual
  /cancelar                → cancela el registro guiado actual

Configuración (variables de entorno o archivo .env):
  TELEGRAM_TOKEN   → token del bot (obtenido de @BotFather)
  TELEGRAM_CHAT_ID → tu chat ID personal (obtenido de @userinfobot)
"""

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from telegram import ReplyKeyboardMarkup, ReplyKeyboardRemove, Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - dependencia opcional
    load_dotenv = None

# ---------------------------------------------------------------------------
# Rutas
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from tracker.categories import backfill_tracker_categories, classify_tracker_expense
from tracker.storage import archive_cycle, load_json, save_json

TRACK_PATH   = PROJECT_ROOT / "data" / "processed" / "track_ciclo.json"
TRACK_HISTORY_DIR = PROJECT_ROOT / "data" / "processed" / "tracker_cycles"

PRESUPUESTO_DEFAULT = 13168.0
RESET_CONFIRM_WINDOW = timedelta(minutes=2)
CATEGORY_AUTO = "Auto clasificar"
CATEGORY_NONE = "Sin categoría"
DATE_TODAY = "Hoy"
DATE_YESTERDAY = "Ayer"
DATE_OTHER = "Elegir otra fecha"
CONFIRM_SAVE = "Confirmar"
CONFIRM_CANCEL = "Cancelar"
GASTO_FLOW_WINDOW = timedelta(minutes=10)
FREQUENT_CATEGORIES = [
    "Alimentación",
    "Transporte",
    "Supermercado y Farmacia",
    "Entretenimiento",
    "Servicios del Hogar y Telecomunicaciones",
]

# Estado en memoria para confirmación de reset
_pending_reset: dict | None = None
_pending_expense: dict[str, dict] = {}

logging.basicConfig(
    format="%(asctime)s — %(levelname)s — %(message)s",
    level=logging.INFO,
)

# ---------------------------------------------------------------------------
# Estado del ciclo
# ---------------------------------------------------------------------------

def _load() -> dict:
    state = load_json(TRACK_PATH, {
        "presupuesto": PRESUPUESTO_DEFAULT,
        "gastos": [],
        "ciclo_inicio": datetime.now().isoformat(timespec="seconds"),
    })

    try:
        state, changed, updated = backfill_tracker_categories(state)
    except RuntimeError as exc:
        logging.warning("Tracker: no se pudo recategorizar con OpenAI: %s", exc)
        return state
    if changed:
        _save(state)
        if updated:
            logging.info("Tracker: %s gasto(s) existentes recategorizados con OpenAI.", updated)
    return state


def _save(state: dict) -> None:
    save_json(TRACK_PATH, state)


def _total_gastado(state: dict) -> float:
    return sum(g["monto"] for g in state["gastos"])


def _barra(gastado: float, presupuesto: float, width: int = 10) -> str:
    pct = min(gastado / presupuesto, 1.0) if presupuesto else 0
    filled = round(pct * width)
    bar = "█" * filled + "░" * (width - filled)
    return f"[{bar}] {pct*100:.1f}%"


def _new_cycle(presupuesto: float) -> dict:
    return {
        "presupuesto": presupuesto,
        "gastos": [],
        "ciclo_inicio": datetime.now().isoformat(timespec="seconds"),
    }


def _date_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [[DATE_TODAY, DATE_YESTERDAY], [DATE_OTHER], [CONFIRM_CANCEL]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def _category_keyboard() -> ReplyKeyboardMarkup:
    rows = [
        [CATEGORY_AUTO, CATEGORY_NONE],
        FREQUENT_CATEGORIES[:2],
        FREQUENT_CATEGORIES[2:4],
        [FREQUENT_CATEGORIES[4], CONFIRM_CANCEL],
    ]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True, one_time_keyboard=True)


def _confirm_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [[CONFIRM_SAVE, CONFIRM_CANCEL]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def _reset_expense_flow(chat_id: str) -> None:
    _pending_expense.pop(chat_id, None)


def _expense_flow_active(chat_id: str) -> bool:
    flow = _pending_expense.get(chat_id)
    if not flow:
        return False
    requested_at = flow.get("requested_at")
    if not requested_at:
        _reset_expense_flow(chat_id)
        return False
    if datetime.now() - datetime.fromisoformat(requested_at) > GASTO_FLOW_WINDOW:
        _reset_expense_flow(chat_id)
        return False
    return True


def _new_expense_flow(chat_id: str) -> None:
    _pending_expense[chat_id] = {
        "step": "amount",
        "requested_at": datetime.now().isoformat(timespec="seconds"),
        "draft": {},
    }


def _format_tracker_date(value: datetime) -> str:
    return value.strftime("%Y-%m-%d")


def _parse_manual_date(raw: str) -> datetime | None:
    raw = raw.strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


def _expense_summary(draft: dict) -> str:
    fecha = draft.get("fecha", "")
    categoria = draft.get("categoria", "No identificado")
    return (
        "Confirma este gasto:\n\n"
        f"Monto: ${draft['monto']:,.2f}\n"
        f"Descripción: {draft['descripcion']}\n"
        f"Fecha: {fecha}\n"
        f"Categoría: {categoria}"
    )


def _store_tracker_expense(draft: dict) -> dict:
    state = _load()
    state["gastos"].append({
        "fecha": f"{draft['fecha']}T12:00:00",
        "monto": draft["monto"],
        "descripcion": draft["descripcion"],
        "categoria": draft["categoria"],
        "tipo": "tracker",
        "categoria_contexto": "",
    })
    try:
        state, _, updated = backfill_tracker_categories(state)
    except RuntimeError as exc:
        logging.warning("Tracker: no se pudo categorizar el gasto con OpenAI: %s", exc)
        updated = 0
    if updated:
        logging.info("Tracker: %s gasto(s) recategorizados tras guardar el gasto.", updated)
    _save(state)
    return state["gastos"][-1]


def _pending_reset_active(update: Update) -> bool:
    global _pending_reset
    if not _pending_reset:
        return False
    if _pending_reset.get("chat_id") != str(update.effective_chat.id):
        return False
    requested_at = _pending_reset.get("requested_at")
    if not requested_at:
        _pending_reset = None
        return False
    if datetime.now() - datetime.fromisoformat(requested_at) > RESET_CONFIRM_WINDOW:
        _pending_reset = None
        return False
    return True


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
    chat_id = str(update.effective_chat.id)
    _new_expense_flow(chat_id)
    await update.message.reply_text(
        "Vamos a registrar un gasto nuevo.\n\nPaso 1/5: escribe el monto.",
        reply_markup=ReplyKeyboardMarkup([[CONFIRM_CANCEL]], resize_keyboard=True, one_time_keyboard=True),
    )


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
        "/gasto — Inicia el flujo guiado para registrar un gasto\n"
        "/status — Resumen: gastado, disponible y últimos 5 gastos\n"
        "/update_presupuesto 14000 — Cambia el presupuesto del ciclo\n"
        "/reset — Cierra y archiva el ciclo actual (pide confirmación)\n"
        "/cancelar — Cancela el registro guiado de un gasto\n"
        "/info — Muestra esta ayuda\n"
    )
    await update.message.reply_text(texto)


async def cmd_cancelar(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update):
        return
    chat_id = str(update.effective_chat.id)
    if not _expense_flow_active(chat_id):
        await update.message.reply_text("No hay un registro de gasto en curso.", reply_markup=ReplyKeyboardRemove())
        return
    _reset_expense_flow(chat_id)
    await update.message.reply_text("Registro cancelado.", reply_markup=ReplyKeyboardRemove())


async def cmd_reset(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global _pending_reset
    if not _authorized(update):
        return

    state = _load()
    n_gastos = len(state["gastos"])
    total    = _total_gastado(state)
    if state["gastos"]:
        fechas = sorted(g["fecha"] for g in state["gastos"])
        ciclo_ref = f"{fechas[0][:10]} → {fechas[-1][:10]}"
    else:
        ciclo_ref = "sin gastos"
    _pending_reset = {
        "chat_id": str(update.effective_chat.id),
        "requested_at": datetime.now().isoformat(timespec="seconds"),
    }

    await update.message.reply_text(
        f"⚠️ Vas a cerrar el ciclo actual ({ciclo_ref}) con {n_gastos} gastos "
        f"y ${total:,.2f} trackeados.\n"
        f"El historial se archivará y se abrirá un ciclo nuevo conservando el presupuesto actual.\n"
        f"¿Confirmas el reset? Responde y o n"
    )


async def handle_confirmacion(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global _pending_reset
    if not _authorized(update):
        return
    if not _pending_reset_active(update):
        return

    respuesta = update.message.text.strip().lower()
    if respuesta == "y":
        state = _load()
        presupuesto_actual = state["presupuesto"]
        archive = archive_cycle(state, TRACK_HISTORY_DIR)
        _save(_new_cycle(presupuesto_actual))
        _pending_reset = None
        if archive:
            await update.message.reply_text(
                f"🔄 Ciclo archivado como *{archive['label']}*.\n"
                f"Presupuesto conservado: ${presupuesto_actual:,.2f}",
                parse_mode="Markdown",
            )
        else:
            await update.message.reply_text(
                f"🔄 No había gastos por archivar. Ciclo reiniciado con presupuesto "
                f"${presupuesto_actual:,.2f}"
            )
    elif respuesta == "n":
        _pending_reset = None
        await update.message.reply_text("Cancelado. El ciclo sigue igual.")


async def handle_expense_flow(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update):
        return

    chat_id = str(update.effective_chat.id)
    if not _expense_flow_active(chat_id):
        return

    text = (update.message.text or "").strip()
    if text == CONFIRM_CANCEL:
        _reset_expense_flow(chat_id)
        await update.message.reply_text("Registro cancelado.", reply_markup=ReplyKeyboardRemove())
        return

    flow = _pending_expense[chat_id]
    flow["requested_at"] = datetime.now().isoformat(timespec="seconds")
    draft = flow["draft"]
    step = flow["step"]

    if step == "amount":
        try:
            draft["monto"] = float(text.replace(",", ""))
        except ValueError:
            await update.message.reply_text("Monto inválido. Escribe un número, por ejemplo: 350.50")
            return
        flow["step"] = "description"
        await update.message.reply_text("Paso 2/5: escribe la descripción del gasto.")
        return

    if step == "description":
        if not text:
            await update.message.reply_text("La descripción no puede ir vacía.")
            return
        draft["descripcion"] = text.replace("_", " ").strip()
        flow["step"] = "date"
        await update.message.reply_text(
            "Paso 3/5: elige la fecha del gasto.",
            reply_markup=_date_keyboard(),
        )
        return

    if step == "date":
        if text == DATE_TODAY:
            draft["fecha"] = _format_tracker_date(datetime.now())
        elif text == DATE_YESTERDAY:
            draft["fecha"] = _format_tracker_date(datetime.now() - timedelta(days=1))
        elif text == DATE_OTHER:
            flow["step"] = "date_manual"
            await update.message.reply_text(
                "Escribe la fecha manualmente en formato YYYY-MM-DD o DD/MM/YYYY.",
                reply_markup=ReplyKeyboardMarkup([[CONFIRM_CANCEL]], resize_keyboard=True, one_time_keyboard=True),
            )
            return
        else:
            await update.message.reply_text(
                "Elige una opción válida para la fecha.",
                reply_markup=_date_keyboard(),
            )
            return
        flow["step"] = "category"
        await update.message.reply_text(
            "Paso 4/5: elige una categoría o deja que el bot la clasifique.",
            reply_markup=_category_keyboard(),
        )
        return

    if step == "date_manual":
        parsed = _parse_manual_date(text)
        if not parsed:
            await update.message.reply_text("Fecha inválida. Usa YYYY-MM-DD o DD/MM/YYYY.")
            return
        draft["fecha"] = _format_tracker_date(parsed)
        flow["step"] = "category"
        await update.message.reply_text(
            "Paso 4/5: elige una categoría o deja que el bot la clasifique.",
            reply_markup=_category_keyboard(),
        )
        return

    if step == "category":
        if text == CATEGORY_AUTO:
            try:
                draft["categoria"] = classify_tracker_expense(
                    descripcion=draft["descripcion"],
                    monto=draft["monto"],
                )
            except RuntimeError as exc:
                logging.warning("Tracker: no se pudo auto clasificar el gasto: %s", exc)
                draft["categoria"] = "No identificado"
        elif text == CATEGORY_NONE:
            draft["categoria"] = "No identificado"
        elif text in FREQUENT_CATEGORIES:
            draft["categoria"] = text
        else:
            await update.message.reply_text(
                "Elige una opción válida para la categoría.",
                reply_markup=_category_keyboard(),
            )
            return
        flow["step"] = "confirm"
        await update.message.reply_text(
            _expense_summary(draft),
            reply_markup=_confirm_keyboard(),
        )
        return

    if step == "confirm":
        if text != CONFIRM_SAVE:
            await update.message.reply_text(
                "Usa Confirmar o Cancelar para terminar este registro.",
                reply_markup=_confirm_keyboard(),
            )
            return

        gasto_actual = _store_tracker_expense(draft)
        _reset_expense_flow(chat_id)

        state = _load()
        categoria = gasto_actual.get("categoria", "No identificado")
        gastado = _total_gastado(state)
        presupuesto = state["presupuesto"]
        restante = presupuesto - gastado
        barra = _barra(gastado, presupuesto)
        uso_pct = (gastado / presupuesto) if presupuesto > 0 else 0
        alerta = "\n⚠️ Mas del 80% usado — considera frenar." if uso_pct >= 0.8 else ""
        excedido = "\n🚨 PRESUPUESTO EXCEDIDO" if gastado > presupuesto else ""

        texto = (
            f"✅ ${draft['monto']:,.2f} registrado — {draft['descripcion']} [{categoria}]\n"
            f"Fecha: {draft['fecha']}\n\n"
            f"{barra}\n"
            f"Gastado:     ${gastado:,.2f}\n"
            f"Presupuesto: ${presupuesto:,.2f}\n"
            f"Restante:    ${restante:,.2f}"
            f"{alerta}{excedido}"
        )
        await update.message.reply_text(texto, reply_markup=ReplyKeyboardRemove())
        return


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
    app.add_handler(CommandHandler("cancelar",            cmd_cancelar))
    app.add_handler(CommandHandler("status",              cmd_status))
    app.add_handler(CommandHandler("update_presupuesto",  cmd_update_presupuesto))
    app.add_handler(CommandHandler("updatepresupuesto",   cmd_update_presupuesto))
    app.add_handler(CommandHandler("reset",               cmd_reset))
    app.add_handler(CommandHandler("info",                cmd_info))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_expense_flow))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_confirmacion))

    logging.info("Bot corriendo. Ctrl+C para detener.")
    app.run_polling()


if __name__ == "__main__":
    main()
