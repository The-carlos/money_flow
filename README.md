# MONEY_FLOW

MONEY_FLOW es una herramienta personal para consolidar, clasificar y visualizar movimientos financieros a partir de estados de cuenta de BBVA México.

El proyecto está orientado a un flujo local y privado:

- extrae movimientos desde PDFs de débito y tarjeta de crédito,
- genera datasets consolidados para análisis,
- estima métricas relevantes de deuda y meses sin intereses,
- clasifica gastos automáticamente,
- expone un dashboard en Streamlit,
- y añade un bot de Telegram para seguimiento manual del ciclo de la tarjeta.

## Objetivo

El objetivo de MONEY_FLOW es convertir estados de cuenta bancarios en una vista operativa del flujo de dinero personal:

- ingresos y egresos por periodo,
- saldo disponible en débito,
- deuda regular de tarjeta de crédito,
- compras a meses sin intereses,
- distribución por categoría,
- y control manual del presupuesto del ciclo de la TDC.

No es un sistema contable general ni una plataforma multiusuario. Está diseñado para uso individual y para el formato actual de estados de cuenta BBVA México.

## Características principales

- Extracción automática de movimientos desde PDFs bancarios.
- Soporte para estados de cuenta de débito y tarjeta de crédito.
- Consolidación en un CSV único para análisis posterior.
- Cálculo de saldo acumulado por movimiento.
- Extracción de métricas de crédito como pago mínimo, pago para no generar intereses, límite y crédito disponible.
- Separación de planes de MSI por periodo.
- Categorización automática por reglas y por modelo GPT vía OpenAI API.
- Dashboard local con Streamlit para consultar métricas y movimientos.
- Bot de Telegram para registrar gastos manuales del ciclo de tarjeta.

## Arquitectura

La estructura principal del proyecto es:

```text
src/
  categorizer/
    apply_categories.py
    categorize.py
    rules.py
  dashboard/
    app.py
  extractor/
    consolidate.py
    credit_parser.py
    pdf_parser.py
    pipeline.py
  tracker/
    bot.py

data/
  raw/
  processed/
```

### 1. Extracción

Los parsers viven en:

- `src/extractor/pdf_parser.py`: parsea estados de cuenta de débito.
- `src/extractor/credit_parser.py`: parsea estados de cuenta de TDC y extrae también planes MSI.

Ambos parsers usan `pdfplumber` y están calibrados con coordenadas del layout actual del PDF. Si BBVA cambia el formato del estado de cuenta, es probable que haya que ajustar límites de columnas o expresiones de extracción.

### 2. Pipeline principal

El flujo principal está en `src/extractor/pipeline.py`.

Ese script:

1. Detecta PDFs nuevos en `data/raw/`.
2. Identifica si cada archivo es de débito o crédito.
3. Ejecuta el parser correspondiente.
4. Calcula saldo acumulado por movimiento.
5. Asigna categoría automática inicial por reglas.
6. Genera artefactos procesados en `data/processed/`.
7. Mantiene un `manifest.json` con huella del archivo para evitar reprocesos accidentales.

### 3. Categorización

Hay dos mecanismos:

- `src/categorizer/rules.py`: reglas determinísticas para categorías frecuentes.
- `src/categorizer/categorize.py`: clasificación con OpenAI API usando descripción y referencia del movimiento.

Si el texto del movimiento no es suficientemente descriptivo, la categoría por default es `No identificado`.

También existe `src/categorizer/apply_categories.py`, que permite aplicar reglas locales y overrides manuales por firma de movimiento, sin depender del orden del CSV.

### 4. Dashboard

El dashboard está en `src/dashboard/app.py` y se ejecuta con Streamlit.

Incluye vistas para:

- resumen financiero,
- movimientos consolidados,
- análisis de débito,
- análisis de crédito,
- planes MSI,
- y tracker del ciclo de la tarjeta.

### 5. Tracker de Telegram

El bot de `src/tracker/bot.py` sirve para registrar gastos manuales del ciclo de la TDC.

Comandos principales:

- `/gasto 350 Uber`
- `/status`
- `/update_presupuesto 14000`
- `/reset`

## Requisitos

- Python 3.9 o superior
- Entorno local con acceso a tus estados de cuenta PDF
- Credenciales de OpenAI si vas a usar clasificación por GPT
- Token de Telegram si vas a usar el bot

Instalación:

```bash
pip install -r requirements.txt
```

## Configuración

Define tus variables en un archivo `.env` local.

Ejemplo:

```env
OPENAI_API_KEY=sk-...
OPENAI_MODEL=gpt-5-mini
OPENAI_API_BASE=https://api.openai.com/v1
OPENAI_CATEGORIZE_BATCH=50

TELEGRAM_TOKEN=
TELEGRAM_CHAT_ID=
```

Variables:

- `OPENAI_API_KEY`: clave de API para clasificación con GPT.
- `OPENAI_MODEL`: modelo a usar para clasificación.
- `OPENAI_API_BASE`: útil si quieres enrutar a otra base compatible.
- `OPENAI_CATEGORIZE_BATCH`: tamaño de lote por request.
- `TELEGRAM_TOKEN`: token del bot.
- `TELEGRAM_CHAT_ID`: restringe el bot a un chat específico.

## Flujo de uso recomendado

### 1. Colocar estados de cuenta

Copia tus PDFs a:

```text
data/raw/
```

### 2. Procesar PDFs

```bash
python3 src/extractor/pipeline.py
```

Para reprocesar todo desde cero:

```bash
python3 src/extractor/pipeline.py --all
```

### 3. Re-categorizar con GPT

```bash
python3 src/categorizer/categorize.py
```

### 4. Aplicar reglas u overrides manuales

Si quieres forzar categorías manuales por firma de movimiento, crea:

```text
data/processed/manual_categories.json
```

Y luego ejecuta:

```bash
python3 src/categorizer/apply_categories.py
```

### 5. Abrir el dashboard

```bash
streamlit run src/dashboard/app.py
```

### 6. Ejecutar el bot de Telegram

```bash
python3 src/tracker/bot.py
```

## Artefactos generados

El proyecto genera archivos derivados en `data/processed/`.

Los más importantes son:

- `movimientos_consolidados.csv`: dataset principal con todos los movimientos.
- `msi_activos_YYYY-MM.csv`: planes MSI detectados por periodo.
- `metricas_credito_YYYY-MM.json`: métricas del estado de cuenta de TDC.
- `manifest.json`: control de PDFs procesados con metadatos y huella del archivo.
- `track_ciclo.json`: estado persistente del tracker del bot.

## Seguridad y privacidad

Este repositorio está preparado para ser público, pero los datos financieros no deben subirse.

Por eso, `.gitignore` excluye:

- `data/raw/`
- `data/processed/`
- `notebooks/`
- `.env`
- `.venv/`

Esto evita publicar:

- estados de cuenta PDF,
- movimientos procesados,
- métricas financieras,
- análisis exploratorios,
- y credenciales.

## Limitaciones conocidas

- El parser depende del formato actual de BBVA México.
- No hay suite de tests automatizados en este momento.
- No está pensado para múltiples bancos ni múltiples usuarios.
- La calidad de clasificación depende de la calidad del texto extraído del PDF.
- Algunos comercios ambiguos seguirán cayendo en `No identificado` si no hay contexto suficiente.

## Estado del proyecto

MONEY_FLOW está en una etapa funcional y utilitaria. Ya resuelve el flujo principal de extracción, clasificación y visualización, pero sigue siendo un proyecto personal en evolución.

Las áreas naturales de mejora son:

- ampliar cobertura de parsers,
- endurecer aún más validaciones de datos,
- mejorar trazabilidad de categorías manuales,
- refinar la taxonomía de categorías,
- y añadir pruebas automatizadas cuando el flujo operativo esté más estable.

## Licencia

Pendiente de definir.
