## Why

`/status` calcula el ciclo, el día del ciclo, el ritmo esperado y la proyección con la fecha de hoy (`datetime.now()`). Cuando el registro de gastos va atrasado, el bot se "adelanta" al usuario. Ejemplo real: el 20/oct, con el último gasto registrado el 03/oct (ciclo 10/sep → 09/oct todavía abierto), `/status` reportó el ciclo 10/oct → 09/nov, "Día 11/31", una proyección de $84,562.84 y una alerta 🚨. Todo eso es falso. El tracker es un registro manual y diferido, así que la referencia temporal debe ser el avance real del registro, no el reloj.

## What Changes

- `/status` toma como **fecha de referencia** la fecha del gasto más reciente del tracker (la fecha del gasto, no la fecha en que se capturó). Esa fecha nunca puede ser posterior a hoy.
- El ciclo mostrado (inicio → fin), el "Día X/Y", el esperado a la fecha, la diferencia, los ritmos diario y semanal reales, la proyección y la alerta se calculan con esa fecha de referencia.
- Si el tracker no tiene gastos (por ejemplo, justo después de `/reset`), se sigue usando la fecha de hoy.
- El mensaje de `/status` indica explícitamente la fecha de corte usada (p. ej. "Al último registro: 03/oct"), para que el usuario sepa a qué fecha corresponden las métricas.
- Con los datos del ejemplo, `/status` pasaría a mostrar el ciclo 10/sep → 09/oct, "Día 24/30", un esperado de ~$17,595.20 y una proyección de ~$37,507.71.

Fuera de alcance (no cambia):
- El mensaje de confirmación de `/gasto` (sigue calculando contra el presupuesto total).
- La validación de fechas de `/gasto`, que rechaza fechas posteriores al ciclo actual según la fecha de hoy.
- El cálculo de MSI apartados (sigue usando el archivo MSI más reciente) y el de recurrentes.
- El dashboard de Streamlit.

## Capabilities

### New Capabilities
- `tracker-status`: el resumen `/status` del bot de Telegram. Cubre la fecha de referencia, el ciclo y el ritmo de gasto del ciclo del tracker.

### Modified Capabilities
<!-- No hay specs previas en openspec/specs/ (proyecto brownfield recién inicializado en OpenSpec). -->

## Impact

- Código: `src/tracker/bot.py` (`cmd_status`, `_cycle_progress` y un helper nuevo para obtener la fecha de referencia).
- No hay cambios de datos ni de formato en `track_ciclo.json`, ni dependencias nuevas.
- Comportamiento visible: cambia el texto y las cifras de `/status` cuando el registro va atrasado. Si el registro está al día (último gasto = hoy), el resultado es idéntico al actual, salvo la línea nueva con la fecha de corte.
