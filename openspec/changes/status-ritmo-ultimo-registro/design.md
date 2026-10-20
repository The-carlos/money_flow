## Context

En `src/tracker/bot.py`, `cmd_status` llama a `_cycle_progress(datetime.now(), presupuesto_libre, gastado)`. A su vez, `_cycle_progress` llama a `_cycle_bounds(today)` y calcula `current_day` con `today`. Todo el bloque temporal (ciclo, día, esperado, ritmo, proyección y alerta en `_status_alert`) depende de ese único parámetro `today`. Los gastos viven en `track_ciclo.json` como lista `gastos` con `fecha` ISO (`YYYY-MM-DDT12:00:00`) y se agregan en orden de captura, no de fecha.

El proyecto no tiene suite de tests ni dependencia de testing.

## Goals / Non-Goals

**Goals:**
- Cambiar únicamente el argumento temporal que recibe `_cycle_progress` en `/status`, sin reescribir las fórmulas.
- Mantener funciones puras y fáciles de probar para la fecha de referencia.

**Non-Goals:**
- Unificar las tres implementaciones duplicadas de `cycle_bounds` (`bot.py`, `recurrentes.py`, `repair_dates.py`).
- Agregar un framework de tests al proyecto.

## Decisions

1. **Fecha de referencia = `max(fecha)` de los gastos, acotada a hoy.** Se descarta "último gasto agregado" (`gastos[-1]`), porque el usuario captura gastos atrasados fuera de orden y un gasto viejo capturado al final retrocedería el status. Se acota con `min(max_fecha, hoy)` porque `/gasto` permite fechas manuales futuras dentro del ciclo actual, y proyectar desde el futuro no tiene sentido.
2. **Helper puro `_status_reference_date(gastos, today) -> tuple[datetime, bool]`** en `bot.py`. Devuelve la fecha de referencia y si proviene de un gasto. Las fechas que no se pueden parsear se ignoran (no rompen `/status`). Alternativa descartada: ponerlo en `storage.py`. Por ahora solo lo usa el bot y `storage.py` no maneja lógica de ciclos.
3. **Se reutiliza `_cycle_progress` sin cambios**, pasándole la fecha de referencia en vez de `datetime.now()`. Su parámetro ya se llama `today` y es genérico. Opcionalmente se renombra a `reference` por claridad, sin cambiar la lógica.
4. **Línea nueva en el mensaje**, debajo de la línea del ciclo: `Al último registro: 03/oct` o `A hoy: 20/oct`. Usa `_format_cycle_date`, que ya existe.
5. **Verificación sin framework**: un script de verificación ad hoc (o un doctest-style con `python3 -c`) que ejercite `_status_reference_date` y `_cycle_progress` con los escenarios de la spec. No se agrega `pytest` a `requirements.txt`.

## Risks / Trade-offs

- [Días reales sin gasto no cuentan hasta registrar algo] → Es el comportamiento elegido por el usuario. La línea "Al último registro" deja claro a qué fecha corresponden las métricas.
- [Si se registra un gasto del ciclo nuevo antes de `/reset`, la fecha de referencia salta al ciclo nuevo mientras `gastado` suma ambos ciclos] → Ese problema ya existe hoy (el tracker no filtra por ciclo). El flujo esperado sigue siendo terminar el ciclo, hacer `/reset` y registrar el siguiente. No se resuelve en este cambio.
- [Gastos con `fecha` corrupta] → Se ignoran al calcular el máximo. Si ninguna es válida, se usa hoy.

## Migration Plan

Sin migración de datos. Basta con reiniciar el proceso del bot (`python3 src/tracker/bot.py`). Rollback: revertir el commit.
