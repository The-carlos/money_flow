## 1. Fecha de referencia

- [x] 1.1 Agregar en `src/tracker/bot.py` el helper puro `_status_reference_date(gastos, today) -> tuple[datetime, bool]`: máximo de `fecha` válidas acotado a `today`, `(today, False)` si no hay gastos válidos. Verificar con `python3 -c` desde `src/`, stubeando `telegram`, `telegram.ext`, `pandas` y `dotenv` con `unittest.mock` (el `.venv` local tiene `numpy`/`cryptography` rotos), los escenarios de la spec: atrasado (10-03 con hoy 10-20 → 10-03, True), fuera de orden, futura (10-25 → 10-20), vacío (→ hoy, False) y fecha corrupta ignorada.

## 2. Status

- [x] 2.1 En `cmd_status`, reemplazar `datetime.now()` por la fecha de referencia al llamar a `_cycle_progress`. Verificar con `python3 -c` que `_cycle_progress(datetime(2026,10,3), 21994.0, 30006.17)` devuelve ciclo 10/sep → 09/oct, `current_day=24`, `total_days=30`, `expected_today≈17595.20` y `projected≈37507.71`, y que con `datetime(2026,10,10)` devuelve `Día 1/31`.
- [x] 2.2 Agregar al texto de `/status`, debajo de la línea del ciclo, `Al último registro: DD/mmm` o `A hoy: DD/mmm` según el origen de la fecha, y renombrar "Esperado/Real a hoy" a "Esperado/Real al corte". Verificar armando el texto con un `track_ciclo.json` temporal (o un `state` en memoria) y revisando la salida.

## 3. Documentación y verificación end-to-end

- [x] 3.1 Actualizar el docstring de `bot.py`, el texto de `/info` y la sección del tracker en `README.md` para explicar que `/status` se calcula al último gasto registrado. Verificar leyendo el diff.
- [ ] 3.2 Probar end-to-end: reiniciar el bot, ejecutar `/status` en Telegram con el ciclo actual atrasado y confirmar que muestra "10/sep → 09/oct · Día 24/30", la línea "Al último registro: 03/oct" y una proyección consistente con la spec (paso manual del usuario, en la máquina donde corre el bot).
