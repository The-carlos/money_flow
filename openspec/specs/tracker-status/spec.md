# tracker-status Specification

## Purpose
Define cómo el comando `/status` del bot de Telegram resume el ciclo del tracker de TDC: ciclo vigente, avance, ritmo de gasto y proyección, tomando como referencia el avance real del registro manual de gastos.

## Requirements

### Requirement: Fecha de referencia del status
El sistema SHALL calcular las métricas temporales de `/status` a una fecha de referencia. La fecha de referencia MUST ser la fecha (campo `fecha` del gasto, no el momento de captura) del gasto más reciente del tracker, sin importar el orden en que se registraron los gastos. Si esa fecha es posterior a hoy, la fecha de referencia MUST ser hoy. Si el tracker no tiene gastos, la fecha de referencia MUST ser hoy.

#### Scenario: Registro atrasado respecto a hoy
- **WHEN** hoy es 2026-10-20 y el gasto más reciente del tracker tiene fecha 2026-10-03
- **THEN** la fecha de referencia de `/status` es 2026-10-03

#### Scenario: Gastos registrados fuera de orden
- **WHEN** el usuario registra primero un gasto con fecha 2026-10-03 y después otro con fecha 2026-09-25
- **THEN** la fecha de referencia de `/status` sigue siendo 2026-10-03

#### Scenario: Registro al día
- **WHEN** el gasto más reciente del tracker tiene la fecha de hoy
- **THEN** la fecha de referencia es hoy y las métricas coinciden con las que se obtendrían usando la fecha actual

#### Scenario: Gasto con fecha futura
- **WHEN** hoy es 2026-10-20 y el gasto más reciente del tracker tiene fecha 2026-10-25
- **THEN** la fecha de referencia es 2026-10-20

#### Scenario: Tracker sin gastos
- **WHEN** el tracker no tiene gastos registrados (por ejemplo, justo después de `/reset`)
- **THEN** la fecha de referencia es hoy

### Requirement: Ciclo y avance según la fecha de referencia
El sistema SHALL determinar el ciclo mostrado en `/status` (del día 10 de un mes al día 9 del mes siguiente) a partir de la fecha de referencia, y SHALL reportar el día del ciclo como la posición de la fecha de referencia dentro de ese ciclo, junto con el total de días del ciclo.

#### Scenario: Ciclo anterior aún en registro
- **WHEN** la fecha de referencia es 2026-10-03
- **THEN** `/status` muestra el ciclo "10/sep → 09/oct" y "Día 24/30"

#### Scenario: Fecha de referencia en día de corte
- **WHEN** la fecha de referencia es 2026-10-10
- **THEN** `/status` muestra el ciclo "10/oct → 09/nov" y "Día 1/31"

### Requirement: Ritmo y proyección según la fecha de referencia
El sistema SHALL calcular el gasto esperado a la fecha, la diferencia contra el gasto real, el ritmo diario y semanal reales, la proyección de cierre y la alerta de ritmo usando el día del ciclo de la fecha de referencia. El presupuesto diario y semanal MUST seguir derivándose del presupuesto libre dividido entre los días totales del ciclo de referencia.

#### Scenario: Métricas con registro atrasado
- **WHEN** la fecha de referencia es 2026-10-03, el presupuesto libre es $21,994.00 y el gasto variable registrado es $30,006.17
- **THEN** `/status` reporta un presupuesto diario de ~$733.13, un esperado a la fecha de ~$17,595.20, un ritmo diario real de ~$1,250.26 y una proyección de ~$37,507.71

#### Scenario: Alerta coherente con la fecha de referencia
- **WHEN** la proyección calculada a la fecha de referencia supera el presupuesto libre en más de 5%
- **THEN** la alerta de `/status` reporta esa proyección, no una calculada con la fecha de hoy

### Requirement: Fecha de corte visible en el status
El sistema SHALL mostrar en el mensaje de `/status` la fecha de referencia usada para los cálculos, e indicar si proviene del último gasto registrado o de la fecha de hoy.

#### Scenario: Status con gastos registrados
- **WHEN** la fecha de referencia proviene del gasto más reciente con fecha 2026-10-03
- **THEN** el mensaje incluye una línea que indica que las métricas están calculadas al último registro, 03/oct

#### Scenario: Status sin gastos
- **WHEN** el tracker no tiene gastos
- **THEN** el mensaje indica que las métricas están calculadas a la fecha de hoy
