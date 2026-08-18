"""Conteo de producción a partir del contador del PLC."""

from typing import Tuple


def calcular_incremento(contador_previo: int, contador_actual: int) -> Tuple[int, bool]:
    """
    Golpes producidos entre dos lecturas del contador del PLC.

    Devuelve (incremento, hubo_reset).

    El contador del PLC se reinicia cuando el operador termina una corrida y
    arranca otra con la misma parte. Si el contador bajó, lo producido antes del
    reinicio ya está acumulado en la base de datos, así que el valor nuevo son
    golpes de la corrida nueva y se cuentan completos.

    >>> calcular_incremento(20, 23)
    (3, False)
    >>> calcular_incremento(64, 3)
    (3, True)
    """
    if contador_actual >= contador_previo:
        return contador_actual - contador_previo, False
    return contador_actual, True


def calcular_delta_turno(contador_previo: int, contador_actual: int) -> Tuple[int, bool]:
    """
    Avance de este lado desde el cierre del turno anterior.

    Devuelve (delta, fue_negativo).

    Si el PLC reinició su contador justo en la frontera del turno, la resta daría
    negativo: se fuerza a 0 para no escribir producción negativa. Se pierden los
    golpes de ese instante, que es preferible a corromper el registro.

    >>> calcular_delta_turno(500, 502)
    (2, False)
    >>> calcular_delta_turno(500, 3)
    (0, True)
    """
    delta = contador_actual - contador_previo
    if delta < 0:
        return 0, True
    return delta, False


def piezas_producidas(conteo_plc: int, multiplicador: int) -> int:
    """
    Piezas que representan un incremento del contador del PLC.

    Lo que cuenta el PLC NO es lo mismo en todas las áreas:

        Estampado    contador = GOLPES del troquel
                     histories            <- golpes (crudo)
                     production_records   <- golpes x multiplicador = piezas

        Las demás    contador = PIEZAS
                     histories            <- piezas
                     production_records   <- piezas (multiplicador = 1)

    Por eso el multiplicador solo aplica en Estampado: es la conversión de
    golpes a piezas. Ver areas/ (usa_multiplicador).

    >>> piezas_producidas(3, 4)     # estampado: 3 golpes, 4 piezas por golpe
    12
    >>> piezas_producidas(3, 1)     # carrocería: 3 piezas son 3 piezas
    3
    """
    return conteo_plc * (multiplicador or 1)
