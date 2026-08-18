"""
Casos de conteo. Cada uno corresponde a una situación real de piso que
costó trabajo diagnosticar; quedan congelados aquí para que no vuelvan.
"""

import pytest

from domain.contadores import (
    calcular_incremento,
    calcular_delta_turno,
    piezas_producidas,
)


class TestIncremento:
    def test_avance_normal(self):
        assert calcular_incremento(20, 23) == (3, False)

    def test_sin_avance(self):
        """Contador detenido: la prensa no golpeó."""
        assert calcular_incremento(150, 150) == (0, False)

    def test_reset_a_cero(self):
        """Termina la corrida y el operador reinicia el contador."""
        assert calcular_incremento(64, 0) == (0, True)

    def test_reset_con_produccion(self):
        """
        Caso real de MT05: la BD tenía 64 y el PLC volvió a 3.
        El delta debe ser 3 (los golpes de la corrida nueva), NUNCA -61.
        """
        incremento, hubo_reset = calcular_incremento(64, 3)
        assert incremento == 3
        assert hubo_reset is True
        assert incremento >= 0

    def test_reset_desde_cero(self):
        assert calcular_incremento(0, 5) == (5, False)

    def test_salto_grande(self):
        """Si el servicio estuvo caído, el PLC siguió contando: se recupera todo."""
        assert calcular_incremento(30, 45) == (15, False)

    @pytest.mark.parametrize("prev,cnt", [(0, 0), (1, 0), (999, 1), (5, 5)])
    def test_nunca_negativo(self, prev, cnt):
        incremento, _ = calcular_incremento(prev, cnt)
        assert incremento >= 0


class TestDeltaTurno:
    def test_cambio_de_turno_normal(self):
        """Caso real de MK05/RH: cerró en 206, cruzó el turno en 207."""
        assert calcular_delta_turno(206, 207) == (1, False)

    def test_sin_avance_en_la_frontera(self):
        assert calcular_delta_turno(480, 480) == (0, False)

    def test_reset_justo_en_la_frontera(self):
        """
        El PLC reinició al cruzar el turno: la resta daría -497.
        Debe forzarse a 0, nunca escribir producción negativa.
        """
        delta, fue_negativo = calcular_delta_turno(500, 3)
        assert delta == 0
        assert fue_negativo is True

    @pytest.mark.parametrize("previo,actual", [(500, 0), (1, 0), (206, 205)])
    def test_nunca_negativo(self, previo, actual):
        delta, _ = calcular_delta_turno(previo, actual)
        assert delta >= 0


class TestPiezasProducidas:
    def test_multiplicador_uno(self):
        assert piezas_producidas(3, 1) == 3

    def test_multiplicador_mayor(self):
        """Un golpe del troquel produce varias piezas."""
        assert piezas_producidas(3, 4) == 12

    def test_multiplicador_nulo_se_trata_como_uno(self):
        """obtener_multiplicador_as400 puede devolver None si AS400 no responde."""
        assert piezas_producidas(7, None) == 7
        assert piezas_producidas(7, 0) == 7


class TestFlujoCompleto:
    """La secuencia real de MK05/RH del 2026-08-11, verificada contra la BD."""

    def test_turno1_luego_turno2(self):
        # Turno 1: el registro nace con el contador en 199
        acumulado_t1 = piezas_producidas(199, 1)

        # Avanza de 199 a 206 antes del cambio de turno
        inc, _ = calcular_incremento(199, 206)
        acumulado_t1 += piezas_producidas(inc, 1)
        assert acumulado_t1 == 206  # coincide con production_records id=232126

        # Cambio de turno con el contador en 207
        delta, _ = calcular_delta_turno(206, 207)
        acumulado_t2 = piezas_producidas(delta, 1)

        # Sigue produciendo hasta 226
        inc, _ = calcular_incremento(207, 226)
        acumulado_t2 += piezas_producidas(inc, 1)
        assert acumulado_t2 == 20  # coincide con production_records id=232132

        # Y el total del día cuadra con los golpes registrados en histories
        assert acumulado_t1 + acumulado_t2 == 226

    def test_corrida_nueva_tras_reset_suma_al_acumulado(self):
        """El bug original: la segunda corrida NO debe borrar la primera."""
        acumulado = 64  # lo que ya está en la base

        inc, hubo_reset = calcular_incremento(64, 0)   # reset
        acumulado += piezas_producidas(inc, 1)
        assert hubo_reset and acumulado == 64          # nada se pierde

        for prev, cnt in [(0, 1), (1, 2), (2, 20)]:
            inc, _ = calcular_incremento(prev, cnt)
            acumulado += piezas_producidas(inc, 1)

        assert acumulado == 84  # 64 de la primera corrida + 20 de la segunda
