"""Cálculo de turno y detección de cambio de turno."""

from datetime import date, time, timedelta

from domain.turnos import get_current_shift, has_shift_changed

HOY = date(2026, 8, 11)
AYER = HOY - timedelta(days=1)

# La configuración real de la planta: dos turnos
DOS_TURNOS = {
    1: {"name": "Diurno", "start": time(8, 0), "end": time(20, 0)},
    2: {"name": "Nocturno", "start": time(20, 0), "end": time(8, 0)},
}


class TestTurnoActual:
    def test_media_maniana_es_turno_1(self):
        assert get_current_shift(time(9, 57), DOS_TURNOS, HOY) == (1, HOY)

    def test_arranque_exacto_del_turno_1(self):
        assert get_current_shift(time(8, 0), DOS_TURNOS, HOY) == (1, HOY)

    def test_noche_es_turno_2_del_mismo_dia(self):
        assert get_current_shift(time(21, 30), DOS_TURNOS, HOY) == (2, HOY)

    def test_madrugada_pertenece_al_turno_2_del_dia_ANTERIOR(self):
        """
        Lo más fácil de equivocar: a las 03:00 la producción es del turno
        nocturno que empezó ayer, así que la fecha planificada retrocede.
        """
        assert get_current_shift(time(3, 0), DOS_TURNOS, HOY) == (2, AYER)

    def test_un_minuto_antes_del_turno_1_sigue_siendo_de_ayer(self):
        assert get_current_shift(time(7, 59), DOS_TURNOS, HOY) == (2, AYER)

    def test_sin_configuracion_usa_horarios_por_defecto(self):
        assert get_current_shift(time(10, 0), {}, HOY) == (1, HOY)
        assert get_current_shift(time(22, 0), {}, HOY) == (2, HOY)
        assert get_current_shift(time(2, 0), {}, HOY) == (2, AYER)

    def test_turno_nocturno_movido_para_pruebas(self):
        """Lo que hiciste el 2026-08-11: mover el nocturno a las 10:00."""
        cfg = {
            1: {"start": time(8, 0)},
            2: {"start": time(10, 0)},
        }
        assert get_current_shift(time(9, 57), cfg, HOY) == (1, HOY)
        assert get_current_shift(time(10, 0), cfg, HOY) == (2, HOY)


class TestCambioDeTurno:
    def test_detecta_el_cruce(self):
        assert has_shift_changed(time(19, 59), time(20, 1), DOS_TURNOS) is True

    def test_cruce_exacto_en_el_minuto_de_inicio(self):
        assert has_shift_changed(time(19, 59), time(20, 0), DOS_TURNOS) is True

    def test_no_hay_cruce_dentro_del_mismo_turno(self):
        assert has_shift_changed(time(9, 0), time(11, 0), DOS_TURNOS) is False

    def test_caso_real_mk05(self):
        """Nocturno movido a las 10:00; el cambio se detectó a las 10:00:22."""
        cfg = {1: {"start": time(8, 0)}, 2: {"start": time(10, 0)}}
        assert has_shift_changed(time(9, 59, 57), time(10, 0, 22), cfg) is True

    def test_no_repite_el_cruce_una_vez_pasado(self):
        cfg = {1: {"start": time(8, 0)}, 2: {"start": time(10, 0)}}
        assert has_shift_changed(time(10, 0, 22), time(10, 8, 31), cfg) is False

    def test_sin_configuracion(self):
        assert has_shift_changed(time(7, 59), time(8, 1), {}) is True
        assert has_shift_changed(time(9, 0), time(10, 0), {}) is False
