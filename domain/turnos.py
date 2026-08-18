"""
Cálculo de turno y detección de cambio de turno.

Versión pura: la configuración de turnos y la fecha de referencia se reciben
como argumentos en vez de leerse de un global. Prensas.py conserva envolturas
delgadas que le pasan SHIFTS_CONFIG y date.today().
"""

from datetime import date as _date, time, timedelta
from typing import Dict, Tuple


def get_current_shift(current_time: time, shifts_config: Dict, hoy: _date) -> Tuple[int, _date]:
    """
    Determina el turno y la fecha planificada para una hora dada.

    `shifts_config` es {id_turno: {'start': time, ...}}. Si viene vacío se usan
    los horarios por defecto (turno 1 de 08:00 a 20:00, turno 2 el resto).

    Devuelve (turno, fecha_plan). La fecha_plan retrocede un día cuando la hora
    cae en la madrugada, porque esa producción pertenece al turno nocturno que
    empezó el día anterior.
    """
    if not shifts_config:
        if time(8, 0) <= current_time < time(20, 0):
            return 1, hoy
        fecha_plan = hoy if current_time >= time(20, 0) else hoy - timedelta(days=1)
        return 2, fecha_plan

    sorted_shifts = sorted(shifts_config.items(), key=lambda x: x[1]['start'])

    if len(sorted_shifts) == 2:
        shift1_id, shift1_data = sorted_shifts[0]
        shift2_id, shift2_data = sorted_shifts[1]

        if shift1_data['start'] <= current_time < shift2_data['start']:
            return shift1_id, hoy
        if current_time >= shift2_data['start']:
            return shift2_id, hoy
        # Antes del inicio del turno 1: pertenece al turno 2 del día anterior
        return shift2_id, hoy - timedelta(days=1)

    for shift_id, shift_data in sorted_shifts:
        if shift_data['start'] <= current_time:
            turno = shift_id
            fecha_plan = hoy
            break
    else:
        turno = sorted_shifts[-1][0]
        fecha_plan = hoy - timedelta(days=1)

    return turno, fecha_plan


def has_shift_changed(previous_time: time, current_time: time, shifts_config: Dict) -> bool:
    """
    True si entre `previous_time` y `current_time` se cruzó el inicio de un turno.

    Solo detecta el cruce hacia adelante dentro del mismo día; el cruce de
    medianoche lo resuelve get_current_shift al recalcular la fecha planificada.
    """
    if not shifts_config:
        return ((previous_time < time(8, 0) <= current_time)
                or (previous_time < time(20, 0) <= current_time))

    for shift_data in shifts_config.values():
        if previous_time < shift_data['start'] <= current_time:
            return True

    return False
