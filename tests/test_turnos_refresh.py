"""
La recarga de turnos corre cada 60 s: solo debe escribir en el log cuando algo
cambió de verdad. Si no, son ~5 760 líneas diarias sin información.
"""

import ast
import textwrap
from datetime import time
from pathlib import Path

RUTA = Path(__file__).resolve().parent.parent / "Prensas.py"


def _cargar_firma():
    src = RUTA.read_text(encoding="utf-8")
    for nodo in ast.walk(ast.parse(src)):
        if isinstance(nodo, ast.FunctionDef) and nodo.name == "_firma_turnos":
            ns = {}
            exec(textwrap.dedent(ast.get_source_segment(src, nodo)), ns)
            return ns["_firma_turnos"]
    raise AssertionError("_firma_turnos no encontrada en Prensas.py")


firma = _cargar_firma()

DOS_TURNOS = {
    1: {"name": "Diurno", "start": time(8, 0), "end": time(20, 0)},
    2: {"name": "Nocturno", "start": time(20, 0), "end": time(8, 0)},
}


def test_misma_config_misma_firma():
    assert firma(DOS_TURNOS) == firma(dict(DOS_TURNOS))


def test_el_orden_no_altera_la_firma():
    invertido = {2: DOS_TURNOS[2], 1: DOS_TURNOS[1]}
    assert firma(DOS_TURNOS) == firma(invertido)


def test_cambiar_un_horario_se_detecta():
    """Lo que hiciste el 11-ago: mover el nocturno a las 10:00."""
    movido = {
        1: {"name": "Diurno", "start": time(8, 0), "end": time(10, 0)},
        2: {"name": "Nocturno", "start": time(10, 0), "end": time(8, 0)},
    }
    assert firma(DOS_TURNOS) != firma(movido)


def test_agregar_un_turno_se_detecta():
    tres = dict(DOS_TURNOS)
    tres[3] = {"name": "Tercero", "start": time(14, 0), "end": time(22, 0)}
    assert firma(DOS_TURNOS) != firma(tres)


def test_renombrar_no_cuenta_como_cambio():
    """El nombre no altera el cálculo de turno: no vale la pena avisar."""
    renombrado = {
        1: {"name": "Matutino", "start": time(8, 0), "end": time(20, 0)},
        2: {"name": "Vespertino", "start": time(20, 0), "end": time(8, 0)},
    }
    assert firma(DOS_TURNOS) == firma(renombrado)


def test_config_vacia_no_truena():
    assert firma({}) == firma(None) == ()
