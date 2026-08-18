"""
Recarga automática de configuración: un cambio de tag en una estación debe
detectarse solo, y NO debe disparar recarga en las IPs que no cambiaron.
"""

import ast
import hashlib
import textwrap
from pathlib import Path

import pytest

RUTA = Path(__file__).resolve().parent.parent / "Prensas.py"


def _cargar_huella():
    """Extrae _huella_config del archivo real, sin arrancar el programa."""
    src = RUTA.read_text(encoding="utf-8")
    for nodo in ast.walk(ast.parse(src)):
        if isinstance(nodo, ast.FunctionDef) and nodo.name == "_huella_config":
            ns = {"hashlib": hashlib}
            exec(textwrap.dedent(ast.get_source_segment(src, nodo)), ns)
            return ns["_huella_config"]
    raise AssertionError("_huella_config no encontrada en Prensas.py")


huella = _cargar_huella()


def config(direccion_contador="D3100", estaciones=("MK05",), port=5002, area="Carrocería Fase 1"):
    return {
        "port": port,
        "serie": "Q",
        "area": area,
        "estaciones": list(estaciones),
        "all_addresses": {(direccion_contador, 1), ("D3104", 10)},
        "station_configs": {
            "MK05": {
                "Contador LH": {"address": direccion_contador, "long": 1},
                "Número de Parte LH": {"address": "D3104", "long": 10},
            }
        },
    }


class TestDeteccionDeCambios:
    def test_sin_cambios_misma_huella(self):
        assert huella(config()) == huella(config())

    def test_el_orden_no_altera_la_huella(self):
        a = config(estaciones=("MK05", "MT04"))
        b = config(estaciones=("MT04", "MK05"))
        assert huella(a) == huella(b)

    def test_cambiar_la_direccion_de_un_tag_se_detecta(self):
        assert huella(config()) != huella(config(direccion_contador="D3200"))

    def test_agregar_una_estacion_se_detecta(self):
        assert huella(config()) != huella(config(estaciones=("MK05", "MT04")))

    def test_cambiar_el_puerto_se_detecta(self):
        assert huella(config()) != huella(config(port=1025))

    def test_cambiar_el_area_se_detecta(self):
        assert huella(config()) != huella(config(area="Estampado"))

    def test_agregar_un_tag_a_una_estacion_se_detecta(self):
        base = config()
        con_tag = config()
        con_tag["station_configs"]["MK05"]["Tiempo Ciclo LH"] = {"address": "D8180", "long": 1}
        assert huella(base) != huella(con_tag)

    def test_la_version_no_afecta_la_huella(self):
        """Si '_version' contara, la config se compararía consigo misma y nunca cuadraría."""
        a = dict(config(), _version=1)
        b = dict(config(), _version=99)
        assert huella(a) == huella(b)


class TestAislamientoEntreIPs:
    def test_cambiar_una_ip_no_altera_la_huella_de_la_otra(self):
        ip_a_antes = config(direccion_contador="D3100")
        ip_b_antes = config(direccion_contador="R6099", estaciones=("2500T  TR",))

        ip_a_despues = config(direccion_contador="D3200")   # solo cambia A
        ip_b_despues = config(direccion_contador="R6099", estaciones=("2500T  TR",))

        assert huella(ip_a_antes) != huella(ip_a_despues)   # A se recarga
        assert huella(ip_b_antes) == huella(ip_b_despues)   # B no se entera


class TestConfigVacia:
    @pytest.mark.parametrize("cfg", [{}, {"estaciones": [], "all_addresses": set()}])
    def test_no_truena_con_config_incompleta(self, cfg):
        assert isinstance(huella(cfg), str)
