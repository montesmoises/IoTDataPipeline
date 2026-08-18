"""
Registro de áreas: nombre en la base de datos -> clase que la implementa.

Las cinco áreas existen explícitamente aunque cuatro usen el comportamiento
base. Así, cuando pidan algo distinto para una de ellas, el lugar donde ponerlo
ya está y no hay que tocar nada más.
"""

import unicodedata

from areas.base import AreaPipeline
from areas.estampado import EstampadoPipeline


def normalizar_area(nombre) -> str:
    """
    'Carrocería Fase 1' -> 'carroceria fase 1'

    Quita acentos además de bajar a minúsculas: si no, 'carrocería' nunca
    empataría con la llave del registro.
    """
    if not nombre:
        return ""
    s = unicodedata.normalize("NFKD", str(nombre))
    s = "".join(c for c in s if not unicodedata.combining(c))
    return " ".join(s.lower().split())


# Áreas que hoy no cambian nada respecto de la base. Existen como punto de
# extensión: cuando una necesite comportamiento propio, se sobreescribe aquí.
class CarroceriaFase1Pipeline(AreaPipeline):
    nombre = "carroceria fase 1"


class CarroceriaFase2Pipeline(AreaPipeline):
    nombre = "carroceria fase 2"


class ChasisPipeline(AreaPipeline):
    nombre = "chasis"


_REGISTRO = {
    "estampado": EstampadoPipeline,
    "carroceria fase 1": CarroceriaFase1Pipeline,
    "carroceria fase 2": CarroceriaFase2Pipeline,
    "chasis": ChasisPipeline,
}


def obtener_pipeline(area_name) -> AreaPipeline:
    """
    Devuelve el pipeline del área. Nunca falla: un área desconocida o sin
    asignar (hay estaciones con area_name en NULL) usa el comportamiento base.
    """
    clave = normalizar_area(area_name)

    cls = _REGISTRO.get(clave)
    if cls is not None:
        return cls()

    # Compatibilidad con el código anterior, que hacía `"estampado" in nombre`:
    # así un área llamada "Estampado Fase 2" sigue resolviendo a Estampado.
    if "estampado" in clave:
        return EstampadoPipeline()

    return AreaPipeline()
