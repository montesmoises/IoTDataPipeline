"""
Comportamiento por área de producción.

La clase base `AreaPipeline` implementa lo GENERAL. Cada área existe como su
propia clase aunque hoy no cambie nada: ese es el lugar obvio donde meter un
comportamiento distinto cuando lo pidan, sin tocar el núcleo ni las demás áreas.

Agregar un área nueva = una clase + una línea en el registro.
"""

from areas.base import AreaPipeline, ContextoArea
from areas.estampado import EstampadoPipeline
from areas.registro import obtener_pipeline, normalizar_area

__all__ = [
    "AreaPipeline",
    "ContextoArea",
    "EstampadoPipeline",
    "obtener_pipeline",
    "normalizar_area",
]
