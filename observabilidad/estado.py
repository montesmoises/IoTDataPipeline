"""
Estado actual de cada estación con el MOTIVO ya clasificado.

El problema que resuelve: cuando una estación no registra producción, la base de
datos no ayuda — si el número de parte está mal, sencillamente no se escribe
nada. La ausencia es el síntoma, no el diagnóstico.

Aquí el servicio anota *por qué* en el momento en que decide no registrar, que
es cuando tiene toda la información. Después basta leerlo.
"""

import threading
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Dict, Optional, Tuple

# ── Motivos ────────────────────────────────────────────────────────────────
PRODUCIENDO = "PRODUCIENDO"              # todo bien, el contador avanza
CONTADOR_DETENIDO = "CONTADOR_DETENIDO"  # todo bien, la prensa no está golpeando
PLC_DESCONECTADO = "PLC_DESCONECTADO"
LECTURA_PARCIAL = "LECTURA_PARCIAL"      # el PLC responde pero faltan bloques
SIN_TAG_DE_PARTE = "SIN_TAG_DE_PARTE"    # el lado tiene contador pero no tag de parte
SIN_NUMERO_PARTE = "SIN_NUMERO_PARTE"    # el PLC manda vacío o basura
PARTE_NO_EXISTE = "PARTE_NO_EXISTE"      # no está dada de alta para esta estación
PARTE_OBSOLETA = "PARTE_OBSOLETA"
MDI_SIN_RESOLVER = "MDI_SIN_RESOLVER"    # estampado: el MDI no mapea a nada
ESTACION_SIN_PARTES = "ESTACION_SIN_PARTES"  # el PLC no reporta ninguna parte

#: Todos los motivos, para poner en 0 los inactivos en Prometheus.
MOTIVOS_POSIBLES = (
    PRODUCIENDO, CONTADOR_DETENIDO, PLC_DESCONECTADO, LECTURA_PARCIAL,
    SIN_TAG_DE_PARTE, SIN_NUMERO_PARTE, PARTE_NO_EXISTE, PARTE_OBSOLETA,
    MDI_SIN_RESOLVER, ESTACION_SIN_PARTES,
)

#: Motivos que NO requieren intervención técnica.
BENIGNOS = frozenset({PRODUCIENDO, CONTADOR_DETENIDO, ESTACION_SIN_PARTES})

#: Traducción de los códigos de error de la BD/catálogo al motivo del tablero.
DESDE_ERROR = {
    "PART_NUMBER_NO_EXISTE_BD": PARTE_NO_EXISTE,
    "PART_NUMBER_OBSOLETO": PARTE_OBSOLETA,
    "NO_PART_NUMBER": SIN_NUMERO_PARTE,
    "NO_PART_NUMBER_ESTAMPADO": MDI_SIN_RESOLVER,
    "MDI_NO_EXISTE": MDI_SIN_RESOLVER,
    "MDI_DE_OTRA_ESTACION": PARTE_NO_EXISTE,
    "SQL_NO_ENCONTRADO": PARTE_NO_EXISTE,
    "DB_ERROR": PARTE_NO_EXISTE,
    "SIN_CONEXION_BD": PARTE_NO_EXISTE,
}


def motivo_de_error(codigo: Optional[str]) -> str:
    """Traduce un código de error a motivo; lo desconocido cae en 'no existe'."""
    if not codigo:
        return SIN_NUMERO_PARTE
    return DESDE_ERROR.get(codigo, PARTE_NO_EXISTE)


@dataclass
class EstadoEstacion:
    estacion: str
    lado: str
    area: str = ""
    ip: str = ""
    motivo: str = ESTACION_SIN_PARTES
    numero_plc: Optional[str] = None       # lo que mandó el PLC, crudo
    numero_validado: Optional[str] = None  # lo que se resolvió, si se resolvió
    contador: Optional[int] = None
    detalle: Optional[str] = None
    actualizado: datetime = field(default_factory=datetime.now)

    @property
    def requiere_atencion(self) -> bool:
        return self.motivo not in BENIGNOS

    def como_dict(self) -> dict:
        d = asdict(self)
        d["actualizado"] = self.actualizado.isoformat(timespec="seconds")
        d["segundos_sin_dato"] = round((datetime.now() - self.actualizado).total_seconds(), 1)
        d["requiere_atencion"] = self.requiere_atencion
        return d


class RegistroEstados:
    """
    Snapshot vivo por (estación, lado).

    Lo escriben los hilos de BD y lo lee el hilo del servidor HTTP, así que
    todo pasa por un lock.
    """

    def __init__(self):
        self._estados: Dict[Tuple[str, str], EstadoEstacion] = {}
        self._lock = threading.Lock()

    def anotar(self, estacion, lado, motivo, **campos):
        """Registra el estado actual de un lado. Sobrescribe el anterior."""
        clave = (estacion, lado)
        with self._lock:
            actual = self._estados.get(clave)
            if actual is None:
                actual = EstadoEstacion(estacion=estacion, lado=lado)
                self._estados[clave] = actual

            actual.motivo = motivo
            actual.actualizado = datetime.now()
            for nombre, valor in campos.items():
                if valor is not None and hasattr(actual, nombre):
                    setattr(actual, nombre, valor)
        return actual

    def anotar_estacion(self, estacion, motivo, **campos):
        """Aplica un motivo a TODOS los lados conocidos de una estación."""
        with self._lock:
            lados = [k for k in self._estados if k[0] == estacion]
        for _, lado in lados:
            self.anotar(estacion, lado, motivo, **campos)

    def olvidar_estacion(self, estacion):
        with self._lock:
            for clave in [k for k in self._estados if k[0] == estacion]:
                del self._estados[clave]

    def snapshot(self, solo_problemas=False, area=None, estacion=None):
        """
        Lista de dicts, ordenada: primero lo que requiere atención.

        `area` y `estacion` aceptan varios valores separados por coma, para que
        las variables de plantilla de Grafana puedan filtrar en multiselección.
        """
        with self._lock:
            estados = list(self._estados.values())

        if solo_problemas:
            estados = [e for e in estados if e.requiere_atencion]
        if area:
            permitidas = {a.strip() for a in str(area).split(",") if a.strip()}
            estados = [e for e in estados if e.area in permitidas]
        if estacion:
            permitidas = {s.strip() for s in str(estacion).split(",") if s.strip()}
            estados = [e for e in estados if e.estacion in permitidas]

        estados.sort(key=lambda e: (not e.requiere_atencion, e.estacion, e.lado))
        return [e.como_dict() for e in estados]

    def areas(self):
        """Áreas conocidas, para poblar el selector del tablero."""
        with self._lock:
            return sorted({e.area for e in self._estados.values() if e.area})

    def resumen(self, area=None, estacion=None):
        """Cifras para los recuadros de arriba del tablero."""
        with self._lock:
            estados = list(self._estados.values())

        if area:
            permitidas = {a.strip() for a in str(area).split(",") if a.strip()}
            estados = [e for e in estados if e.area in permitidas]
        if estacion:
            permitidas = {s.strip() for s in str(estacion).split(",") if s.strip()}
            estados = [e for e in estados if e.estacion in permitidas]

        por_motivo = {}
        for e in estados:
            por_motivo[e.motivo] = por_motivo.get(e.motivo, 0) + 1

        return {
            "lados_totales": len(estados),
            "produciendo": sum(1 for e in estados if e.motivo == PRODUCIENDO),
            "detenidos": sum(1 for e in estados if e.motivo == CONTADOR_DETENIDO),
            "requieren_atencion": sum(1 for e in estados if e.requiere_atencion),
            "estaciones": len({e.estacion for e in estados}),
            "por_motivo": dict(sorted(por_motivo.items())),
        }


#: Registro global, compartido por los hilos de proceso y el servidor HTTP.
REGISTRO = RegistroEstados()
