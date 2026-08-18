"""Contrato que toda área cumple. La base implementa el comportamiento general."""

from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Tuple

from domain.partes import procesar_numero_parte


@dataclass
class ContextoArea:
    """
    Lo que un área necesita del mundo exterior para resolver un número de parte.

    Se pasa como argumento en vez de importarse, para que las áreas no dependan
    de AS400 ni de la base de datos directamente.
    """
    estacion: str
    log: Any
    validar_estampado: Optional[Callable] = None  # (mdi, estacion, log) -> (numeros, error)


class AreaPipeline:
    """
    Comportamiento general: el que aplica a Carrocería, Chasis y cualquier área
    que no necesite nada especial.
    """

    nombre = "general"

    # Piezas por golpe. Fuera de Estampado un golpe es una pieza, así que no se
    # consulta el atributo `pieces_per_shot` y el multiplicador es siempre 1.
    usa_multiplicador = False

    def resolver_partes(self, raw: str, ctx: ContextoArea) -> Tuple[List[str], Optional[str]]:
        """
        Traduce lo que manda el PLC a números de parte reales.

        Devuelve (numeros, codigo_error). Si la lista viene vacía, el código de
        error explica por qué y termina en el tablero de partes rechazadas.
        """
        numeros = procesar_numero_parte(raw)
        if numeros:
            return numeros, None
        return [], "NO_PART_NUMBER"

    def extras_history(self, dato: dict) -> dict:
        """
        Columnas extra al registrar en histories. La base no agrega ninguna.
        """
        return {}

    def requiere_validacion_bd(self) -> bool:
        """
        True si el número aún debe validarse contra part_numbers al crear el
        registro. Estampado ya validó contra AS400 y SQL, así que devuelve False.
        """
        return True
