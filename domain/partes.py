"""Interpretación de los números de parte que manda el PLC."""

from itertools import product
from typing import List


def procesar_numero_parte(numero_plc: str) -> List[str]:
    """
    Expande el número de parte del PLC en todas sus combinaciones.

    El PLC puede condensar varios números en una sola cadena usando '/' como
    separador de alternativas dentro de un segmento:

        'DGH9 53 83 XB/ZB'  ->  ['DGH95383XB', 'DGH95383ZB']
        'ABC 12/34 99'      ->  ['ABC1299', 'ABC3499']

    Se divide primero por espacios (segmentos) y luego cada segmento por '/'
    (alternativas); el producto cartesiano de los segmentos da las combinaciones.
    Los espacios se eliminan del resultado para empatar con la comparación
    REPLACE(pn.number, ' ', '') que usan las consultas contra part_numbers.

    NO aplica a Estampado: esa área resuelve el MDI contra AS400.
    """
    if not numero_plc:
        return []

    segmentos = []
    for seg in numero_plc.split(' '):
        if not seg:
            continue
        # Alternativas del segmento, descartando vacías (p.ej. 'XB/' -> ['XB'])
        alternativas = [alt for alt in seg.split('/') if alt]
        if alternativas:
            segmentos.append(alternativas)

    if not segmentos:
        return []

    combinaciones = []
    for combo in product(*segmentos):
        nombre = ''.join(combo).replace(' ', '').strip()
        # Dedup: 'AB/AB' no debe generar dos registros para el mismo número
        if nombre and nombre not in combinaciones:
            combinaciones.append(nombre)

    return combinaciones
