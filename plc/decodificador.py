"""Traducción de los words del PLC a texto y agrupación de tags por lado."""

from typing import Tuple


def decodificar_bloque(bloque) -> Tuple[str, list, dict]:
    """
    Convierte una lista de words del PLC en el texto que representan.

    Cada word lleva DOS caracteres ASCII: el byte bajo primero. Se descartan
    los nulos y los no imprimibles, que es basura de memoria del PLC.

    Devuelve (original, [limpio], meta):
      - original: el texto tal como viene, con sus espacios
      - limpio:   sin espacios, que es como se compara contra part_numbers
                  (las consultas usan REPLACE(pn.number, ' ', ''))

    >>> decodificar_bloque([0x4241, 0x4443])
    ('ABCD', ['ABCD'], {})
    >>> decodificar_bloque([])
    (None, None, {})
    """
    if not bloque:
        return None, None, {}

    chars = [chr(v & 0xFF) + chr((v >> 8) & 0xFF) for v in bloque]
    original = "".join(chars).replace("\x00", "")
    original = ''.join(c for c in original if c.isprintable()).strip()

    limpia = original.replace(' ', '')
    if not limpia:
        return original, [], {}

    return original, [limpia], {}


def parse_tag(tag_name: str) -> Tuple[str, str]:
    """
    Deduce tipo y lado desde el NOMBRE del tag.

    'Contador RH'        -> ('contador', 'RH')
    'Número de Parte LH' -> ('parte', 'LH')
    'Puerto'             -> ('otro', 'GLOBAL')

    El orden importa: 'LH REAR' debe probarse antes que 'LH', o toda estación
    con lados traseros se agruparía mal.
    """
    lower = tag_name.lower()
    upper = tag_name.upper()

    grupo = "GLOBAL"
    if upper.endswith("LH REAR"):
        grupo = "LH REAR"
    elif upper.endswith("RH REAR"):
        grupo = "RH REAR"
    elif upper.endswith("LH"):
        grupo = "LH"
    elif upper.endswith("RH"):
        grupo = "RH"

    tipo = "otro"
    if "contador" in lower:
        tipo = "contador"
    elif "tiempo" in lower or "ciclo" in lower:
        tipo = "tiempo"
    elif "parte" in lower or "part" in lower:
        tipo = "parte"
    elif "troquel" in lower or "die" in lower:
        tipo = "troquel"

    return tipo, grupo
