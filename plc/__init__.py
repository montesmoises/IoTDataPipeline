"""
Interpretación de lo que manda el PLC: decodificación de bloques y tags.

Igual que domain/, aquí no se importa pyodbc ni customtkinter. La lectura por
socket vive en Prensas.py; esto solo traduce los words a algo legible.
"""

from plc.decodificador import decodificar_bloque, parse_tag

__all__ = ["decodificar_bloque", "parse_tag"]
