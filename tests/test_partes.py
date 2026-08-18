"""Expansión de números de parte que manda el PLC (áreas distintas de Estampado)."""

import pytest

from domain.partes import procesar_numero_parte


class TestExpansion:
    def test_caso_del_usuario(self):
        """'DGH9 53 83 XB/ZB' son en realidad dos números de parte."""
        assert procesar_numero_parte("DGH9 53 83 XB/ZB") == ["DGH95383XB", "DGH95383ZB"]

    def test_alternativa_en_segmento_intermedio(self):
        assert procesar_numero_parte("ABC 12/34 99") == ["ABC1299", "ABC3499"]

    def test_dos_alternativas_dan_producto_cartesiano(self):
        assert procesar_numero_parte("A/B C/D") == ["AC", "AD", "BC", "BD"]

    def test_numero_simple_sin_separadores(self):
        assert procesar_numero_parte("SIMPLE123") == ["SIMPLE123"]

    def test_solo_espacios_se_concatenan(self):
        assert procesar_numero_parte("ABC 123") == ["ABC123"]

    def test_espacios_dobles(self):
        assert procesar_numero_parte("DGH9  53 XB/ZB") == ["DGH953XB", "DGH953ZB"]

    def test_alternativa_vacia_se_ignora(self):
        assert procesar_numero_parte("XB/ XB") == ["XBXB"]

    def test_deduplica(self):
        """'AB/AB' no debe crear dos registros para el mismo número."""
        assert procesar_numero_parte("AB/AB 9") == ["AB9"]

    @pytest.mark.parametrize("entrada", ["", "   ", None])
    def test_entradas_vacias(self, entrada):
        assert procesar_numero_parte(entrada) == []

    def test_resultado_sin_espacios(self):
        """Las consultas comparan con REPLACE(pn.number, ' ', ''): no deben quedar espacios."""
        for nombre in procesar_numero_parte("DGH9 53 83 XB/ZB"):
            assert " " not in nombre

    def test_el_orden_es_estable(self):
        """Importa porque determina qué registro se crea primero."""
        assert procesar_numero_parte("A/B C") == procesar_numero_parte("A/B C")
