"""Contrato de áreas: resolución de partes y extras por área."""

import logging

import pytest

from areas import obtener_pipeline, normalizar_area
from areas.base import AreaPipeline, ContextoArea
from areas.estampado import EstampadoPipeline

LOG = logging.getLogger("test")


def ctx(estacion="MK05", validador=None):
    return ContextoArea(estacion=estacion, log=LOG, validar_estampado=validador)


class TestNormalizacion:
    def test_quita_acentos(self):
        assert normalizar_area("Carrocería Fase 1") == "carroceria fase 1"

    def test_baja_a_minusculas_y_recorta(self):
        assert normalizar_area("  CHASIS  ") == "chasis"

    def test_colapsa_espacios_multiples(self):
        assert normalizar_area("Carrocería   Fase   2") == "carroceria fase 2"

    @pytest.mark.parametrize("entrada", [None, "", "   "])
    def test_vacios(self, entrada):
        assert normalizar_area(entrada) == ""


class TestRegistro:
    def test_las_cinco_areas_reales_resuelven(self):
        """Los nombres exactos que están en la base de datos."""
        assert isinstance(obtener_pipeline("Estampado"), EstampadoPipeline)
        assert obtener_pipeline("Carrocería Fase 1").nombre == "carroceria fase 1"
        assert obtener_pipeline("Carrocería Fase 2").nombre == "carroceria fase 2"
        assert obtener_pipeline("Chasis").nombre == "chasis"

    def test_area_nula_usa_la_base(self):
        """Hay estaciones con area_name en NULL; no deben tronar."""
        for entrada in (None, "", "   "):
            assert type(obtener_pipeline(entrada)) is AreaPipeline

    def test_area_desconocida_usa_la_base(self):
        assert type(obtener_pipeline("Área Nueva Sin Registrar")) is AreaPipeline

    def test_compatibilidad_con_substring_estampado(self):
        """El código anterior hacía `"estampado" in nombre`: se conserva."""
        assert isinstance(obtener_pipeline("Estampado Fase 2"), EstampadoPipeline)


class TestAreaGeneral:
    def test_expande_numero_con_diagonal(self):
        numeros, error = AreaPipeline().resolver_partes("DGH9 53 83 XB/ZB", ctx())
        assert numeros == ["DGH95383XB", "DGH95383ZB"]
        assert error is None

    def test_numero_simple(self):
        numeros, error = AreaPipeline().resolver_partes("576960C010", ctx())
        assert numeros == ["576960C010"]
        assert error is None

    def test_vacio_devuelve_codigo_de_error(self):
        numeros, error = AreaPipeline().resolver_partes("", ctx())
        assert numeros == []
        assert error == "NO_PART_NUMBER"

    def test_no_agrega_columnas_extra(self):
        assert AreaPipeline().extras_history({"troquel_id": 215}) == {}

    def test_requiere_validacion_contra_part_numbers(self):
        assert AreaPipeline().requiere_validacion_bd() is True


class TestEstampado:
    def test_usa_el_validador_de_mdi_no_la_expansion(self):
        """
        'DA6A53/54603' NO debe expandirse por '/': en Estampado el MDI se
        resuelve contra AS400, que devuelve otros números distintos.
        """
        llamadas = []

        def validador(mdi, estacion, log):
            llamadas.append((mdi, estacion))
            return ["DA6A53603", "DA6A54603"], None

        numeros, error = EstampadoPipeline().resolver_partes(
            "DA6A53/54603", ctx(estacion="2500T  TR", validador=validador)
        )
        assert numeros == ["DA6A53603", "DA6A54603"]
        assert error is None
        assert llamadas == [("DA6A53/54603", "2500T  TR")]

    def test_un_mdi_puede_dar_varios_numeros(self):
        """Caso real: el MDI DGH953/54271/273 resolvió a 5 números activos."""
        cinco = ["DGH953271A", "DGH953273A", "DGH954271A", "DGH954273B", "DGH953271A-BP"]
        numeros, _ = EstampadoPipeline().resolver_partes(
            "DGH953/54271/273", ctx(validador=lambda *a: (cinco, None))
        )
        assert numeros == cinco

    def test_mdi_sin_resolver_propaga_el_error(self):
        numeros, error = EstampadoPipeline().resolver_partes(
            "XXX", ctx(validador=lambda *a: ([], "PART_NUMBER_OBSOLETO"))
        )
        assert numeros == []
        assert error == "PART_NUMBER_OBSOLETO"

    def test_mdi_vacio(self):
        numeros, error = EstampadoPipeline().resolver_partes("  ", ctx())
        assert numeros == []
        assert error == "NO_PART_NUMBER_ESTAMPADO"

    def test_sin_validador_no_truena(self):
        numeros, error = EstampadoPipeline().resolver_partes("DA6A53/54603", ctx())
        assert numeros == []
        assert error == "NO_PART_NUMBER_ESTAMPADO"

    def test_el_troquel_va_en_sequence(self):
        assert EstampadoPipeline().extras_history({"troquel_id": 215}) == {"sequence": 215}

    def test_sin_troquel_va_cero(self):
        assert EstampadoPipeline().extras_history({}) == {"sequence": 0}

    def test_no_revalida_contra_part_numbers(self):
        assert EstampadoPipeline().requiere_validacion_bd() is False


class TestNumerosSinEspacios:
    """
    Caso real de 2500T TR (2026-08-12): el MDI BDTS28BFC resuelve a la parte
    'BDTS28BFC -P', que en la BD lleva un espacio. Todas las consultas aguas
    abajo comparan contra REPLACE(pn.number,' ',''), así que el número debe
    salir del catálogo SIN espacios o nada empata y la producción se pierde.
    """

    def test_el_validador_devuelve_numeros_sin_espacios(self):
        from persistence.catalogo import _sin_espacios
        assert _sin_espacios("BDTS28BFC -P") == "BDTS28BFC-P"
        assert _sin_espacios("DGH9 53 83 XB") == "DGH95383XB"
        assert _sin_espacios(None) == ""

    def test_estampado_propaga_lo_que_da_el_catalogo(self):
        numeros, error = EstampadoPipeline().resolver_partes(
            "BDTS28BFC", ctx(estacion="2500T  TR",
                             validador=lambda *a: (["BDTS28BFC-P"], None))
        )
        assert numeros == ["BDTS28BFC-P"]
        assert all(" " not in n for n in numeros)
        assert error is None


class TestMultiplicador:
    """
    Piezas por golpe: SOLO Estampado. En las demás áreas un golpe es una pieza
    y no se consulta el atributo, así que un valor mal capturado en otra área
    no puede alterar su producción.
    """

    def test_solo_estampado_usa_multiplicador(self):
        assert EstampadoPipeline().usa_multiplicador is True

    @pytest.mark.parametrize("area", ["Carrocería Fase 1", "Carrocería Fase 2",
                                      "Chasis", None, "Área Nueva"])
    def test_las_demas_areas_no(self, area):
        assert obtener_pipeline(area).usa_multiplicador is False


class TestAgregarUnAreaNueva:
    def test_solo_hace_falta_heredar_y_sobreescribir(self):
        """
        La prueba que demuestra el objetivo: comportamiento propio para un área
        sin tocar el núcleo ni las demás.
        """
        class PinturaPipeline(AreaPipeline):
            nombre = "pintura"

            def extras_history(self, dato):
                return {"sequence": dato.get("cabina", 0)}

        p = PinturaPipeline()
        assert p.extras_history({"cabina": 3}) == {"sequence": 3}
        # y lo que no sobreescribe sigue siendo el comportamiento general
        assert p.resolver_partes("ABC 12/34", ctx())[0] == ["ABC12", "ABC34"]
        # las demás áreas no se enteran
        assert AreaPipeline().extras_history({"cabina": 3}) == {}
