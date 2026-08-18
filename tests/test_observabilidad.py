"""Estado clasificado y bitácora de rechazos."""

import json
from datetime import date

import pytest

from observabilidad import estado as est
from observabilidad.estado import RegistroEstados, motivo_de_error
from observabilidad.rechazos import RechazosStore


class TestTraduccionDeErrores:
    """Los códigos que ya produce el sistema deben mapear a un motivo del tablero."""

    @pytest.mark.parametrize("codigo,esperado", [
        ("PART_NUMBER_NO_EXISTE_BD", est.PARTE_NO_EXISTE),
        ("PART_NUMBER_OBSOLETO", est.PARTE_OBSOLETA),
        ("NO_PART_NUMBER", est.SIN_NUMERO_PARTE),
        ("NO_PART_NUMBER_ESTAMPADO", est.MDI_SIN_RESOLVER),
        ("MDI_NO_EXISTE", est.MDI_SIN_RESOLVER),
        ("MDI_DE_OTRA_ESTACION", est.PARTE_NO_EXISTE),
    ])
    def test_codigos_conocidos(self, codigo, esperado):
        assert motivo_de_error(codigo) == esperado

    def test_codigo_desconocido_no_truena(self):
        assert motivo_de_error("ALGO_NUEVO") == est.PARTE_NO_EXISTE

    def test_sin_codigo(self):
        assert motivo_de_error(None) == est.SIN_NUMERO_PARTE


class TestRegistroEstados:
    def test_anotar_y_leer(self):
        r = RegistroEstados()
        r.anotar("MK05", "RH", est.PRODUCIENDO, contador=153, numero_validado="DGH97165ZA")
        (fila,) = r.snapshot()
        assert fila["estacion"] == "MK05"
        assert fila["motivo"] == est.PRODUCIENDO
        assert fila["contador"] == 153
        assert fila["requiere_atencion"] is False

    def test_produciendo_y_detenido_no_requieren_atencion(self):
        """La distinción clave: 'la prensa está parada' no es un problema técnico."""
        r = RegistroEstados()
        r.anotar("A", "LH", est.PRODUCIENDO)
        r.anotar("B", "LH", est.CONTADOR_DETENIDO)
        r.anotar("C", "LH", est.ESTACION_SIN_PARTES)
        assert r.snapshot(solo_problemas=True) == []

    def test_los_motivos_tecnicos_si_requieren_atencion(self):
        r = RegistroEstados()
        for i, motivo in enumerate([est.PARTE_NO_EXISTE, est.PLC_DESCONECTADO,
                                    est.LECTURA_PARCIAL, est.MDI_SIN_RESOLVER,
                                    est.SIN_TAG_DE_PARTE, est.PARTE_OBSOLETA]):
            r.anotar(f"E{i}", "LH", motivo)
        assert len(r.snapshot(solo_problemas=True)) == 6

    def test_los_problemas_van_primero(self):
        r = RegistroEstados()
        r.anotar("ZZZ", "LH", est.PRODUCIENDO)
        r.anotar("AAA", "LH", est.PARTE_NO_EXISTE)
        assert r.snapshot()[0]["estacion"] == "AAA"

    def test_anotar_conserva_campos_previos(self):
        """Al cambiar de motivo no se debe perder el número que mandó el PLC."""
        r = RegistroEstados()
        r.anotar("MK05", "RH", est.PRODUCIENDO, numero_plc="DGH9 71 65ZA", contador=200)
        r.anotar("MK05", "RH", est.CONTADOR_DETENIDO)
        (fila,) = r.snapshot()
        assert fila["numero_plc"] == "DGH9 71 65ZA"
        assert fila["contador"] == 200

    def test_anotar_estacion_aplica_a_todos_los_lados(self):
        r = RegistroEstados()
        r.anotar("MK05", "LH", est.PRODUCIENDO)
        r.anotar("MK05", "RH", est.PRODUCIENDO)
        r.anotar("MT04", "LH", est.PRODUCIENDO)
        r.anotar_estacion("MK05", est.PLC_DESCONECTADO)
        motivos = {(f["estacion"], f["lado"]): f["motivo"] for f in r.snapshot()}
        assert motivos[("MK05", "LH")] == est.PLC_DESCONECTADO
        assert motivos[("MK05", "RH")] == est.PLC_DESCONECTADO
        assert motivos[("MT04", "LH")] == est.PRODUCIENDO   # no se toca

    def test_resumen(self):
        r = RegistroEstados()
        r.anotar("A", "LH", est.PRODUCIENDO)
        r.anotar("A", "RH", est.CONTADOR_DETENIDO)
        r.anotar("B", "LH", est.PARTE_NO_EXISTE)
        res = r.resumen()
        assert res["lados_totales"] == 3
        assert res["produciendo"] == 1
        assert res["detenidos"] == 1
        assert res["requieren_atencion"] == 1
        assert res["estaciones"] == 2


class TestRechazosStore:
    def test_registra_y_agrupa_con_conteo(self, tmp_path):
        """Lo que el CSV no podía: distinguir un alta faltante de un incidente."""
        s = RechazosStore(tmp_path / "r.jsonl")
        for _ in range(96):
            s.registrar("MT05", "576960C010", "PART_NUMBER_NO_EXISTE_BD", lado="RH")
        s.registrar("MK05", "OTRA", "NO_PART_NUMBER", lado="LH")

        filas = s.leer()
        assert filas[0]["numero_plc"] == "576960C010"
        assert filas[0]["veces"] == 96
        assert filas[0]["estacion"] == "MT05"
        assert len(filas) == 2

    def test_dedup_por_dia_opcional(self, tmp_path):
        s = RechazosStore(tmp_path / "r.jsonl", dedup_por_dia=True)
        assert s.registrar("MT05", "X", "E") is True
        assert s.registrar("MT05", "X", "E") is False
        assert s.leer()[0]["veces"] == 1

    def test_el_indice_sobrevive_al_reinicio(self, tmp_path):
        ruta = tmp_path / "r.jsonl"
        s1 = RechazosStore(ruta, dedup_por_dia=True)
        s1.registrar("MT05", "X", "E")

        s2 = RechazosStore(ruta, dedup_por_dia=True)   # "reinicio"
        assert s2.registrar("MT05", "X", "E") is False

    def test_linea_corrupta_no_tumba_la_lectura(self, tmp_path):
        ruta = tmp_path / "r.jsonl"
        ruta.write_text(
            json.dumps({"fecha": date.today().isoformat(), "estacion": "A", "lado": "LH",
                        "numero_plc": "X", "tipo_error": "E", "ts": "2026-08-12T10:00:00"}) + "\n"
            + '{"roto": \n'
            + json.dumps({"fecha": date.today().isoformat(), "estacion": "B", "lado": "LH",
                          "numero_plc": "Y", "tipo_error": "E", "ts": "2026-08-12T10:01:00"}) + "\n",
            encoding="utf-8")
        assert len(RechazosStore(ruta).leer()) == 2

    def test_normaliza_el_numero_del_plc(self, tmp_path):
        s = RechazosStore(tmp_path / "r.jsonl")
        s.registrar("MT05", "  DGH9   53  \n", "E")
        assert s.leer()[0]["numero_plc"] == "DGH9 53"

    def test_guarda_el_mdi_cuando_aplica(self, tmp_path):
        s = RechazosStore(tmp_path / "r.jsonl")
        s.registrar("2500T  TR", "BDWK34812", "MDI_NO_EXISTE", mdi="BDWK34812")
        linea = json.loads((tmp_path / "r.jsonl").read_text(encoding="utf-8").strip())
        assert linea["mdi"] == "BDWK34812"

    def test_archivo_inexistente_devuelve_vacio(self, tmp_path):
        assert RechazosStore(tmp_path / "no-existe.jsonl").leer() == []

    def test_registrar_nunca_lanza(self, tmp_path):
        """Registrar un rechazo jamás debe tumbar el conteo de producción."""
        s = RechazosStore(tmp_path / "sub" / "no-existe" / "r.jsonl")
        assert s.registrar("A", "X", "E") is False   # falla, pero no lanza
