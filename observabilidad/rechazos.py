"""
Bitácora de partes rechazadas, en JSONL de solo-append.

Reemplaza el CSV que se releía COMPLETO con pandas en cada rechazo (y luego
recorría tres columnas con .apply) solo para decidir si escribir. Con una parte
mal dada de alta, ese rechazo se repite cada ciclo y el costo crecía con el
archivo — todo dentro del hilo que además atiende los PLCs.

Aquí el índice de duplicados se carga UNA VEZ al arrancar y vive en memoria:
cada rechazo cuesta una comparación en un set y, si toca escribir, una línea.
"""

import json
import logging
import os
import threading
from datetime import date, datetime
from pathlib import Path

logger = logging.getLogger("supervisor")


class RechazosStore:
    """
    Registra cada rechazo de número de parte.

    `dedup_por_dia=True` conserva el comportamiento anterior (una fila por
    estación+número+día). En False registra cada ocurrencia, que es lo que
    permite distinguir "un alta faltante golpeando cada ciclo" de "un incidente
    aislado" — la columna 'veces' del tablero.
    """

    def __init__(self, path, dedup_por_dia=False):
        self.path = Path(path)
        self.dedup_por_dia = dedup_por_dia
        self._vistos = set()
        self._lock = threading.Lock()
        self._cargar_indice()

    def _cargar_indice(self):
        """Una sola lectura, al arrancar. Nunca más se relee el archivo."""
        if not self.path.exists():
            return
        try:
            with self.path.open(encoding="utf-8") as f:
                for linea in f:
                    linea = linea.strip()
                    if not linea:
                        continue
                    try:
                        r = json.loads(linea)
                        self._vistos.add((r["estacion"], r["numero_plc"], r["fecha"]))
                    except (json.JSONDecodeError, KeyError):
                        continue  # línea corrupta: se ignora, no tumba el resto
            logger.info(f"📋 Bitácora de rechazos: {len(self._vistos)} combinación(es) previas")
        except Exception as e:
            logger.error(f"Error leyendo la bitácora de rechazos: {e}")

    def registrar(self, estacion, numero_plc, tipo_error, lado="--", area="", mdi=None):
        """
        Anota un rechazo. Devuelve True si escribió, False si ya estaba.

        No lanza excepciones: registrar un rechazo nunca debe tumbar el conteo.
        """
        hoy = date.today().isoformat()
        numero = _limpiar(numero_plc)
        clave = (estacion, numero, hoy)

        try:
            with self._lock:
                if self.dedup_por_dia and clave in self._vistos:
                    return False
                self._vistos.add(clave)

                registro = {
                    "ts": datetime.now().isoformat(timespec="seconds"),
                    "fecha": hoy,
                    "estacion": estacion,
                    "lado": lado,
                    "area": area,
                    "numero_plc": numero,
                    "tipo_error": tipo_error,
                }
                if mdi:
                    registro["mdi"] = _limpiar(mdi)

                with self.path.open("a", encoding="utf-8") as f:
                    f.write(json.dumps(registro, ensure_ascii=False) + "\n")
            return True
        except Exception as e:
            logger.error(f"Error registrando rechazo de {numero_plc} en {estacion}: {e}")
            return False

    def leer(self, desde=None, limite=1000):
        """
        Devuelve los rechazos para el tablero, agrupados por
        (estación, lado, número, motivo) con su conteo y última hora.

        `desde` es 'YYYY-MM-DD'; por omisión, hoy.
        """
        desde = desde or date.today().isoformat()
        if not self.path.exists():
            return []

        agrupado = {}
        try:
            with self.path.open(encoding="utf-8") as f:
                for linea in f:
                    linea = linea.strip()
                    if not linea:
                        continue
                    try:
                        r = json.loads(linea)
                    except json.JSONDecodeError:
                        continue
                    if r.get("fecha", "") < desde:
                        continue

                    clave = (r.get("estacion"), r.get("lado"),
                             r.get("numero_plc"), r.get("tipo_error"))
                    fila = agrupado.get(clave)
                    if fila is None:
                        agrupado[clave] = {
                            "estacion": r.get("estacion"),
                            "lado": r.get("lado"),
                            "area": r.get("area", ""),
                            "numero_plc": r.get("numero_plc"),
                            "tipo_error": r.get("tipo_error"),
                            "veces": 1,
                            "primera_vez": r.get("ts"),
                            "ultima_vez": r.get("ts"),
                        }
                    else:
                        fila["veces"] += 1
                        fila["ultima_vez"] = r.get("ts")
        except Exception as e:
            logger.error(f"Error leyendo la bitácora de rechazos: {e}")
            return []

        filas = sorted(agrupado.values(), key=lambda x: x["veces"], reverse=True)
        return filas[:limite]


def _limpiar(valor):
    """Normaliza espacios y acota el largo; el PLC manda cadenas sucias."""
    return " ".join(str(valor or "").split())[:100]
