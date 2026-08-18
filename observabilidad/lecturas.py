"""
Últimas lecturas del PLC, tal como las ve el recolector.

Se llena en cada ciclo de lectura, ANTES de cualquier decisión de negocio: aquí
está lo que el PLC mandó, aunque después la parte se rechace o el contador no
avance. Es el equivalente a mirar la tabla del recolector, pero por HTTP y
usando la MISMA conexión al PLC — sin abrir una segunda.
"""

import threading
from datetime import datetime
from typing import Dict, Tuple


class RegistroLecturas:
    """Última lectura por (estación, lado). Lo escribe el hilo del colector."""

    def __init__(self):
        self._lecturas: Dict[Tuple[str, str], dict] = {}
        self._lock = threading.Lock()

    def anotar(self, estacion, lado, ip=None, area=None, numero_plc=None,
               numero_validado=None, contador=None, tiempo_ciclo=None,
               troquel=None, validado=None, error=None):
        """Guarda lo que se acaba de leer. Sobrescribe la lectura anterior."""
        with self._lock:
            self._lecturas[(estacion, lado)] = {
                "estacion": estacion,
                "lado": lado,
                "ip": ip,
                "area": area,
                "numero_plc": numero_plc,          # crudo, con espacios
                "numero_validado": numero_validado,  # resuelto, si se resolvió
                "contador": contador,
                "tiempo_ciclo": tiempo_ciclo,
                "troquel": troquel,
                "validado": validado,
                "error": error,
                "ts": datetime.now(),
            }

    def olvidar_estacion(self, estacion):
        with self._lock:
            for clave in [k for k in self._lecturas if k[0] == estacion]:
                del self._lecturas[clave]

    def snapshot(self, ip=None, estacion=None):
        """Lista ordenada por estación y lado, con la antigüedad de cada dato."""
        with self._lock:
            filas = list(self._lecturas.values())

        if ip:
            filas = [f for f in filas if f.get("ip") == ip]
        if estacion:
            filas = [f for f in filas if f.get("estacion") == estacion]

        ahora = datetime.now()
        salida = []
        for f in sorted(filas, key=lambda x: (x["estacion"], x["lado"])):
            d = dict(f)
            d["ts"] = f["ts"].isoformat(timespec="seconds")
            d["hace_segundos"] = round((ahora - f["ts"]).total_seconds(), 1)
            salida.append(d)
        return salida


#: Registro global, compartido entre el colector y el servidor HTTP.
LECTURAS = RegistroLecturas()
