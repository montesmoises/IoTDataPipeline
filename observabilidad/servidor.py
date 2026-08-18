"""
Servidor HTTP del servicio: un solo puerto para monitoreo y diagnóstico.

    GET /health                 ¿está vivo? (para el servicio de Windows)
    GET /metrics                Prometheus
    GET /estaciones/estado      snapshot con el motivo clasificado  -> Grafana (Infinity)
    GET /partes-rechazadas      bitácora agrupada                   -> Grafana (Infinity)
    GET /debug/plc/<ip>         último bloque CRUDO leído           -> diagnóstico en vivo

El último es el que reemplaza abrir el recolector: muestra los words tal como
llegan del PLC, antes de decodificar, que es donde se ve si viene basura.

Corre en su propio hilo (daemon) con ThreadingHTTPServer: son pocos endpoints
consultados cada 15 s, no hace falta un framework.
"""

import json
import logging
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from observabilidad.estado import REGISTRO
from observabilidad.lecturas import LECTURAS

logger = logging.getLogger("supervisor")

#: Último bloque crudo por IP, para /debug/plc. Lo llena el colector.
_ultimas_lecturas = {}
_lock_lecturas = threading.Lock()

_inicio = datetime.now()
_rechazos_store = None


def registrar_lectura_cruda(ip, estacion, datos_crudos):
    """Guarda la última lectura de una IP para poder inspeccionarla por HTTP."""
    with _lock_lecturas:
        _ultimas_lecturas[ip] = {
            "ip": ip,
            "estacion": estacion,
            "ts": datetime.now().isoformat(timespec="milliseconds"),
            "datos": datos_crudos,
        }


def usar_store_rechazos(store):
    global _rechazos_store
    _rechazos_store = store


class _Handler(BaseHTTPRequestHandler):

    def log_message(self, formato, *args):
        pass  # sin ruido: cada scrape de Prometheus generaría una línea

    def _responder(self, codigo, cuerpo, tipo="application/json; charset=utf-8"):
        if not isinstance(cuerpo, bytes):
            cuerpo = cuerpo.encode("utf-8")
        self.send_response(codigo)
        self.send_header("Content-Type", tipo)
        self.send_header("Content-Length", str(len(cuerpo)))
        self.send_header("Access-Control-Allow-Origin", "*")  # Grafana desde otro host
        self.end_headers()
        self.wfile.write(cuerpo)

    def _json(self, datos, codigo=200):
        self._responder(codigo, json.dumps(datos, ensure_ascii=False, default=str))

    def do_GET(self):
        ruta = urlparse(self.path)
        camino = ruta.path.rstrip("/") or "/"
        params = parse_qs(ruta.query)

        try:
            if camino == "/health":
                self._json({
                    "estado": "ok",
                    "uptime_segundos": round((datetime.now() - _inicio).total_seconds()),
                    "resumen": REGISTRO.resumen(),
                })

            elif camino == "/metrics":
                self._responder(200, generate_latest(), CONTENT_TYPE_LATEST)

            elif camino == "/estaciones/estado":
                solo = params.get("solo_problemas", ["0"])[0] in ("1", "true", "si")
                self._json({
                    "generado": datetime.now().isoformat(timespec="seconds"),
                    "resumen": REGISTRO.resumen(),
                    "estaciones": REGISTRO.snapshot(solo_problemas=solo),
                })

            elif camino == "/estaciones/lecturas":
                # Lo que el PLC está mandando AHORA, por estación y lado, leído
                # con la misma conexión del recolector. Sustituye abrir el
                # recolector para "ver qué manda esa estación".
                self._json({
                    "generado": datetime.now().isoformat(timespec="seconds"),
                    "lecturas": LECTURAS.snapshot(
                        ip=params.get("ip", [None])[0],
                        estacion=params.get("estacion", [None])[0],
                    ),
                })

            elif camino == "/partes-rechazadas":
                if _rechazos_store is None:
                    self._json({"error": "bitácora no configurada"}, 503)
                    return
                desde = params.get("desde", [None])[0]
                limite = int(params.get("limite", ["1000"])[0])
                self._json({
                    "generado": datetime.now().isoformat(timespec="seconds"),
                    "rechazos": _rechazos_store.leer(desde=desde, limite=limite),
                })

            elif camino.startswith("/debug/plc"):
                ip = camino.rsplit("/", 1)[-1]
                with _lock_lecturas:
                    if ip in ("plc", ""):
                        self._json({"ips": sorted(_ultimas_lecturas)})
                        return
                    lectura = _ultimas_lecturas.get(ip)
                self._json(lectura or {"error": f"sin lecturas de {ip}"}, 200 if lectura else 404)

            elif camino == "/":
                self._json({
                    "servicio": "IoTDataPipeline",
                    "endpoints": [
                        "/health", "/metrics", "/estaciones/estado",
                        "/partes-rechazadas", "/debug/plc/<ip>",
                    ],
                })
            else:
                self._json({"error": "no encontrado"}, 404)

        except Exception as e:
            logger.error(f"Error atendiendo {self.path}: {e}")
            self._json({"error": str(e)}, 500)


def iniciar(puerto=9100):
    """Arranca el servidor en un hilo daemon. Devuelve el servidor o None."""
    try:
        servidor = ThreadingHTTPServer(("0.0.0.0", puerto), _Handler)
    except OSError as e:
        logger.error(f"❌ No se pudo abrir el puerto {puerto} para el servidor HTTP: {e}")
        return None

    hilo = threading.Thread(target=servidor.serve_forever, name="http", daemon=True)
    hilo.start()
    logger.info(
        f"🌐 Servidor HTTP en :{puerto} — /health /metrics /estaciones/estado "
        f"/partes-rechazadas /debug/plc/<ip>"
    )
    return servidor
