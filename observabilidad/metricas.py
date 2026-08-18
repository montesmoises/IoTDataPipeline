"""
Métricas para Prometheus.

Regla de cardinalidad: las etiquetas son estación, lado, área, IP y motivo —
todos con pocos valores posibles. El NÚMERO DE PARTE nunca es etiqueta: son
miles y cada combinación crea una serie nueva. El número de parte vive en el
snapshot de estado y en los logs, donde puede ser texto libre sin costo.

Con 109 estaciones y hasta 4 lados son ~436 series por métrica: cómodo.
"""

from prometheus_client import Counter, Gauge, Histogram

# ── Lo que manda el PLC ────────────────────────────────────────────────────
plc_contador = Gauge(
    "plc_contador", "Valor crudo del contador del PLC",
    ["estacion", "lado"],
)
plc_tiempo_ciclo = Gauge(
    "plc_tiempo_ciclo_segundos", "Tiempo de ciclo reportado por el PLC",
    ["estacion", "lado"],
)
plc_conectado = Gauge(
    "plc_conectado", "1 si el PLC responde, 0 si no",
    ["ip"],
)
plc_ultima_lectura = Gauge(
    "plc_ultima_lectura_timestamp", "Epoch de la última lectura correcta",
    ["ip"],
)

# ── Salud de la adquisición ────────────────────────────────────────────────
plc_lecturas = Counter(
    "plc_lecturas_total", "Lecturas al PLC por resultado",
    ["ip", "resultado"],           # ok | timeout | parcial | error
)
plc_duracion_lectura = Histogram(
    "plc_duracion_lectura_segundos", "Duración del ciclo de lectura",
    ["ip"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

# ── Negocio ────────────────────────────────────────────────────────────────
produccion_piezas = Counter(
    "produccion_piezas_total", "Piezas registradas en production_records",
    ["estacion", "lado", "area"],
)
produccion_golpes = Counter(
    "produccion_golpes_total", "Incremento crudo del contador (golpes o piezas según el área)",
    ["estacion", "lado", "area"],
)
partes_rechazadas = Counter(
    "partes_rechazadas_total", "Números de parte rechazados",
    ["estacion", "tipo_error"],    # sin el número de parte: cardinalidad
)
resets_contador = Counter(
    "resets_contador_total", "Reinicios del contador del PLC detectados",
    ["estacion", "lado"],
)
cambios_turno = Counter(
    "cambios_turno_total", "Cambios de turno procesados",
    ["estacion"],
)

# ── Estado clasificado (para alertar sin leer el snapshot) ─────────────────
# `area` no multiplica series: cada estación pertenece a una sola. Se incluye
# para poder filtrar los tableros por área con variables de plantilla.
estacion_motivo = Gauge(
    "estacion_motivo", "1 en el motivo activo de cada lado, 0 en los demás",
    ["estacion", "lado", "area", "motivo"],
)

# ── Configuración ──────────────────────────────────────────────────────────
config_version = Gauge(
    "config_version", "Versión de configuración que está usando el lector",
    ["ip"],
)


def marcar_motivo(estacion, lado, motivo, motivos_posibles, area=""):
    """
    Pone en 1 el motivo activo y en 0 los demás, para que
    `estacion_motivo{motivo="PARTE_NO_EXISTE"} == 1` sea directamente alertable.
    """
    for m in motivos_posibles:
        estacion_motivo.labels(estacion, lado, str(area or ""), m).set(1 if m == motivo else 0)
