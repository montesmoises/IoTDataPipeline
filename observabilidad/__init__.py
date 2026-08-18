"""
Observabilidad: qué está pasando en la planta, sin abrir el recolector.

Tres piezas con propósitos distintos:

    estado.py    Snapshot de AHORA por estación/lado, con el motivo ya
                 clasificado. Es texto y estado actual, por eso NO va en
                 Prometheus: se sirve por HTTP y Grafana lo lee como tabla.

    rechazos.py  Bitácora de partes rechazadas en JSONL (append-only, sin
                 relecturas). Reemplaza el CSV que se releía completo con
                 pandas en cada rechazo.

    metricas.py  Series numéricas para Prometheus: contadores, timeouts,
                 producción. Sirven para historia y ALERTAS.

    servidor.py  Un solo puerto que expone las tres.
"""
