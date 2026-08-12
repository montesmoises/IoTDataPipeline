"""
Persistencia del estado de contadores en disco (state_cache/).

Este archivo guarda la LÍNEA BASE de cada contador: sin él, al arrancar no se
sabe desde dónde contar y se pierde la producción del hueco. Por eso la
escritura es atómica.
"""

import json
import logging
import os
from datetime import datetime, time

logger = logging.getLogger("supervisor")


def guardar_estado(state_file, active_records, ip):
    """
    Guarda el estado de forma ATÓMICA: temporal + fsync + os.replace.

    Escribir directo sobre el archivo final (open 'w') lo trunca de inmediato:
    un corte de energía a media escritura dejaba un JSON incompleto y al
    arrancar se perdía la línea base de TODAS las estaciones de esa IP.

    Con os.replace el archivo final nunca queda a medias: o tiene el contenido
    viejo completo, o el nuevo completo.
    """
    tmp_path = state_file.with_suffix('.json.tmp')
    try:
        serializable_data = {}
        for clave, record in active_records.items():
            rec_copy = record.copy()
            if isinstance(rec_copy.get('hora_cambio'), time):
                rec_copy['hora_cambio'] = rec_copy['hora_cambio'].strftime("%H:%M:%S")
            if rec_copy.get('id_registro') is None:
                rec_copy['id_registro'] = 0
            serializable_data[clave] = rec_copy

        with open(tmp_path, 'w', encoding='utf-8') as f:
            json.dump(serializable_data, f, indent=4, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())  # forzar a disco antes del rename

        os.replace(tmp_path, state_file)  # atómico en Windows y POSIX
    except Exception as e:
        logger.error(f"Error guardando estado para {ip}: {e}")
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass


def cargar_estado(state_file, ip):
    """
    Lee el estado previo. Devuelve {clave: registro} (vacío si no hay archivo).

    Si el archivo está corrupto se preserva con otro nombre en vez de
    sobrescribirlo en el siguiente guardado, y se avisa lo que implica.
    """
    if not state_file.exists():
        return {}

    active_records = {}
    try:
        with open(state_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        for clave, record in data.items():
            try:
                h_str = record.get('hora_cambio', '00:00:00')
                record['hora_cambio'] = datetime.strptime(h_str, "%H:%M:%S").time()

                # 🛡️ MIGRACIÓN DE JSON ANTIGUO: añadir '_GLOBAL' si la llave no trae lado
                if not any(clave.endswith(suf) for suf in
                           ['_GLOBAL', '_RH', '_LH', '_RH REAR', '_LH REAR', '_--']):
                    clave = f"{clave}_GLOBAL"

                if 'numero_original' not in record:
                    validated_part = clave.split('_', 1)[1] if '_' in clave else ''
                    record['numero_original'] = validated_part

                active_records[clave] = record
            except Exception as e:
                logger.error(f"Error al deserializar registro {clave}: {e}")
                continue

        logger.info(f"Estado recuperado para {ip}: {len(active_records)} registros cargados.")
        return active_records

    except Exception as e:
        logger.error(
            f"❌ No se pudo leer el estado de {ip}: {e}. "
            f"Se arranca SIN líneas base: los contadores se reestablecen desde "
            f"la lectura actual y no se contará la producción del hueco."
        )
        try:
            corrupto = state_file.with_suffix(
                f".json.corrupto-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
            )
            state_file.rename(corrupto)
            logger.error(f"   Archivo preservado como {corrupto.name} para revisión.")
        except Exception:
            pass
        return {}
