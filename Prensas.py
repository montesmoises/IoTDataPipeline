import pandas as pd
import datetime
import time as system_time
from pymcprotocol import Type3E
from datetime import datetime, time, timedelta, date
from collections import defaultdict
import asyncio, hashlib, pyodbc
import functools
import logging
import os
import sys
from pathlib import Path
from typing import Optional, Dict, Any, Tuple
import threading
from concurrent.futures import ThreadPoolExecutor
import re
import json
import traceback
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler

# CustomTkinter para la nueva UI
import customtkinter as ctk
from tkinter import ttk, StringVar

# Lógica de negocio pura (sin PLC ni BD). Ver domain/__init__.py
from domain.contadores import calcular_incremento, calcular_delta_turno, piezas_producidas
from domain import turnos as _turnos

# Comportamiento por área. Agregar un área = una clase + una línea. Ver areas/
from areas import obtener_pipeline, ContextoArea

# Acceso a datos. Todo el SQL vive en persistence/repositorio.py
from persistence import repositorio as repo
from persistence import estado as estado_store
from persistence import catalogo
from persistence.repositorio import actualizar_registro, obtener_part_number_id

# ═══════════════════════════ CONFIGURACIÓN GLOBAL ═══════════════════════════

# Cargar variables del entorno desde .env
load_dotenv()

# Directorios
LOGS_DIR = Path("logs")
LOGS_DIR.mkdir(exist_ok=True)
CSV_DIR = Path("part_numbers_not_found")
CSV_DIR.mkdir(exist_ok=True)
STATE_DIR = Path("state_cache")
STATE_DIR.mkdir(exist_ok=True)

# Configurar logging CON ROTACIÓN
logging.basicConfig(level=logging.NOTSET, handlers=[])
logger = logging.getLogger("supervisor")
logger.setLevel(logging.INFO)
logger.propagate = False

# 🔄 Handler con rotación para el logger principal
supervisor_log_path = LOGS_DIR / "supervisor.log"
supervisor_file_handler = RotatingFileHandler(
    supervisor_log_path,
    mode='a',
    encoding='utf-8',
    maxBytes=100*1024*1024,  # 10 MB
    backupCount=2           # 5 backups
)
supervisor_file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
supervisor_file_handler.setFormatter(formatter)
logger.addHandler(supervisor_file_handler)

#  Handler para consola (solo INFO y superior)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)  # Solo INFO, WARNING, ERROR
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

station_loggers = {}

# Intervalo (en segundos) para volver a leer la configuración de la BD
POLL_INTERVAL = 60  # 🚀 Aumentado a 60s (se puede forzar update manual)

# Variables globales para control de actualización manual
global_async_loop = None
global_config_event = None

# Archivo CSV de números de parte no encontrados
CSV_FILE = CSV_DIR / "parts_not_found.csv"

#  CONFIGURACIÓN GLOBAL DE CONEXIÓN PLC
PLC_CONNECTION_TIMEOUT = 15  # 5 segundos de timeout
PLC_READ_TIMEOUT = 10        # 3 segundos para lectura
RECONNECT_DELAY = 10        # 10 segundos entre reconexiones fallidas

# ═══════════════════════════ POOLS DE HILOS (I/O BLOQUEANTE) ═══════════════════════════
# pymcprotocol y pyodbc son bloqueantes: si se llaman directo desde una corrutina
# congelan el event loop entero y con él la lectura de TODOS los PLCs.
# Estos executors los sacan del loop. Las conexiones se indexan por hilo, así que
# dos hilos nunca comparten una conexión (ver ConnectionPool).
PLC_IO_WORKERS = int(os.getenv('PLC_IO_WORKERS', '16'))  # 1 hilo por PLC concurrente
DB_WORKERS = int(os.getenv('DB_WORKERS', '8'))           # limita conexiones SQL vivas

_plc_executor = ThreadPoolExecutor(max_workers=PLC_IO_WORKERS, thread_name_prefix="plc-io")
_db_executor = ThreadPoolExecutor(max_workers=DB_WORKERS, thread_name_prefix="db")

# ═══════════════════════════ POOL DE CONEXIONES ═══════════════════════════

class ConnectionPool:
    """Pool simple para reutilizar conexiones"""

    _sql_pool = {}  # {connection_key: (conn, last_used)}
    _as400_pool = {}  # {connection_key: (conn, last_used)}
    _cleanup_interval = 300  # Limpiar conexiones inactivas cada 300 segundos
    _max_idle_time = 600  # Max 10 minutos inactiva
    _lock = threading.RLock()

    @classmethod
    def get_sql_connection(cls):
        """
        Obtiene o crea conexión SQL reutilizable, UNA POR HILO.

        Antes existía una sola conexión global ("default_sql") compartida por todo
        el sistema: los commit de una estación confirmaban el trabajo a medias de
        otra, y en cuanto el I/O dejó de bloquear el loop esa conexión pasaría a
        usarse concurrentemente (pyodbc no lo soporta). Indexar por hilo garantiza
        que dos unidades de trabajo nunca compartan conexión, y acota el total al
        tamaño de los executors.
        """
        with cls._lock:
            current_time = datetime.now()
            connection_key = f"sql_{threading.get_ident()}"

            # Limpiar conexiones antiguas periódicamente
            if hasattr(cls, '_last_cleanup'):
                if (current_time - cls._last_cleanup).total_seconds() > cls._cleanup_interval:
                    cls._cleanup_old_connections()
                    cls._last_cleanup = current_time
            else:
                cls._last_cleanup = current_time

            # Reutilizar conexión existente si está disponible y válida
            if connection_key in cls._sql_pool:
                conn, last_used = cls._sql_pool[connection_key]

                # Verificar si la conexión sigue viva
                try:
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1")
                    cursor.fetchone()

                    # Actualizar tiempo de uso
                    cls._sql_pool[connection_key] = (conn, current_time)

                    # Log solo la primera vez
                    if not hasattr(cls, '_sql_reused_logged'):
                        logger.debug("♻️ Reutilizando conexión SQL existente")
                        cls._sql_reused_logged = True

                    return conn
                except:
                    # Conexión muerta, cerrar y crear nueva
                    try:
                        conn.close()
                    except:
                        pass
                    del cls._sql_pool[connection_key]

            # Crear nueva conexión
            conn = cls._create_sql_connection()
            if conn:
                cls._sql_pool[connection_key] = (conn, current_time)
                logger.debug("🔗 Nueva conexión SQL creada para pool")

            return conn

    @classmethod
    def _create_sql_connection(cls):
        """Crea una nueva conexión SQL (sin loggear cada vez)"""
        server = os.getenv('DB_SERVER')
        database = os.getenv('DB_NAME')
        username = os.getenv('DB_USER')
        password = os.getenv('DB_PASSWORD')

        if not all([server, database, username, password]):
            return None

        try:
            connection_string = (
                f'DRIVER={{ODBC Driver 17 for SQL Server}};'
                f'SERVER={server};'
                f'DATABASE={database};'
                f'UID={username};'
                f'PWD={password};'
                f'Connect Timeout=10;'
            )

            conn = pyodbc.connect(connection_string)
            return conn
        except Exception:
            return None

    @classmethod
    def get_as400_connection(cls):
        """Obtiene o crea conexión AS400 reutilizable, UNA POR HILO (ver get_sql_connection)"""
        with cls._lock:
            current_time = datetime.now()
            connection_key = f"as400_{threading.get_ident()}"

            # Reutilizar conexión existente si está disponible y válida
            if connection_key in cls._as400_pool:
                conn, last_used = cls._as400_pool[connection_key]

                # Verificar si la conexión sigue viva (timeout de 5 segundos)
                try:
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1 FROM SYSIBM.SYSDUMMY1")
                    cursor.fetchone()

                    # Actualizar tiempo de uso
                    cls._as400_pool[connection_key] = (conn, current_time)

                    # Log solo la primera vez
                    if not hasattr(cls, '_as400_reused_logged'):
                        logger.debug("♻️ Reutilizando conexión AS400 existente")
                        cls._as400_reused_logged = True

                    return conn
                except:
                    # Conexión muerta, cerrar y crear nueva
                    try:
                        conn.close()
                    except:
                        pass
                    del cls._as400_pool[connection_key]

            # Crear nueva conexión
            conn = cls._create_as400_connection()
            if conn:
                cls._as400_pool[connection_key] = (conn, current_time)
                logger.debug("🔗 Nueva conexión AS400 creada para pool")

            return conn

    @classmethod
    def _create_as400_connection(cls):
        """Crea una nueva conexión AS400"""
        host = os.getenv('AS400_HOST')
        user = os.getenv('AS400_USER')
        password = os.getenv('AS400_PASSWORD')

        if not all([host, user, password]):
            return None

        try:
            conn_str = f"DRIVER={{iSeries Access ODBC Driver}};SYSTEM={host};UID={user};PWD={password};"
            conn = pyodbc.connect(conn_str)
            return conn
        except Exception:
            return None

    @classmethod
    def _cleanup_old_connections(cls):
        """Limpia conexiones inactivas"""
        with cls._lock:
            current_time = datetime.now()

            # Limpiar SQL
            keys_to_remove = []
            for key, (conn, last_used) in cls._sql_pool.items():
                if (current_time - last_used).total_seconds() > cls._max_idle_time:
                    try:
                        conn.close()
                    except:
                        pass
                    keys_to_remove.append(key)

            for key in keys_to_remove:
                del cls._sql_pool[key]

            if keys_to_remove:
                logger.debug(f"🧹 Limpiadas {len(keys_to_remove)} conexiones SQL inactivas")

            # Limpiar AS400
            keys_to_remove = []
            for key, (conn, last_used) in cls._as400_pool.items():
                if (current_time - last_used).total_seconds() > cls._max_idle_time:
                    try:
                        conn.close()
                    except:
                        pass
                    keys_to_remove.append(key)

            for key in keys_to_remove:
                del cls._as400_pool[key]

            if keys_to_remove:
                logger.debug(f"🧹 Limpiadas {len(keys_to_remove)} conexiones AS400 inactivas")

    @classmethod
    def close_all(cls):
        """Cierra todas las conexiones (para shutdown)"""
        with cls._lock:
            for conn, _ in cls._sql_pool.values():
                try:
                    conn.close()
                except:
                    pass

            for conn, _ in cls._as400_pool.values():
                try:
                    conn.close()
                except:
                    pass

            cls._sql_pool.clear()
            cls._as400_pool.clear()
            logger.info("🔒 Todas las conexiones del pool cerradas")

# ═══════════════════════════ CONFIGURACIÓN DE TURNOS ═══════════════════════════

# Variable global para configuración de turnos
SHIFTS_CONFIG = {}

def load_shifts_config() -> Dict[int, Dict]:
    """Carga configuración de turnos desde la base de datos"""
    conn = ConnectionPool.get_sql_connection()
    if conn is None:
        logger.warning("⚠️ No se pudo conectar para cargar turnos")
        return {}

    try:
        cursor = conn.cursor()
        sql = """
        SELECT id, name, start_time, end_time 
        FROM shifts 
        ORDER BY start_time
        """
        cursor.execute(sql)
        rows = cursor.fetchall()

        if not rows:
            logger.error("❌ No se encontraron turnos activos en BD")
            return {}

        shifts = {}
        for shift_id, name, start_time, end_time in rows:
            # Convertir datetime.time de pyodbc a datetime.time de Python
            if isinstance(start_time, str):
                start_time = datetime.strptime(start_time, "%H:%M:%S").time()
            if isinstance(end_time, str):
                end_time = datetime.strptime(end_time, "%H:%M:%S").time()

            shifts[shift_id] = {
                'start': start_time,
                'end': end_time,
                'name': name
            }

        logger.debug(f"Turnos leídos: {len(shifts)}")
        return shifts

    except Exception as e:
        logger.error(f"❌ Error cargando turnos: {e}")
        logger.error(traceback.format_exc())
        return {}
    # NOTA: No cerramos la conexión aquí, el pool la maneja

def _firma_turnos(cfg):
    """Firma comparable de la configuración de turnos."""
    return tuple(sorted(
        (sid, d['start'].strftime('%H:%M'), d['end'].strftime('%H:%M'))
        for sid, d in (cfg or {}).items()
    ))

def refresh_shifts_config():
    """
    Recarga los turnos y actualiza la variable global.

    Corre en cada ciclo del supervisor (60 s) para que un cambio de horario en
    la BD se tome solo, sin botón. Por eso SOLO registra en el log cuando algo
    cambió de verdad: si no, serían ~5 760 líneas diarias sin información.
    """
    global SHIFTS_CONFIG
    new_config = load_shifts_config()

    if new_config:
        cambio = _firma_turnos(new_config) != _firma_turnos(SHIFTS_CONFIG)
        SHIFTS_CONFIG = new_config
        if cambio:
            logger.info(f"🔄 Turnos actualizados desde BD: {len(new_config)} turno(s)")
            for shift_id, data in sorted(new_config.items()):
                logger.info(
                    f"   Turno {shift_id} ({data['name']}): "
                    f"{data['start'].strftime('%H:%M')} - {data['end'].strftime('%H:%M')}"
                )
        return True
    else:
        logger.error("❌ No se pudo actualizar configuración de turnos")
        return False

def get_current_shift(current_time: time) -> Tuple[int, date]:
    """
    Determina el turno y fecha planificada basado en la hora actual.

    Args:
        current_time: Hora actual

    Returns:
        Tuple[turno, fecha_plan]
    """
    # La lógica vive en domain/turnos.py; aquí solo se le pasa el estado del
    # entorno (configuración cargada de la BD y la fecha de hoy).
    return _turnos.get_current_shift(current_time, SHIFTS_CONFIG, date.today())

def has_shift_changed(previous_time: time, current_time: time) -> bool:
    """
    Verifica si hubo un cambio de turno entre dos horas.

    Args:
        previous_time: Hora anterior
        current_time: Hora actual

    Returns:
        True si hubo cambio de turno
    """
    return _turnos.has_shift_changed(previous_time, current_time, SHIFTS_CONFIG)

def safe_get_current_shift(current_time: time) -> Tuple[int, date]:
    """
    Versión segura que maneja cambios en configuración de turnos.

    Args:
        current_time: Hora actual

    Returns:
        Tuple[turno, fecha_plan]
    """
    try:
        return get_current_shift(current_time)
    except Exception as e:
        logger.error(f"❌ Error calculando turno: {e}")
        # Valores por defecto como fallback
        if time(8, 0) <= current_time < time(20, 0):
            return 1, date.today()
        else:
            return 2, date.today() if current_time >= time(20, 0) else date.today() - timedelta(days=1)

def registrar_error_validacion(estacion, numero_original, tipo_error):
    """
    Registra e CSV los números de parte que no pasan la validación.
    Solo registrna UNA VEZ por día (no se repite el mismo número en la misma fecha).
    SIN TIMESTAMP - solo fecha.
    """
    try:
        ts = datetime.now()
        fecha_hoy = ts.strftime('%Y-%m-%d')  # Solo fecha, sin hora

        #  LIMPIEZA EXHAUSTIVA
        numero_original_limpio = str(numero_original)

        # 1. NORMALIZAR ESPACIOS - esto es clave
        numero_original_limpio = ' '.join(numero_original_limpio.split())

        # 2. Limpiar caracteres problemáticos
        numero_original_limpio = numero_original_limpio.replace(',', ';')
        numero_original_limpio = numero_original_limpio.replace('\n', ' ')
        numero_original_limpio = numero_original_limpio.replace('\r', ' ')
        numero_original_limpio = numero_original_limpio.replace('\t', ' ')

        # 3. Limitar y limpiar final
        numero_original_limpio = numero_original_limpio[:100].strip()

        # Limpiar estación
        estacion_limpia = ' '.join(str(estacion).split()).strip()
        tipo_error_limpio = ' '.join(str(tipo_error).split()).strip()

        file_exists = CSV_FILE.exists()

        #  VERIFICACIÓN DE DUPLICADOS (SOLO estacion + numero + fecha)
        if file_exists:
            try:
                # Leer CSV SIN TIMESTAMP
                existing_df = pd.read_csv(
                    CSV_FILE,
                    encoding='utf-8',
                    on_bad_lines='skip',
                    engine='python'
                )

                # Verificar columnas requeridas (sin timestamp)
                required_columns = ['estacion', 'numero_original_plc', 'fecha']
                if all(col in existing_df.columns for col in required_columns):
                    #  LIMPIAR DATOS EXISTENTES
                    existing_df['estacion'] = existing_df['estacion'].astype(str).apply(
                        lambda x: ' '.join(str(x).split()).strip()
                    )
                    existing_df['numero_original_plc'] = existing_df['numero_original_plc'].astype(str).apply(
                        lambda x: ' '.join(str(x).split()).replace(',', ';').strip()[:100]
                    )
                    existing_df['fecha'] = existing_df['fecha'].astype(str).str.strip()

                    #  BUSCAR DUPLICADOS EXACTOS
                    estacion_buscar = estacion_limpia
                    numero_buscar = numero_original_limpio

                    # Buscar coincidencia exacta
                    duplicado_mask = (
                        (existing_df['estacion'] == estacion_buscar) &
                        (existing_df['numero_original_plc'] == numero_buscar) &
                        (existing_df['fecha'] == fecha_hoy)
                    )

                    duplicados = existing_df[duplicado_mask]

                    if not duplicados.empty:
                        logger.info(f"ℹ️ Ya registrado HOY: {estacion_buscar} - {numero_buscar}")
                        return False

                else:
                    logger.warning(f"⚠️ CSV no tiene columnas necesarias")

            except Exception as e:
                logger.warning(f"⚠️ Error verificando duplicados: {e}")
                # Continuar para guardar

        #  CREAR NUEVO REGISTRO SIN TIMESTAMP
        df_nuevo = pd.DataFrame([{
            'estacion': estacion_limpia,
            'numero_original_plc': numero_original_limpio,
            'tipo_error': tipo_error_limpio,
            'fecha': fecha_hoy
            #  NO TIMESTAMP - completamente eliminado
        }])

        #  GUARDAR
        try:
            df_nuevo.to_csv(
                CSV_FILE,
                mode='a',
                header=not file_exists,
                index=False,
                encoding='utf-8'
            )
            logger.info(f"📝 NUEVO registro: {estacion_limpia} - {numero_original_limpio}")
            return True

        except Exception as e:
            logger.error(f"❌ Error guardando CSV: {e}")
            return False

    except Exception as e:
        logger.error(f"❌ Error en registrar_error_validacion: {e}")
        return False

# ═══════════════════════════ HELPERS LOGGING ═══════════════════════════

def get_station_logger(estacion):
    """Obtiene o crea un logger específico para una estación con ROTACIÓN DE LOGS"""
    if estacion in station_loggers:
        return station_loggers[estacion]

    station_logger = logging.getLogger(f"station.{estacion}")
    # CAMBIO: Solo WARNING y ERROR
    station_logger.setLevel(logging.INFO)  # Solo WARNING, ERROR y CRITICAL
    station_logger.propagate = False

    log_path = LOGS_DIR / f"{estacion}.log"

    # 🔄 CAMBIO: Usar RotatingFileHandler en lugar de FileHandler
    # Tamaño máximo: 10 MB por archivo, mantener 5 archivos de backup
    file_handler = RotatingFileHandler(
        log_path,
        mode='a',
        encoding='utf-8',
        maxBytes=100*1024*1024,  # 10 MB
        backupCount=2           # mantener hasta 5 archivos de respaldo
    )
    file_handler.setLevel(logging.INFO)  # Solo WARNING y ERROR
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    station_logger.addHandler(file_handler)

    # ❌ REMOVER handler de consola para estaciones
    # Las estaciones solo deben escribir en sus archivos de log

    station_loggers[estacion] = station_logger
    return station_logger

# ═══════════════════════════ CONEXIONES BD ═══════════════════════════

def create_connection():
    """Crea o reutiliza conexión a SQL Server usando pool"""
    return ConnectionPool.get_sql_connection()

def crear_conexion_as400(host: str = None, user: str = None,
                        password: str = None, database: str = "") -> Optional[pyodbc.Connection]:
    """Crea o reutiliza conexión a AS400 usando pool"""
    return ConnectionPool.get_as400_connection()

# El multiplicador ya no viene de AS400: sale de attributes.pieces_per_shot
# en la misma consulta SQL. Ver persistence/catalogo.py

# ═══════════════════════════ FUNCIONES BASE DE DATOS (COMUNES) ═══════════════════════════

def load_config():
    """Carga configuración incluyendo el NOMBRE DEL ÁREA"""
    conn = create_connection()
    if conn is None:
        logger.warning("⚠️ No se pudo establecer conexión para cargar configuración")
        return {}

    try:
        sql = """
        SELECT
          wc.name          AS work_center_name,
          wc.ip            AS work_center_ip,
          tt.name          AS tag_type_name,
          t.address        AS tag_address,
          t.long           AS tag_long,
          a.name           AS area_name
        FROM work_centers wc
        JOIN tags t        ON wc.id = t.work_center_id
        JOIN tag_types tt  ON t.tag_type_id = tt.id
        LEFT JOIN lines l  ON wc.line_id = l.id
        LEFT JOIN areas a  ON l.area_id = a.id
        """
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()

        if not rows:
            logger.warning("⚠️ No se encontraron configuraciones en la base de datos")
            return {}

        ip_groups = defaultdict(lambda: {
            'estaciones': [], 'port': 1025, 'serie': 'Q',
            'all_addresses': set(), 'station_configs': {}, 'area': 'Default'
        })

        global system_monitor

        for wc, ip, tag, addr, lng, area in rows:
            if not ip or not ip.strip():
                continue
            
            if area:
                ip_groups[ip]['area'] = area

            if wc not in system_monitor['estaciones']:
                system_monitor['estaciones'][wc] = {}

            system_monitor['estaciones'][wc]['area'] = area or 'N/A'
            if 'ip' not in system_monitor['estaciones'][wc]:
                 system_monitor['estaciones'][wc]['ip'] = ip

            tag_lower = tag.lower()
            if tag_lower == "puerto":
                ip_groups[ip]['port'] = int(addr)
            elif tag_lower == "serie plc":
                ip_groups[ip]['serie'] = addr
            else:
                if wc not in ip_groups[ip]['station_configs']:
                    ip_groups[ip]['station_configs'][wc] = {}

                ip_groups[ip]['station_configs'][wc][tag] = {
                    "address": addr,
                    "long": int(lng)
                }
                # NUEVO: Guardar como bloque en lugar de romper direcciones en individuales
                ip_groups[ip]['all_addresses'].add((addr, int(lng)))

            if wc not in ip_groups[ip]['estaciones']:
                ip_groups[ip]['estaciones'].append(wc)

        # 🔄 Solo mostrar log si hay cambios significativos en la configuración
        config_hash = hashlib.md5(str(sorted(ip_groups.items())).encode()).hexdigest()

        if not hasattr(load_config, 'last_config_hash'):
            load_config.last_config_hash = None

        if config_hash != load_config.last_config_hash:
            logger.info(f"📋 Configuración cargada: {len(ip_groups)} IPs, {len(system_monitor['estaciones'])} estaciones")
            if load_config.last_config_hash:
                logger.info("🔄 Configuración actualizada desde BD")
            load_config.last_config_hash = config_hash

        return dict(ip_groups)

    except Exception as e:
        logger.error(f"❌ Error cargando configuración: {e}")
        logger.error(traceback.format_exc())
        return {}
    # NOTA: No cerramos la conexión aquí, el pool la maneja

# El SQL vive en persistence/repositorio.py. Quién consulta el multiplicador lo
# decide el ÁREA: solo Estampado lee pieces_per_shot (antes venía de AS400).
def lector_multiplicador(pipeline):
    """Devuelve la función que el repositorio usará para el multiplicador."""
    if getattr(pipeline, 'usa_multiplicador', False):
        return catalogo.obtener_multiplicador
    return repo._sin_multiplicador

obtener_id_registro_activo = repo.obtener_id_registro_activo
crear_nuevo_registro = repo.crear_nuevo_registro

# ═══════════════════════════ UTILS (STRING & BLOCK) ═══════════════════════════

def expand_block(address: str, length: int) -> list[str]:
    """
    Dado un address tipo "W12A0" o "W120A" y un length n,
    devuelve ["W12A0","W12A1",...,"W12A0+(n-1)"] o ["W120A","W121A",...,"W120+(n-1)A"]
    """
    import re

    match1 = re.match(r'^([A-Z]+)(\d+)([A-F])(\d+)$', address)
    if match1:
        prefix = match1.group(1)
        numbers1 = match1.group(2)
        middle_letter = match1.group(3)
        last_num = int(match1.group(4))
        return [f"{prefix}{numbers1}{middle_letter}{last_num + i}" for i in range(length)]

    match2 = re.match(r'^([A-Z]+)(\d+)([A-F])$', address)
    if match2:
        prefix = match2.group(1)
        numbers = int(match2.group(2))
        suffix_letter = match2.group(3)
        return [f"{prefix}{numbers + i}{suffix_letter}" for i in range(length)]

    match3 = re.match(r'^([A-Z]+)(\d+)$', address)
    if match3:
        prefix = match3.group(1)
        numbers = int(match3.group(2))
        return [f"{prefix}{numbers + i}" for i in range(length)]

    prefix = ''.join(ch for ch in address if not ch.isdigit())
    num_str = ''.join(ch for ch in address if ch.isdigit())
    if num_str:
        num = int(num_str)
        return [f"{prefix}{num + i}" for i in range(length)]
    else:
        return [f"{address}{i}" for i in range(length)]

def decodificar_bloque(bloque):
    """
    Decodifica el bloque del PLC.
    Devuelve directamente el número limpio.
    """
    if not bloque:
        return None, None, {}

    chars = [chr(v & 0xFF) + chr((v >> 8) & 0xFF) for v in bloque]
    original = "".join(chars).replace("\x00", "")
    
    # Filtrar caracteres no imprimibles del PLC
    original = ''.join(c for c in original if c.isprintable()).strip()
    
    # limpia = original.strip()
    limpia = original.replace(' ', '')

    if not limpia:
        return original, [], {}

    return original, [limpia], {}

# procesar_numero_parte vive ahora en domain/partes.py (importado arriba)

# ═══════════════════════════ 🆕 NUEVA FUNCIÓN: VALIDACIÓN ESTAMPADO ═══════════════════════════

# Cache global para almacenar números de parte validados por MDI y estación
# Estructura: {(estacion, mdi): [lista_de_numeros_parte_validados]}
validacion_estampado_cache = {}

def validar_numeros_parte_estampado(mdi: str, estacion: str, log) -> tuple:
    """
    Traduce el MDI del troquel a los números de parte de esa estación.

    Antes consultaba AS400 (LX834F01.IIU) y luego filtraba contra SQL Server.
    Ahora sale todo de una sola consulta: el MDI vive en attributes.[key]='mid'.
    Ver persistence/catalogo.py

    Devuelve (numeros_activos, error). El error va al tablero de rechazos y
    distingue: MDI_NO_EXISTE, MDI_DE_OTRA_ESTACION, PART_NUMBER_OBSOLETO.
    """
    cache_key = (estacion, mdi)
    if cache_key in validacion_estampado_cache:
        return validacion_estampado_cache[cache_key]

    conn = create_connection()
    if conn is None:
        log.error(f"❌ Sin conexión a BD para resolver MDI={mdi}")
        return [], "SIN_CONEXION_BD"

    try:
        with conn.cursor() as cursor:
            numeros, _mults, error = catalogo.resolver_mdi(cursor, mdi, estacion, log)
    except Exception as e:
        log.error(f"❌ Error resolviendo MDI={mdi}: {e}")
        return [], "DB_ERROR"

    resultado = (numeros, error)
    validacion_estampado_cache[cache_key] = resultado
    return resultado

# ═══════════════════════════ PATRONES STRATEGY & FACTORY ═══════════════════════════

def registrar_history(cursor, pipeline, part_number_id, cantidad, fecha_fmt, tiempo, dato, log):
    """
    Inserta el detalle en histories con las columnas extra que decida el área.

    Estampado agrega `sequence` con el troquel; las demás áreas no agregan nada.
    Antes esto vivía en una jerarquía DBStrategy paralela al pipeline de área;
    ahora el área es una sola cosa (ver areas/).
    """
    try:
        repo.insertar_history(cursor, part_number_id, cantidad, fecha_fmt, tiempo,
                              **pipeline.extras_history(dato or {}))
    except Exception as e:
        log.error(f"Error insert history: {e}")

# ═══════════════════════════ RECOLECTOR DINÁMICO ═══════════════════════════

class IPDataCollector:
    def __init__(self, ip, estaciones, area):
        self.ip = ip
        self.estaciones = estaciones
        self.area = area
        # El pipeline del área se resuelve una vez, no en cada lectura.
        self._pipeline = obtener_pipeline(area)

    def _parse_tag(self, tag_name):
        """Detecta tipo y grupo del tag (ej: 'Contador RH' -> 'contador', 'RH')"""
        lower = tag_name.lower()
        upper = tag_name.upper()

        grupo = "GLOBAL"
        if upper.endswith("LH REAR"):
            grupo = "LH REAR"
        elif upper.endswith("RH REAR"):
            grupo = "RH REAR"
        elif upper.endswith("LH"):
            grupo = "LH"
        elif upper.endswith("RH"):
            grupo = "RH"

        tipo = "otro"
        if "contador" in lower: tipo = "contador"
        elif "tiempo" in lower or "ciclo" in lower: tipo = "tiempo"
        elif "parte" in lower or "part" in lower: tipo = "parte"
        elif "troquel" in lower or "die" in lower: tipo = "troquel"

        return tipo, grupo

    def _merge_blocks(self, blocks, max_gap=15):
        """Agrupa bloques contiguos/cercanos para minimizar lecturas al PLC."""
        from collections import defaultdict

        parsed_blocks = defaultdict(list)

        for addr, lng in blocks:
            match = re.match(r'^([A-Za-z]+)([0-9A-Fa-f]+)$', addr)
            if not match:
                parsed_blocks[f"UNKNOWN_{addr}"].append({
                    'start': 0,
                    'len': lng,
                    'orig': (addr, lng)
                })
                continue

            prefix, num_str = match.groups()
            prefix = prefix.upper()
            base = 16 if prefix in ('W', 'B', 'X', 'Y') else 10
            start = int(num_str, base)

            parsed_blocks[f"{prefix}:{base}"].append({
                'start': start,
                'len': lng,
                'orig': (addr, lng)
            })

        merged_requests = []

        for group_key, items in parsed_blocks.items():
            if group_key.startswith('UNKNOWN_'):
                for item in items:
                    merged_requests.append({
                        'head': item['orig'][0],
                        'len': item['orig'][1],
                        'sub_blocks': [{'orig': item['orig'], 'offset': 0}]
                    })
                continue

            items.sort(key=lambda x: x['start'])
            current = None

            for item in items:
                if current is None:
                    current = {
                        'head': item['orig'][0],
                        'start': item['start'],
                        'end': item['start'] + item['len'] - 1,
                        'sub_blocks': [{'orig': item['orig'], 'offset': 0}]
                    }
                    continue

                gap = item['start'] - current['end'] - 1
                new_end = max(current['end'], item['start'] + item['len'] - 1)
                total_len = new_end - current['start'] + 1

                if gap <= max_gap and gap >= 0 and total_len <= 960:
                    current['end'] = new_end
                    current['sub_blocks'].append({
                        'orig': item['orig'],
                        'offset': item['start'] - current['start']
                    })
                elif item['start'] <= current['end']:
                    current['end'] = new_end
                    current['sub_blocks'].append({
                        'orig': item['orig'],
                        'offset': item['start'] - current['start']
                    })
                else:
                    merged_requests.append({
                        'head': current['head'],
                        'len': current['end'] - current['start'] + 1,
                        'sub_blocks': current['sub_blocks']
                    })
                    current = {
                        'head': item['orig'][0],
                        'start': item['start'],
                        'end': item['start'] + item['len'] - 1,
                        'sub_blocks': [{'orig': item['orig'], 'offset': 0}]
                    }

            if current is not None:
                merged_requests.append({
                    'head': current['head'],
                    'len': current['end'] - current['start'] + 1,
                    'sub_blocks': current['sub_blocks']
                })

        return merged_requests

    def _lectura_degradada(self, cfg, block_data):
        """
        True si algún bloque que usa la estación falló en la lectura (quedó en None).
        Permite distinguir "la estación dejó de producir" de "no pude leer la estación":
        en el segundo caso el estado en caché debe conservarse intacto.
        """
        for info in cfg.values():
            key = (info.get('address'), info.get('long'))
            if key in block_data and block_data[key] is None:
                return True
        return False

    def _process_station_data(self, estacion, cfg, block_data, timestamp):
        """Procesa datos de una estación específica obtenida de los bloques"""
        from collections import defaultdict

        groups_data = defaultdict(dict)

        for tag_name, info in cfg.items():
            tipo, grupo = self._parse_tag(tag_name)
            
            addr, lng = info['address'], info['long']
            # block_vals obtiene directamente el bloque leído, llenando con 0 si algo falla
            block_vals = block_data.get((addr, lng))

            if block_vals is None:

                continue

            val = None
            if tipo == "contador":
                val = block_vals[0] if block_vals else 0
            elif tipo == "tiempo":
                try:
                    val = abs(int(block_vals[0])/1000.0)
                except:
                    val = 0.0
            elif tipo == "parte":
                #  SIMPLIFICADO: Decodificar
                orig, limpios, meta = decodificar_bloque(block_vals)
                val = {'orig': orig, 'list': limpios, 'meta': meta}
            elif tipo == "troquel":
                val = block_vals[0] if block_vals else 0

            if val is not None:
                groups_data[grupo][tipo] = val

        # 🆕 NUEVO: Obtener el logger de la estación para usar en validación
        log = get_station_logger(estacion)

        datos_estacion = []
        for grp, data in groups_data.items():
            if 'contador' not in data:
                continue

            partes = data.get('parte', {'orig': '', 'list': [], 'meta': {}})
            troquel_id = data.get('troquel', None)

            # El área decide cómo se traduce lo que manda el PLC a números de
            # parte. Estampado resuelve el MDI contra AS400; las demás expanden
            # las alternativas con '/'. Ver areas/
            raw = partes.get('orig') or ''
            ctx_area = ContextoArea(
                estacion=estacion, log=log,
                validar_estampado=validar_numeros_parte_estampado,
            )
            nombres_parte, error_parte = self._pipeline.resolver_partes(raw, ctx_area)

            if nombres_parte:
                # requiere_validacion_bd: en Estampado el número ya se validó
                # contra AS400 y part_numbers, así que entra como validado.
                validado_flag = None if self._pipeline.requiere_validacion_bd() else True

                if len(nombres_parte) > 1:
                    log.info(
                        f"🔀 {estacion}/{grp} [{self._pipeline.nombre}]: "
                        f"'{raw}' -> {nombres_parte} (contador={data['contador']})"
                    )

                for p_nombre in nombres_parte:
                    datos_estacion.append({
                        'parte': p_nombre,
                        'original': partes['orig'],
                        'contador': data['contador'],
                        'tiempo': data.get('tiempo', 0.0),
                        'troquel_id': troquel_id,
                        'validado': validado_flag,
                        'error_validacion': None,
                        'lado': grp  # 🆕 Identificar el lado/grupo
                    })
            elif raw.strip():
                log.warning(
                    f"❌ {estacion}/{grp} [{self._pipeline.nombre}]: no se resolvió "
                    f"ningún número de parte para '{raw}' ({error_parte})"
                )
                datos_estacion.append({
                    'parte': None,
                    'original': partes['orig'],
                    'contador': data['contador'],
                    'tiempo': data.get('tiempo', 0.0),
                    'troquel_id': troquel_id,
                    'validado': False,
                    'error_validacion': error_parte,
                    'lado': grp  # 🆕 Identificar el lado/grupo
                })

        return datos_estacion

    async def collect_and_enqueue(self, plc, group_info, _blocks=None):
        """Método optimizado con lectura merge-batchread por bloques."""
        try:
            loop = asyncio.get_running_loop()
            blocks = _blocks if _blocks is not None else list(group_info.get('all_addresses', []))
            merged_reqs = self._merge_blocks(blocks, max_gap=15)
            block_data = {}
            for req in merged_reqs:
                head = req['head']
                length = req['len']
                try:
                    # batchread_wordunits es una llamada de socket BLOQUEANTE: se ejecuta
                    # en un hilo para no congelar la lectura del resto de los PLCs.
                    # Las lecturas de un mismo PLC siguen siendo secuenciales (await),
                    # que es lo que exige pymcprotocol.
                    vals = await loop.run_in_executor(
                        _plc_executor,
                        functools.partial(plc.batchread_wordunits, headdevice=head, readsize=length)
                    )
                    if not vals or len(vals) < length:
                        received = len(vals) if vals else 0
                        logger.warning(f"⚠️ batchread_wordunits devolvió {received}/{length} words para '{head}' en PLC {self.ip}.")
                        for sub in req['sub_blocks']:
                            block_data[sub['orig']] = None
                        continue
                    for sub in req['sub_blocks']:
                        orig_addr, orig_len = sub['orig']
                        offset = sub['offset']
                        block_data[sub['orig']] = vals[offset:offset + orig_len]
                except OSError as batch_error:
                    winerr = getattr(batch_error, 'winerror', None) or batch_error.errno
                    if winerr == 10054:
                        logger.warning(f"🔌 WinError 10054 leyendo '{head}' en PLC {self.ip}.")
                    elif winerr == 10061:
                        logger.warning(f"🔌 WinError 10061 leyendo '{head}' en PLC {self.ip}.")
                    else:
                        logger.warning(f"⚠️ OSError [{winerr}] leyendo '{head}' en PLC {self.ip}: {batch_error}")
                    for sub in req['sub_blocks']:
                        block_data[sub['orig']] = None
                    raise
                except Exception as batch_error:
                    logger.warning(f"Error leyendo bloque '{head}' (len {length}) en PLC {self.ip}: {batch_error}")
                    for sub in req['sub_blocks']:
                        block_data[sub['orig']] = None
            batch = []
            now = datetime.now()
            for est in self.estaciones:
                cfg = group_info['station_configs'].get(est, {})
                # plc_ok=False marca lectura incompleta: el consumidor conserva el
                # caché en vez de cerrar registros por ausencia de datos.
                lectura_ok = not self._lectura_degradada(cfg, block_data)
                datos_estacion = self._process_station_data(est, cfg, block_data, now)
                if datos_estacion:
                    batch.append({'estacion': est, 'datos': datos_estacion, 'ts': now, 'area': self.area, 'plc_ok': lectura_ok})
                    if est in system_monitor['estaciones']:
                        if 'lados' not in system_monitor['estaciones'][est]:
                            system_monitor['estaciones'][est]['lados'] = {}
                        for dato in datos_estacion:
                            lado = dato.get('lado', 'GLOBAL')
                            validado_flag = dato.get('validado')
                            error_val = dato.get('error_validacion')
                            if validado_flag is None:
                                prev_lados = system_monitor['estaciones'].get(est, {}).get('lados', {})
                                if lado in prev_lados:
                                    prev_ui = prev_lados[lado]
                                    prev_orig = prev_ui.get('parte_actual', '').replace(' ⏳', '').replace(' ✅', '').replace(' ❌', '')
                                    if prev_orig == dato['original']:
                                        validado_flag = prev_ui.get('validado')
                                        error_val = prev_ui.get('error_validacion')
                            if validado_flag is None:
                                status_validacion = " ⏳"
                            elif validado_flag is True:
                                status_validacion = " ✅"
                            else:
                                status_validacion = " ❌"
                            system_monitor['estaciones'][est]['lados'][lado] = {'parte_actual': dato['original'] + status_validacion, 'contador': dato['contador'], 'tiempo_ciclo': dato['tiempo'], 'validado': validado_flag, 'error_validacion': error_val}
                        system_monitor['estaciones'][est].update({'ultima_actualizacion': now, 'ip': self.ip})
                else:
                    if not lectura_ok:
                        logger.warning(
                            f"⚠️ Lectura incompleta de {est} en PLC {self.ip}: "
                            f"se preserva el estado en caché (no se cierran registros)."
                        )
                    batch.append({'estacion': est, 'datos': [], 'ts': now, 'area': self.area, 'plc_ok': lectura_ok})
            if batch and self.ip in ip_data_queues:
                try:
                    ip_data_queues[self.ip].put_nowait(batch)
                except asyncio.QueueFull:
                    logger.warning(f"Cola llena para {self.ip}, descartando batch")
            if self.ip in system_monitor['ips']:
                system_monitor['ips'][self.ip].update({'conectado': True, 'ultima_lectura': now})
        except OSError:
            raise  # Ya fue logueado en el bloque interno, evitar log duplicado
        except Exception as e:
            logger.error(f"Error en collect_and_enqueue para {self.ip}: {str(e)[:100]}")
            raise
# ═══════════════════════════ PROCESADOR (CONSUMIDOR) ═══════════════════════════

class IPDataProcessor:
    def __init__(self, ip):
        self.ip = ip
        self.active_records = {}
        self.last_scanned_parts = {}  # 🆕 Polling optimization cache
        self.state_file = STATE_DIR / f"state_{self.ip.replace('.', '_')}.json"
        self.load_state()

    def load_state(self):
        """Carga el estado previo. La lectura vive en persistence/estado.py"""
        self.active_records = estado_store.cargar_estado(self.state_file, self.ip)

    def save_state(self):
        """Guarda el estado. La escritura atómica vive en persistence/estado.py"""
        estado_store.guardar_estado(self.state_file, self.active_records, self.ip)

    def _ensure_active_record(self, cursor, estacion, fecha_plan, turno, num, num_orig, cnt, log, fecha_fmt, pipeline, contador_previo=None, lado='--'):
        """
        Garantiza que exista un registro activo (Status 7) para la estación y parte.
        Modelo incremental: la BD es la dueña del acumulado (produced_quantity);
        aquí solo se fija la línea base del contador (contador_registro = cnt)
        desde donde se calcularán los deltas. No se reconstruyen offsets ni
        corridas: lo ya producido permanece intacto en BD.
        - contador_previo: en cambio de turno, valor del contador al cierre del
          turno anterior; el avance (cnt - contador_previo) es la producción
          inicial del nuevo registro.
        """
        leer_mult = lector_multiplicador(pipeline)

        id_reg, q_plan, q_prod, status, prod_start_db, mult = obtener_id_registro_activo(
            cursor, estacion, fecha_plan, turno, num, log, leer_mult
        )

        mult = mult or 1

        # Casos de Retorno (Diccionario de estado)
        reg_state = {
            'id_registro': None,
            'quantity_planeada': 0,
            'multiplicador': mult,
            'contador_registro': cnt,  # Línea base: los deltas se cuentan desde aquí
            'hora_cambio': datetime.now().time().replace(microsecond=0),
            'numero_original': num_orig,
            'lado': lado,  # 🆕 Persistir el lado
            'necesita_production_start': False,
            'error_bd': None,  # 🆕 Para propagar el error de BD
            'registro_creado': False,  # True solo si el registro se insertó en esta llamada
            'delta_inicial': 0  # Golpes del primer tramo (solo aplica a registros creados)
        }

        # CASO 1: CREAR NUEVO
        if id_reg is None:
            if contador_previo is not None:
                # Cambio de turno: la producción inicial es el avance desde el
                # contador con que cerró el turno anterior.
                delta_inicial, fue_negativo = calcular_delta_turno(contador_previo, cnt)
                if fue_negativo:
                    log.warning(
                        f"⚠️ Cambio de turno con delta negativo detectado en {estacion}/{num}/{lado}: "
                        f"cnt_actual={cnt}, contador_previo_turno={contador_previo}, mult={mult}. "
                        f"Se fuerza qty_inicial=0 para evitar negativos en production_records."
                    )
                qty_inicial = piezas_producidas(delta_inicial, mult)

                log.info(
                    f"🕒 Nuevo registro por cambio de turno en {estacion}/{num}/{lado}: "
                    f"cnt_actual={cnt}, contador_previo={contador_previo}, delta_inicial={delta_inicial}, "
                    f"qty_inicial={qty_inicial}, turno={turno}"
                )
            else:
                # Parte nueva: el contador actual del PLC pertenece a esta corrida.
                if mult == 1:
                    mult = leer_mult(cursor, num, estacion, log) or 1
                delta_inicial = cnt
                qty_inicial = piezas_producidas(cnt, mult)

            id_reg, q_plan, q_prod, mult_new, error_bd = crear_nuevo_registro(
                cursor, num, estacion, qty_inicial, turno, fecha_fmt, fecha_plan, num_orig, log,
                leer_mult
            )

            if id_reg is None:
                reg_state['error_bd'] = error_bd
                return reg_state

            reg_state.update({
                'id_registro': id_reg,
                'quantity_planeada': q_plan,
                'multiplicador': mult_new or mult,
                'necesita_production_start': False,  # ✅ Ya tiene start del INSERT
                'registro_creado': True,
                'delta_inicial': delta_inicial
            })
            return reg_state

        # CASO 2: REACTIVAR (Status 8) — mismo registro, mismo acumulado.
        # Lo producido antes sigue en BD; lo nuevo se sumará como delta a partir
        # del contador actual del PLC (haya reset o no).
        if status == 8:
            try:
                repo.reactivar_registro(cursor, id_reg)
                log.info(
                    f"✅ Registro {id_reg} reactivado (status 8 → 7). "
                    f"Acumulado BD preservado: {q_prod or 0}. Conteo continúa desde cnt={cnt}."
                )
            except Exception as e:
                log.error(f"❌ Error reactivando registro {id_reg}: {e}")

            reg_state.update({
                'id_registro': id_reg,
                'quantity_planeada': q_plan,
                'multiplicador': mult,
                'necesita_production_start': False
            })
            return reg_state

        # CASO 3: ACTIVO
        if prod_start_db is None and status == 3:
            reg_state['necesita_production_start'] = True

        reg_state.update({
            'id_registro': id_reg,
            'quantity_planeada': q_plan,
            'multiplicador': mult,
        })
        return reg_state

    async def process_continuously(self):
        if self.ip not in ip_data_queues: return
        loop = asyncio.get_running_loop()
        while True:
            try:
                batch = await ip_data_queues[self.ip].get()

                # Todo el batch corre en UN hilo del pool de BD.
                # Antes se lanzaban corrutinas "concurrentes" con un semáforo, pero
                # como ninguna cedía el control (pyodbc es bloqueante) corrían en
                # serie de todos modos, congelando el event loop mientras tanto.
                # Un hilo por batch mantiene el loop libre y deja active_records
                # bajo un solo hilo a la vez, sin necesidad de locks.
                await loop.run_in_executor(_db_executor, self._process_batch, batch)

                ip_data_queues[self.ip].task_done()

            except asyncio.CancelledError:
                logger.info(f"🛑 Procesador de {self.ip} detenido")
                raise
            except RuntimeError as e:
                # El intérprete está apagando los executors (cierre de la app):
                # seguir intentando solo genera ruido y nunca va a funcionar.
                if 'shutdown' in str(e).lower():
                    logger.info(f"🛑 Procesador de {self.ip} detenido: el pool de hilos se cerró")
                    return
                logger.error(f"Error en loop de procesamiento para {self.ip}: {e}")
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.error(f"Error en loop de procesamiento para {self.ip}: {e}")
                await asyncio.sleep(0.5)

    def _process_batch(self, batch):
        """Procesa secuencialmente las estaciones de un batch. Corre en hilo de BD."""
        hubo_cambios = False
        for pkg in batch:
            try:
                if self._process_estacion(pkg):
                    hubo_cambios = True
            except Exception as e:
                est = pkg.get('estacion', '?')
                logger.error(f"Error procesando {est} en {self.ip}: {e}")
                logger.error(traceback.format_exc())

        # Un solo guardado por batch. Antes se escribía el archivo completo de la IP
        # una vez POR ESTACIÓN, así que con muchas estaciones se reescribía decenas
        # de veces por ciclo; ahora es una sola vez, lo que paga el costo del fsync.
        if hubo_cambios:
            self.save_state()

    def _process_estacion(self, pkg):
        estacion = pkg['estacion']
        datos = pkg['datos']
        area = pkg.get('area', 'Default')
        plc_ok = pkg.get('plc_ok', True)
        now = pkg['ts']

        log = get_station_logger(estacion)

        # ✅ Usar pool de conexiones
        conn = create_connection()
        if conn is None:
            log.error("❌ No se pudo obtener conexión a BD para procesar estación")
            return

        pipeline = obtener_pipeline(area)

        state_changed = False

        try:
            with conn.cursor() as cursor:
                hora = now.time().replace(microsecond=0)

                # Usar versión segura para obtener turno
                turno, fecha_plan = safe_get_current_shift(hora)

                # Registrar para diagnóstico (opcional)
                if not SHIFTS_CONFIG:
                    log.warning(f"⚠️ Usando valores por defecto para turnos en {estacion}")

                fecha_fmt = now.strftime('%Y-%m-%d %H:%M:%S')

                if not datos:
                    if not plc_ok:
                        # Lectura incompleta/fallida: NO se puede concluir que la estación
                        # dejó de producir. Se conserva el caché (línea base de contadores)
                        # para que al reconectar el delta recupere lo producido en el hueco.
                        log.warning(
                            f"⚠️ Lectura incompleta del PLC para {estacion}. "
                            f"Manteniendo estado en caché sin cambios."
                        )
                        return

                    # Lectura buena y sin partes: la estación sí dejó de producir.
                    repo.cerrar_registros_de_estacion(cursor, estacion, fecha_plan, turno, fecha_fmt)

                    keys_to_delete = [k for k in self.active_records if k.startswith(f"{estacion}_")]
                    for k in keys_to_delete:
                        del self.active_records[k]
                        state_changed = True

                    conn.commit()
                    return state_changed

                claves_actuales_en_plc = set()
                for d in datos:
                    if d['parte']:  # Solo agregar si tiene parte válida
                        # 🆕 CLAVE ÚNICA POR LADO: estacion_parte_lado
                        lado = d.get('lado', '--')
                        claves_actuales_en_plc.add(f"{estacion}_{d['parte']}_{lado}")

                # Cierre por ausencia SOLO con lectura completa: en una lectura parcial
                # (p.ej. falló el bloque de un lado) la ausencia de una parte no significa
                # que dejó de producirse, y cerrarla borraría su línea base de contador.
                if plc_ok:
                    claves_obsoletas = []
                    for k in self.active_records:
                        # 🆕 Verificar prefijo y ausencia en claves actuales
                        if k.startswith(f"{estacion}_") and k not in claves_actuales_en_plc:
                            claves_obsoletas.append(k)

                    for k in claves_obsoletas:
                        record_id = self.active_records[k].get('id_registro')
                        if record_id:
                             try:
                                repo.cerrar_registro(cursor, record_id, fecha_fmt)
                             except Exception as e:
                                log.error(f"Error cerrando registro obsoleto {record_id}: {e}")

                        del self.active_records[k]
                        state_changed = True
                else:
                    log.warning(
                        f"⚠️ Lectura parcial del PLC en {estacion}: se omite el cierre de "
                        f"registros ausentes para no perder su contador base."
                    )

                for d in datos:
                    num = d['parte']
                    num_orig = d['original']
                    cnt = d['contador']
                    tiempo = d['tiempo']
                    troquel_id = d.get('troquel_id')
                    
                    lado_actual = d.get('lado', '--')
                    cache_key = f"{estacion}_{num_orig}_{num}_{lado_actual}"
                    current_state = {"parte_original": num_orig, "contador": cnt}
                    previous_state = self.last_scanned_parts.get(cache_key)

                    # 🚀 OPTIMIZACIÓN DE POLLEO: Si la pieza y contador son idénticos al milisegundo anterior, saltamos validación SQL
                    if previous_state and previous_state["parte_original"] == num_orig and previous_state["contador"] == cnt:
                        # El contador está congelado, pero si ya cruzamos la frontera de
                        # turno hay que CERRAR el registro del turno anterior: de lo
                        # contrario se queda abierto indefinidamente en el turno que ya pasó.
                        # El registro del turno nuevo NO se crea aquí a propósito: nacerá
                        # cuando haya producción real, porque hora_cambio no se actualiza
                        # y el bloque de cambio de turno volverá a dispararse entonces.
                        _reg_frio = self.active_records.get(f"{estacion}_{num}_{lado_actual}")
                        if (_reg_frio and _reg_frio.get('id_registro')
                                and not _reg_frio.get('cerrado_por_turno')
                                and has_shift_changed(_reg_frio['hora_cambio'], hora)):
                            try:
                                repo.cerrar_registro(cursor, _reg_frio['id_registro'], fecha_fmt)
                                _reg_frio['cerrado_por_turno'] = True
                                conn.commit()
                                state_changed = True
                                log.info(
                                    f"🕒 Registro {_reg_frio['id_registro']} cerrado en el cambio de turno "
                                    f"({estacion}/{num}/{lado_actual}) con el contador detenido en {cnt}. "
                                    f"El registro del turno nuevo se creará cuando haya producción."
                                )
                            except Exception as e:
                                log.error(f"Error cerrando registro por turno con contador detenido: {e}")
                        continue  # El UI ya preservó el estado visual, evitamos saturar SQL Server y CSVs

                    # Si es nuevo o ha cambiado, actualizamos nuestro caché antes del procesamiento pesado
                    self.last_scanned_parts[cache_key] = current_state
                    validado = d.get('validado')
                    error_val = d.get('error_validacion', None)

                    if validado is False:
                        log.warning(f"⚠️ Número de parte NO VALIDADO (previamente): {num_orig} - Error: {error_val}")

                        #  CRÍTICO: Registrar en CSV ANTES de hacer continue
                        if error_val:
                            log.info(f"📝 Registrando error en CSV: estacion={estacion}, num_orig={num_orig}, error={error_val}")
                            registrar_error_validacion(estacion, num_orig, error_val)
                            
                        # El UI ya fue actualizado a ❌ en collect_and_enqueue por Estampado

                        continue

                    clave = f"{estacion}_{num}_{d.get('lado', '--')}"  # 🆕 CLAVE POR LADO

                    if clave not in self.active_records:
                        new_record = self._ensure_active_record(cursor, estacion, fecha_plan, turno, num, num_orig, cnt, log, fecha_fmt, pipeline, lado=d.get('lado', '--'))
                        
                        if new_record and new_record.get('error_bd'):
                            error_bd = new_record['error_bd']
                            log.warning(f"⚠️ Número de parte RECHAZADO EN BD: {num_orig} - Error: {error_bd}")
                            registrar_error_validacion(estacion, num_orig, error_bd)
                            
                            lado_actual = d.get('lado', 'GLOBAL')
                            if estacion in system_monitor['estaciones'] and 'lados' in system_monitor['estaciones'][estacion]:
                                if lado_actual in system_monitor['estaciones'][estacion]['lados']:
                                    system_monitor['estaciones'][estacion]['lados'][lado_actual]['parte_actual'] = f"{num_orig} ❌"
                                    system_monitor['estaciones'][estacion]['lados'][lado_actual]['validado'] = False
                            continue
                        
                        if not new_record or new_record.get('id_registro') is None:
                             continue

                        # History inicial: SOLO cuando el registro se CREÓ en esta pasada.
                        # El primer tramo (0 → cnt) nunca llega al bloque "if cnt != prev"
                        # porque contador_registro se inicializa igual a cnt.
                        # En recuperaciones/reactivaciones NO se inserta nada: esos golpes
                        # ya están en histories y volver a insertarlos los duplicaría.
                        if new_record.get('registro_creado'):
                            _delta_inicial = new_record.get('delta_inicial', 0)
                            if _delta_inicial > 0:
                                pid_nuevo = obtener_part_number_id(cursor, num, estacion)
                                if pid_nuevo:
                                    extras_ini = {'troquel_id': troquel_id}
                                    try:
                                        registrar_history(
                                            cursor, pipeline, pid_nuevo, _delta_inicial,
                                            fecha_fmt, d.get('tiempo', 0.0), extras_ini, log
                                        )
                                        log.info(
                                            f"📝 History inicial registrado al crear nuevo registro {num}: "
                                            f"delta={_delta_inicial}, turno={turno}"
                                        )
                                    except Exception as _e_h:
                                        log.error(f"❌ Error insertando history inicial para {num}: {_e_h}")

                        # Si llegamos aquí, el registro se creó bien, la parte SÍ es válida en DB
                        lado_actual = d.get('lado', 'GLOBAL')
                        if estacion in system_monitor['estaciones'] and 'lados' in system_monitor['estaciones'][estacion]:
                            if lado_actual in system_monitor['estaciones'][estacion]['lados']:
                                system_monitor['estaciones'][estacion]['lados'][lado_actual]['parte_actual'] = f"{num_orig} ✅"
                                system_monitor['estaciones'][estacion]['lados'][lado_actual]['validado'] = True
                             
                        self.active_records[clave] = new_record
                        state_changed = True

                    reg = self.active_records[clave]
                    if reg['id_registro'] is None:
                        continue

                    #  CAMBIO: Detectar cambio de turno usando hora_cambio
                    cambio_turno = has_shift_changed(reg["hora_cambio"], hora)

                    if cambio_turno:
                        old_id = reg['id_registro']
                        prev_counter = reg.get('contador_registro', cnt)

                        log.warning(
                            f"🕒 Cambio de turno detectado en {estacion}/{num}/{d.get('lado', '--')}: "
                            f"hora_anterior={reg.get('hora_cambio')}, hora_actual={hora}, "
                            f"contador_previo={prev_counter}, contador_actual={cnt}, old_id={old_id}"
                        )

                        try:
                            repo.cerrar_registro(cursor, old_id, fecha_fmt)
                            log.info(f"✅ Registro anterior cerrado por cambio de turno: id={old_id}, production_end={fecha_fmt}")
                        except Exception as e:
                            log.error(f"Error cerrando registro {old_id}: {e}")

                        # Recalcular turno
                        if not SHIFTS_CONFIG:
                            if time(8, 0) <= hora < time(20, 0): turno = 1
                            elif hora >= time(20, 0): turno = 2
                            else: turno = 2
                        else:
                            turno, _ = safe_get_current_shift(hora)

                        log.info(
                            f"🔄 Preparando nuevo registro por turno para {estacion}/{num}/{d.get('lado', '--')}: "
                            f"turno_nuevo={turno}, fecha_plan={fecha_plan}, contador_previo={prev_counter}, cnt_actual={cnt}"
                        )

                        # Crear/obtener el registro del NUEVO turno.
                        new_reg_data = self._ensure_active_record(
                            cursor, estacion, fecha_plan, turno, num, num_orig, cnt, log, fecha_fmt,
                            pipeline, contador_previo=prev_counter,
                            lado=d.get('lado', '--')
                        )

                        if new_reg_data and new_reg_data.get('id_registro'):
                            reg.update(new_reg_data)
                            # Ya hay registro del turno nuevo: se limpia la marca de
                            # "cerrado con el contador detenido" para el siguiente turno.
                            reg.pop('cerrado_por_turno', None)
                            log.info(
                                f"✅ Nuevo estado tras cambio de turno en {estacion}/{num}/{d.get('lado', '--')}: "
                                f"nuevo_id={reg.get('id_registro')}, "
                                f"contador_registro={reg.get('contador_registro')}"
                            )

                            # Avance de este lado desde el cierre del turno anterior.
                            # Si el registro se creó en esta llamada, qty_inicial ya lo
                            # incluye; si ya existía (p.ej. lo creó el otro lado o se
                            # reactivó), se suma como delta para no perderlo ni duplicar.
                            delta_turno = max(cnt - prev_counter, 0)
                            if delta_turno > 0:
                                if not new_reg_data.get('registro_creado'):
                                    _necesita_start_ct = reg.get('necesita_production_start', False)
                                    actualizar_registro(
                                        cursor,
                                        delta_turno * reg.get('multiplicador', 1),
                                        fecha_fmt,
                                        reg['id_registro'],
                                        7,
                                        log,
                                        necesita_start=_necesita_start_ct
                                    )
                                    if _necesita_start_ct:
                                        reg['necesita_production_start'] = False

                                _pid_ct = obtener_part_number_id(cursor, num, estacion)
                                if _pid_ct:
                                    _extras_ct = {'troquel_id': troquel_id}
                                    try:
                                        registrar_history(
                                            cursor, pipeline, _pid_ct, delta_turno,
                                            fecha_fmt, d.get('tiempo', 0.0), _extras_ct, log
                                        )
                                        log.info(
                                            f"📝 History cambio de turno: {num} "
                                            f"delta={delta_turno}, turno={turno}"
                                        )
                                    except Exception as _eh_ct:
                                        log.error(f"❌ Error history cambio turno {num}: {_eh_ct}")
                            state_changed = True
                        else:
                            # No se pudo crear/obtener el registro del turno nuevo.
                            # NO se toca 'reg': conserva el id del turno anterior (ya
                            # cerrado) y escribir ahí reabriría el turno equivocado.
                            # Se omite esta parte en este ciclo; como hora_cambio no se
                            # actualiza, el cambio de turno se reintenta en la siguiente
                            # lectura sin perder el contador base.
                            log.error(
                                f"❌ No se pudo abrir registro del turno {turno} para "
                                f"{estacion}/{num}/{d.get('lado', '--')}: se reintentará "
                                f"en la siguiente lectura (contador base={prev_counter} preservado)."
                            )
                            continue

                    prev = reg.get("contador_registro", cnt)

                    if cnt != prev:
                        multiplicador = reg.get("multiplicador", 1)

                        # 🔍 DIAGNÓSTICO: Log de cambio de contador
                        log.debug(f"📊 Cambio contador en {num}: {prev} → {cnt}")

                        # MODELO INCREMENTAL: solo se calcula el delta de golpes desde
                        # la última lectura; la BD acumula (produced_quantity += delta).
                        incremento_ciclo, hubo_reset = calcular_incremento(prev, cnt)
                        if hubo_reset:
                            log.warning(
                                f"⚠️ Reset detectado en {num}: {prev} -> {cnt}. "
                                f"Acumulado en BD intacto; se suma el contador nuevo como delta."
                            )

                        delta_produccion = piezas_producidas(incremento_ciclo, multiplicador)

                        necesita_start = reg.get('necesita_production_start', False)

                        actualizar_registro(
                            cursor,
                            delta_produccion,
                            fecha_fmt,
                            reg['id_registro'],
                            7,
                            log,
                            necesita_start=necesita_start
                        )

                        pid = obtener_part_number_id(cursor, num, estacion)
                        if pid:
                            extras = {'troquel_id': troquel_id}
                            # En histories se guarda el incremento de golpes sin multiplicar
                            # (mismo comportamiento para estampado y área general)
                            if incremento_ciclo > 0:
                                registrar_history(cursor, pipeline, pid, incremento_ciclo,
                                                  fecha_fmt, tiempo, extras, log)

                        if necesita_start:
                            reg['necesita_production_start'] = False

                        reg['contador_registro'] = cnt
                        reg['hora_cambio'] = hora
                        state_changed = True

                conn.commit()
                return state_changed

        except Exception as e:
            log.error(f"❌ Error procesando {estacion}: {e}")
            log.error(traceback.format_exc())
        # NOTA: No cerramos la conexión aquí, el pool la maneja
        # El guardado del estado lo hace _process_batch, una vez por batch.
        return False

# ═══════════════════════════ ASYNCIO & MAIN LOOPS ═══════════════════════════

ip_data_queues = {}
ip_processors = {}
system_monitor = {'ips': {}, 'estaciones': {}, 'estadisticas': {'errores_conexion': 0, 'inicio_sistema': datetime.now()}}

#  NUEVO: Función para conectar al PLC con timeout
async def connect_plc_with_timeout(plc, ip, port, timeout=PLC_CONNECTION_TIMEOUT):
    """Conecta al PLC con timeout controlado"""
    # FIX WinError 10061: cerrar socket previo ANTES de reconectar.
    # Un socket en estado TIME_WAIT/CLOSE_WAIT hace que el OS rechace
    # la nueva conexión con "Connection Refused" (10061).
    try:
        plc.close()
    except Exception:
        pass

    try:
        # Usar run_in_executor para evitar bloquear el event loop
        loop = asyncio.get_event_loop()
        await asyncio.wait_for(
            loop.run_in_executor(None, plc.connect, ip, port),
            timeout=timeout
        )
        return True
    except asyncio.TimeoutError:
        logger.warning(f"⏱️ Timeout al conectar con PLC {ip}:{port}")
        return False
    except OSError as e:
        # FIX: captura específica para errores de socket Windows
        winerr = getattr(e, 'winerror', None) or e.errno
        if winerr == 10061:
            logger.warning(f"🔌 WinError 10061 – PLC {ip}:{port} rechazó la conexión (Connection Refused). Reintentando en {RECONNECT_DELAY}s...")
        elif winerr == 10054:
            logger.warning(f"🔌 WinError 10054 – PLC {ip}:{port} cerró la conexión (Connection Reset). Reintentando en {RECONNECT_DELAY}s...")
        else:
            logger.warning(f"⚠️ OSError [{winerr}] conectando con PLC {ip}:{port}: {str(e)[:100]}")
        return False
    except Exception as e:
        logger.warning(f"⚠️ Error conectando con PLC {ip}:{port}: {str(e)[:100]}")
        return False

async def plc_reader(ip, port, group_info):
    """
    Lector del PLC.

    `group_info` es el dict VIVO que mantiene el supervisor: cuando cambia la
    configuración de esta IP, el supervisor lo actualiza en su lugar y sube
    '_version'. Aquí se detecta y se reconstruye el colector sin cerrar la
    conexión al PLC, así que las estaciones de las demás IPs ni se enteran.
    """

    version_config = group_info.get('_version', 0)
    collector = IPDataCollector(ip, group_info['estaciones'], group_info.get('area', 'Default'))

    if ip not in ip_data_queues:
        ip_data_queues[ip] = asyncio.Queue(maxsize=1000)

    #  NUEVO: Configuración inicial del PLC
    def create_plc_instance():
        """Crea una nueva instancia del PLC"""
        plc = Type3E()
        plc.network = 0
        plc.pc = 0xFF
        plc.timer = PLC_READ_TIMEOUT
        try:
            plc.soc_timeout = PLC_READ_TIMEOUT
        except:
            pass
        return plc

    plc = create_plc_instance()
    connected = False
    consecutive_failures = 0
    max_consecutive_failures = 3

    #  NUEVO: Intervalo de lectura más inteligente
    last_read_time = datetime.now()
    read_interval = 1.0  # Leer cada 1 segundo

    #  NUEVO: Direcciones para lectura
    addrs = list(group_info['all_addresses']) if 'all_addresses' in group_info else []

    while True:
        try:
            # ¿Cambió la configuración de esta IP desde el último ciclo?
            if group_info.get('_version', 0) != version_config:
                version_config = group_info.get('_version', 0)
                collector = IPDataCollector(
                    ip, group_info['estaciones'], group_info.get('area', 'Default')
                )
                addrs = list(group_info.get('all_addresses', []))

                nuevo_puerto = group_info.get('port', port)
                if nuevo_puerto != port:
                    # El puerto sí obliga a reconectar; los tags no.
                    logger.info(f"🔌 {ip}: el puerto cambió {port} → {nuevo_puerto}, reconectando")
                    port = nuevo_puerto
                    try: plc.close()
                    except Exception: pass
                    connected = False
                    plc = create_plc_instance()
                else:
                    logger.info(
                        f"♻️ {ip}: configuración recargada (v{version_config}) "
                        f"sin cerrar la conexión — {len(addrs)} bloque(s), "
                        f"{len(group_info['estaciones'])} estación(es)"
                    )

            if not connected:
                logger.info(f"🔌 Intentando conectar a PLC {ip}:{port}")

                # Intentar conexión con timeout
                connected = await connect_plc_with_timeout(plc, ip, port)

                if connected:
                    consecutive_failures = 0
                    logger.info(f"✅ Conectado a PLC {ip}:{port}")

                    # Actualizar monitor
                    system_monitor['ips'][ip] = {
                        'conectado': True,
                        'estaciones': group_info['estaciones'],
                        'procesando': False,
                        'ultima_lectura': datetime.now(),
                        'conexion_establecida': datetime.now()
                    }

                    # Notificar a las estaciones
                    for est in group_info['estaciones']:
                        get_station_logger(est).info(f"✅ PLC {ip} conectado")
                else:
                    consecutive_failures += 1
                    if consecutive_failures >= max_consecutive_failures:
                        logger.error(f"❌ Múltiples fallos de conexión con PLC {ip}, esperando {RECONNECT_DELAY}s")
                        await asyncio.sleep(RECONNECT_DELAY)
                    else:
                        await asyncio.sleep(2)  # Espera corta entre intentos
                    continue

            #  LECTURA DEL PLC
            current_time = datetime.now()
            time_since_last_read = (current_time - last_read_time).total_seconds()

            if time_since_last_read >= read_interval:
                try:
                    # Leer datos
                    await collector.collect_and_enqueue(plc, group_info)
                    last_read_time = current_time

                except OSError as read_error:
                    winerr = getattr(read_error, 'winerror', None) or read_error.errno
                    err_str = str(read_error)

                    # Timeout reconectar pero con delay corto
                    if "timed out" in err_str and winerr is None:
                        logger.warning(f"⏱️ Timeout leyendo PLC {ip} — cerrando socket y reconectando (delay corto)...")
                        try: plc.close()
                        except Exception: pass
                        connected = False
                        plc = create_plc_instance()
                        await asyncio.sleep(2)
                        

                    # WinError reales: requieren cierre de socket y reconexión completa
                    elif winerr == 10054:
                        logger.warning(f"🔌 WinError 10054 – PLC {ip} cerró la conexión durante lectura (Connection Reset by Peer). Reconectando...")
                        try: plc.close()
                        except Exception: pass
                        connected = False
                        plc = create_plc_instance()
                        await asyncio.sleep(RECONNECT_DELAY)
                    elif winerr == 10061:
                        logger.warning(f"🔌 WinError 10061 – PLC {ip} rechazó la lectura (Connection Refused). Reconectando...")
                        try: plc.close()
                        except Exception: pass
                        connected = False
                        plc = create_plc_instance()
                        await asyncio.sleep(RECONNECT_DELAY)
                    else:
                        logger.error(f"⚠️ OSError [{winerr}] en lectura PLC {ip}: {err_str[:120]}")
                        try: plc.close()
                        except Exception: pass
                        connected = False
                        plc = create_plc_instance()
                        await asyncio.sleep(RECONNECT_DELAY)
                except Exception as read_error:
                    logger.error(f"❌ Error en lectura PLC {ip}: {str(read_error)[:100]}")
                    try:
                        plc.close()
                    except Exception:
                        pass
                    connected = False
                    plc = create_plc_instance()  # Recrear instancia
                    await asyncio.sleep(1)

            #  ESPERA INTELIGENTE
            time_to_wait = max(0, read_interval - (datetime.now() - current_time).total_seconds())
            if time_to_wait > 0:
                await asyncio.sleep(time_to_wait)

        except Exception as e:
            logger.error(f"❌ Error crítico en plc_reader para {ip}: {str(e)[:200]}")
            connected = False
            plc = create_plc_instance()
            await asyncio.sleep(RECONNECT_DELAY)

def _huella_config(group_info):
    """
    Firma del contenido relevante de una IP. Si cambia, hay que recargar.
    Se ignora '_version' para no compararse consigo misma.
    """
    relevante = {
        'port': group_info.get('port'),
        'serie': group_info.get('serie'),
        'area': group_info.get('area'),
        'estaciones': sorted(group_info.get('estaciones', [])),
        'all_addresses': sorted(map(str, group_info.get('all_addresses', []))),
        'station_configs': {
            est: sorted((t, str(v.get('address')), v.get('long'))
                        for t, v in cfg.items())
            for est, cfg in sorted(group_info.get('station_configs', {}).items())
        },
    }
    return hashlib.md5(str(relevante).encode()).hexdigest()


async def supervisor():
    tasks = {}
    configs_vivas = {}   # ip -> group_info mutable compartido con su lector
    last_successful_config = {}
    config_failures = 0
    max_config_failures = 5
    last_status_log = datetime.now()

    loop = asyncio.get_running_loop()

    # 🔄 Cargar turnos al inicio (consulta bloqueante → hilo)
    await loop.run_in_executor(_db_executor, refresh_shifts_config)

    # Dejar asentado en qué base se está escribiendo: al correr en paralelo
    # servidor/local es lo que evita confundir pruebas con producción.
    logger.info(f"💾 Escribiendo en {os.getenv('DB_SERVER')}/{os.getenv('DB_NAME')}")

    logger.info("🚀 Supervisor iniciado")

    while True:
        try:
            # load_config consulta la BD: también fuera del event loop.
            # Los turnos se recargan en cada ciclo: si alguien cambia un horario
            # en la BD, se toma solo, sin botón y sin reiniciar el servicio.
            config = await loop.run_in_executor(_db_executor, load_config)
            await loop.run_in_executor(_db_executor, refresh_shifts_config)

            if not config:
                config_failures += 1
                if config_failures >= max_config_failures:
                    logger.warning(f"⚠️ {config_failures} fallos consecutivos cargando config. Reintentando en {POLL_INTERVAL}s...")
                    if last_successful_config:
                        # Solo mostrar cada 60 segundos para no saturar logs
                        current_time = datetime.now()
                        if (current_time - last_status_log).total_seconds() > 60:
                            logger.info("📋 Usando última configuración válida")
                            last_status_log = current_time
                        config = last_successful_config
                    else:
                        await asyncio.sleep(POLL_INTERVAL)
                        continue
                else:
                    await asyncio.sleep(POLL_INTERVAL)
                    continue
            else:
                if config_failures > 0:
                    logger.info("✅ Configuración cargada exitosamente después de fallos")
                config_failures = 0
                last_successful_config = config

            ips_actuales = set(config.keys())

            # Configuración VIVA: en vez de pasarle una copia al lector y olvidarla,
            # se guarda un dict por IP que se actualiza EN SU LUGAR. El lector
            # compara '_version' en cada ciclo y se recarga solo. Así un cambio de
            # tag se toma sin reiniciar el proceso y sin tocar las otras IPs.
            for ip, nuevo in config.items():
                if ip not in configs_vivas:
                    configs_vivas[ip] = dict(nuevo, _version=1)
                    continue

                if _huella_config(nuevo) != _huella_config(configs_vivas[ip]):
                    version = configs_vivas[ip].get('_version', 0) + 1
                    configs_vivas[ip].clear()
                    configs_vivas[ip].update(nuevo)
                    configs_vivas[ip]['_version'] = version
                    logger.info(
                        f"🔄 Configuración de {ip} actualizada (v{version}): "
                        f"{len(nuevo.get('estaciones', []))} estación(es), "
                        f"{len(nuevo.get('all_addresses', []))} bloque(s). "
                        f"El lector la tomará en el siguiente ciclo."
                    )

            for ip in list(configs_vivas):
                if ip not in ips_actuales:
                    del configs_vivas[ip]

            # Iniciar nuevas tareas para IPs que no existen
            for ip in ips_actuales:
                if ip not in tasks:
                    port = config[ip]['port']
                    logger.info(f"🔌 Iniciando PLC reader para {ip}:{port}")

                    #  CORRECCIÓN CRÍTICA: Crear cola ANTES del procesador
                    # Esto evita la condición de carrera
                    if ip not in ip_data_queues:
                        ip_data_queues[ip] = asyncio.Queue(maxsize=1000)

                    # Crear procesador y su tarea
                    if ip not in ip_processors:
                        processor = IPDataProcessor(ip)
                        ip_processors[ip] = processor

                        proc_task = asyncio.create_task(processor.process_continuously())
                        tasks[f"{ip}_processor"] = proc_task
                    
                    # Crear tarea de lectura
                    reader_task = asyncio.create_task(plc_reader(ip, port, configs_vivas[ip]))
                    tasks[ip] = reader_task

            # Detener tareas para IPs que ya no existen
            for ip in list(tasks.keys()):
                if ip.endswith('_processor'):
                    continue

                if ip not in ips_actuales:
                    logger.info(f"🛑 Deteniendo PLC reader para {ip}")
                    tasks[ip].cancel()
                    del tasks[ip]

                    # Detener procesador también
                    proc_key = f"{ip}_processor"
                    if proc_key in tasks:
                        tasks[proc_key].cancel()
                        del tasks[proc_key]

            # Espera inteligente (tiempo de poll O evento manual)
            if global_config_event:
                try:
                    await asyncio.wait_for(global_config_event.wait(), timeout=POLL_INTERVAL)
                    global_config_event.clear()
                    logger.info("⚡ Actualización de configuración forzada por usuario")
                except asyncio.TimeoutError:
                    pass
            else:
                await asyncio.sleep(POLL_INTERVAL)

        except Exception as e:
            logger.error(f"❌ Error en supervisor: {e}")
            logger.error(traceback.format_exc())
            await asyncio.sleep(POLL_INTERVAL)

# ═══════════════════════════ UI MODERNA (CustomTkinter) ═══════════════════════════

class ModernDashboardUI:
    def __init__(self):
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")

        self.root = ctk.CTk()
        self.root.title("🖥️ Monitor de Prensas - Sistema de Producción")
        self.root.geometry("1400x800")

        # Variables para búsqueda
        self.search_var = StringVar()
        self.search_var.trace('w', lambda *args: self.filter_stations())

        # Diccionario para mapear estaciones a items del tree
        self.station_items = {}

        # Variable para el botón de actualizar turnos
        self.refresh_shifts_btn = None
        self.refresh_config_btn = None  # 🆕 Inicializar variable

        self.build_ui()
        self.update_loop()

    def build_ui(self):
        # Frame principal
        main_frame = ctk.CTkFrame(self.root)
        main_frame.pack(fill="both", expand=True, padx=10, pady=10)

        # Panel superior con estadísticas Y BOTÓN
        stats_frame = ctk.CTkFrame(main_frame)
        stats_frame.pack(fill="x", padx=10, pady=(10, 5))

        # Frame interno para organizar mejor
        stats_container = ctk.CTkFrame(stats_frame)
        stats_container.pack(fill="x", padx=20, pady=15)

        # Fila 1: Estadísticas
        self.lbl_ips = ctk.CTkLabel(
            stats_container,
            text="🌐 IPs Online: 0/0",
            font=("Arial", 16, "bold")
        )
        self.lbl_ips.grid(row=0, column=0, padx=20, pady=5, sticky="w")

        self.lbl_est = ctk.CTkLabel(
            stats_container,
            text="📍 Estaciones: 0",
            font=("Arial", 16, "bold")
        )
        self.lbl_est.grid(row=0, column=1, padx=20, pady=5, sticky="w")

        self.lbl_online = ctk.CTkLabel(
            stats_container,
            text="✅ Online: 0",
            font=("Arial", 16, "bold"),
            text_color="green"
        )
        self.lbl_online.grid(row=0, column=2, padx=20, pady=5, sticky="w")

        self.lbl_offline = ctk.CTkLabel(
            stats_container,
            text="❌ Offline: 0",
            font=("Arial", 16, "bold"),
            text_color="red"
        )
        self.lbl_offline.grid(row=0, column=3, padx=20, pady=5, sticky="w")

        # Fila 2: Botones de actualización
        self.refresh_shifts_btn = ctk.CTkButton(
            stats_container,
            text="🔄 Actualizar Turnos",
            command=self.refresh_shifts,
            width=180,
            font=("Arial", 12, "bold"),
            fg_color="#4CAF50",
            hover_color="#45a049"
        )
        self.refresh_shifts_btn.grid(row=1, column=0, padx=10, pady=10, sticky="ew")

        # 🆕 Botón Actualizar Configuración
        self.refresh_config_btn = ctk.CTkButton(
            stats_container,
            text="⚙️ Actualizar Config",
            command=self.refresh_config,
            width=180,
            font=("Arial", 12, "bold"),
            fg_color="#2196F3",
            hover_color="#1976D2"
        )
        self.refresh_config_btn.grid(row=1, column=1, padx=10, pady=10, sticky="ew")

        self.lbl_uptime = ctk.CTkLabel(
            stats_container,
            text="⏱️ Uptime: 00:00:00",
            font=("Arial", 16, "bold")
        )
        self.lbl_uptime.grid(row=1, column=2, columnspan=2, padx=20, pady=10, sticky="w")

        # Añadir label para mostrar estado de turnos
        self.lbl_shifts_status = ctk.CTkLabel(
            stats_container,
            text="Turnos: No cargados",
            font=("Arial", 12),
            text_color="gray"
        )
        self.lbl_shifts_status.grid(row=2, column=0, columnspan=4, padx=20, pady=(5, 0), sticky="w")

        # Panel de búsqueda
        search_frame = ctk.CTkFrame(main_frame)
        search_frame.pack(fill="x", padx=10, pady=5)

        ctk.CTkLabel(
            search_frame,
            text="🔍 Buscar:",
            font=("Arial", 14, "bold")
        ).pack(side="left", padx=(20, 10), pady=10)

        search_entry = ctk.CTkEntry(
            search_frame,
            textvariable=self.search_var,
            placeholder_text="Nombre de estación o IP...",
            width=400,
            font=("Arial", 12)
        )
        search_entry.pack(side="left", padx=(0, 20), pady=10)

        # Tabla de estaciones
        tree_frame = ctk.CTkFrame(main_frame)
        tree_frame.pack(fill="both", expand=True, padx=10, pady=5)

        # Scrollbar
        scrollbar = ctk.CTkScrollbar(tree_frame)
        scrollbar.pack(side="right", fill="y")

        # Definir columnas (🆕 Agregada columna "Lado")
        cols = ("Estación", "Lado", "Área", "IP", "Parte Original", "Contador", "Estado")

        # Configurar estilo del Treeview
        style = ttk.Style()
        style.theme_use("default")
        style.configure(
            "Treeview",
            background="#2b2b2b",
            foreground="white",
            fieldbackground="#2b2b2b",
            rowheight=35,
            font=("Arial", 11)
        )
        style.configure(
            "Treeview.Heading",
            background="#1f538d",
            foreground="white",
            font=("Arial", 12, "bold")
        )
        style.map('Treeview', background=[('selected', '#144870')])

        self.tree = ttk.Treeview(
            tree_frame,
            columns=cols,
            show="headings",
            yscrollcommand=scrollbar.set
        )
        scrollbar.configure(command=self.tree.yview)

        col_widths = {
            "Estación": 200,
            "Lado": 120,  # 🆕 Ancho para columna Lado
            "Área": 150,
            "IP": 150,
            "Parte Original": 300,
            "Contador": 120,
            "Estado": 180
        }

        for c in cols:
            self.tree.heading(c, text=c)
            self.tree.column(c, width=col_widths.get(c, 120), anchor="center")

        self.tree.pack(fill="both", expand=True)

    def refresh_shifts(self):
        """Función para recargar configuración de turnos"""
        # Deshabilitar botón mientras se actualiza
        self.refresh_shifts_btn.configure(
            text="⏳ Actualizando...",
            state="disabled",
            fg_color="gray"
        )
        self.root.update()  # Forzar actualización de UI

        try:
            # Llamar a la función de actualización
            success = refresh_shifts_config()

            if success:
                # Mostrar confirmación temporal
                self.lbl_shifts_status.configure(
                    text=f"✅ Turnos actualizados: {len(SHIFTS_CONFIG)} turnos cargados",
                    text_color="green"
                )

                # Actualizar columna de área en la tabla (si los turnos están relacionados con áreas)
                self.update_shifts_in_table()

                # Restaurar botón después de 2 segundos
                self.root.after(2000, lambda: self.reset_shifts_button("✅ Turnos actualizados"))
            else:
                self.lbl_shifts_status.configure(
                    text="❌ Error actualizando turnos",
                    text_color="red"
                )
                self.root.after(2000, lambda: self.reset_shifts_button("❌ Error, reintentar"))

        except Exception as e:
            logger.error(f"❌ Error en actualización de turnos: {e}")
            self.lbl_shifts_status.configure(
                text="❌ Error en actualización",
                text_color="red"
            )
            self.root.after(2000, lambda: self.reset_shifts_button("❌ Error, reintentar"))

    def reset_shifts_button(self, text=None):
        """Restaura el botón a su estado normal"""
        if text is None:
            text = "🔄 Actualizar Turnos"

        self.refresh_shifts_btn.configure(
            text=text,
            state="normal",
            fg_color="#4CAF50",
            hover_color="#45a049"
        )

        # Restaurar texto del estado después de 3 segundos más
        self.root.after(3000, lambda: self.lbl_shifts_status.configure(
            text=f"Turnos: {len(SHIFTS_CONFIG)} cargados" if SHIFTS_CONFIG else "Turnos: No cargados",
            text_color="gray"
        ))

    def update_shifts_in_table(self):
        """Actualiza información de turnos en la tabla si es necesario"""
        # Si los turnos afectan cómo se muestran las áreas, actualizar aquí
        for estacion, info in system_monitor['estaciones'].items():
            if estacion in self.station_items:
                # Obtener valores actualizados
                vals = self.get_station_values(estacion, info)
                self.tree.item(self.station_items[estacion], values=vals)

    def refresh_config(self):
        """Forzar actualización de configuración"""
        if global_async_loop and global_config_event:
            self.refresh_config_btn.configure(text="⏳ Actualizando...", state="disabled", fg_color="gray")
            
            # Disparar evento en el loop async
            global_async_loop.call_soon_threadsafe(global_config_event.set)
            
            # Restaurar botón visualmente tras breve pausa
            self.root.after(2000, lambda: self.refresh_config_btn.configure(
                text="⚙️ Actualizar Config", state="normal", fg_color="#2196F3"
            ))
        else:
            logger.warning("⚠️ No hay loop async disponible para actualizar config")

    def get_station_values(self, estacion, info):
        """Obtiene valores formateados para una estación"""
        diff = (datetime.now() - info.get('ultima_actualizacion', datetime.min)).total_seconds()
        ip = info.get('ip', '--')
        plc_connected = system_monitor['ips'].get(ip, {}).get('conectado', False)

        if not plc_connected:
            status = "🔴 PLC OFFLINE"
        elif diff < 60:
            status = "🟢 Online"
        else:
            status = "🟡 Sin datos"

        return (
            estacion,
            info.get('area', 'N/A'),
            ip,
            info.get('parte_actual', '--'),
            info.get('contador', 0),
            status
        )

    def filter_stations(self):
        """Filtrar estaciones por búsqueda y mostrar TODOS los lados"""
        search_text = self.search_var.get().lower()

        # list() antes de ordenar: snapshot frente a los hilos que mutan el dict
        items = sorted(list(system_monitor['estaciones'].items()), key=lambda x: x[0])

        #  Track existing items to avoid rebuilding entire tree
        seen_items = set()

        for est, info in items:
            if search_text and search_text not in est.lower() and search_text not in info.get('ip', '').lower():
                # Ocultar items de esta estación si no coincide con búsqueda
                for key in list(self.station_items.keys()):
                    if key.startswith(f"{est}_"):
                        self.tree.delete(self.station_items[key])
                        del self.station_items[key]
                continue

            diff = (datetime.now() - info.get('ultima_actualizacion', datetime.min)).total_seconds()

            ip = info.get('ip', '--')
            plc_connected = system_monitor['ips'].get(ip, {}).get('conectado', False)

            #  Status basado en conexión PLC
            if not plc_connected:
                status = "🔴 PLC OFFLINE"
            elif diff < 60:
                status = "🟢 Online"
            else:
                status = "🟡 Sin datos"

            # 🆕 NUEVO: Obtener todos los lados detectados
            lados_data = info.get('lados', {})
            
            # Si no hay lados, mostrar info general (backward compatibility)
            if not lados_data:
                key = f"{est}_NO_SIDE"
                vals = (
                    est,
                    "--",  # Lado (sin configurar)
                    info.get('area', 'N/A'),
                    ip,
                    info.get('parte_actual', '--'),
                    info.get('contador', 0),
                    status
                )
                
                if key in self.station_items:
                    self.tree.item(self.station_items[key], values=vals)
                else:
                    item_id = self.tree.insert("", "end", values=vals)
                    self.station_items[key] = item_id
                seen_items.add(key)
            else:
                # 🆕 Mostrar UN LADO POR FILA, agrupados por estación
                first_lado = True
                for lado, lado_info in sorted(lados_data.items()):
                    key = f"{est}_{lado}"
                    
                    # 🎨 Agrupación visual: Mostrar estación solo en la primera fila
                    estacion_display = est if first_lado else ""
                    
                    vals = (
                        estacion_display,  # 🎯 Vacío para las filas siguientes
                        lado,              # LH, RH, etc.
                        info.get('area', 'N/A'),
                        ip,
                        lado_info.get('parte_actual', '--'),
                        lado_info.get('contador', 0),
                        status
                    )
                    
                    if key in self.station_items:
                        self.tree.item(self.station_items[key], values=vals)
                    else:
                        item_id = self.tree.insert("", "end", values=vals)
                        self.station_items[key] = item_id
                    
                    seen_items.add(key)
                    first_lado = False

        # Limpiar items que ya no existen
        for key in list(self.station_items.keys()):
            if key not in seen_items:
                self.tree.delete(self.station_items[key])
                del self.station_items[key]

    def update_loop(self):
        # Snapshot: el hilo de asyncio y los hilos de BD mutan system_monitor
        # mientras la UI lo recorre. Iterar una copia evita
        # "dictionary changed size during iteration".
        ips_snapshot = list(system_monitor['ips'].values())
        estaciones_snapshot = list(system_monitor['estaciones'].items())

        n_ips = sum(1 for ip in ips_snapshot if ip.get('conectado'))
        total_estaciones = len(estaciones_snapshot)

        n_online = 0
        n_offline = 0

        for est, info in estaciones_snapshot:
            diff = (datetime.now() - info.get('ultima_actualizacion', datetime.min)).total_seconds()
            ip = info.get('ip', '')
            plc_connected = system_monitor['ips'].get(ip, {}).get('conectado', False)

            if plc_connected and diff < 60:
                n_online += 1
            else:
                n_offline += 1

        self.lbl_ips.configure(text=f"🌐 IPs Online: {n_ips}/{len(ips_snapshot)}")
        self.lbl_est.configure(text=f"📍 Estaciones: {total_estaciones}")
        self.lbl_online.configure(text=f"✅ Online: {n_online}")
        self.lbl_offline.configure(text=f"❌ Offline: {n_offline}")

        uptime = datetime.now() - system_monitor['estadisticas']['inicio_sistema']
        hours, remainder = divmod(int(uptime.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)
        self.lbl_uptime.configure(text=f"⏱️ Uptime: {hours:02d}:{minutes:02d}:{seconds:02d}")

        # Actualizar información de turnos en UI
        if SHIFTS_CONFIG:
            turnos_info = ", ".join([f"{data['name']} ({data['start'].strftime('%H:%M')})"
                                     for data in SHIFTS_CONFIG.values()][:2])
            self.lbl_shifts_status.configure(
                text=f"Turnos: {turnos_info}",
                text_color="gray"
            )
        else:
            self.lbl_shifts_status.configure(
                text="Turnos: No cargados",
                text_color="orange"
            )

        self.filter_stations()

        self.root.after(1000, self.update_loop)

    def run(self):
        self.root.mainloop()

# ═══════════════════════════ MAIN OPTIMIZADO ═══════════════════════════

def start_async():
    """Inicia el loop asyncio con manejo de errores mejorado"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # 🆕 Inicializar variables globales de control
    global global_async_loop, global_config_event
    global_async_loop = loop
    
    # Crear evento thread-safe
    async def setup_event():
        global global_config_event
        global_config_event = asyncio.Event()
    
    loop.run_until_complete(setup_event())

    #  CONFIGURAR MANEJADOR DE EXCEPCIONES NO CAPTURADAS
    def handle_exception(loop, context):
        msg = context.get("exception", context["message"])
        logger.error(f"🚨 Excepción no capturada en loop asyncio: {msg}")

    loop.set_exception_handler(handle_exception)

    try:
        logger.info("🔄 Iniciando loop asyncio...")
        loop.run_until_complete(supervisor())
    except KeyboardInterrupt:
        logger.info("👋 Detención solicitada por usuario")
    except Exception as e:
        logger.error(f"❌ Error crítico en el loop asyncio: {e}")
        logger.error(traceback.format_exc())
    finally:
        logger.info("🛑 Cerrando loop asyncio...")
        # Cancelar todas las tareas pendientes
        pending = asyncio.all_tasks(loop)
        for task in pending:
            task.cancel()

        # Esperar a que las tareas se cancelen
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))

        # 🔥 Cerrar executors antes que el pool: los hilos pueden estar usando conexiones
        logger.info("🧵 Cerrando pools de hilos...")
        _plc_executor.shutdown(wait=True, cancel_futures=True)
        _db_executor.shutdown(wait=True, cancel_futures=True)

        # 🔥 Cerrar todas las conexiones del pool
        ConnectionPool.close_all()

        loop.close()
        logger.info("✅ Loop asyncio cerrado correctamente")

if __name__ == '__main__':
    #  CONFIGURAR LOGGING MÁS DETALLADO PARA DEBUG
    debug_handler = logging.StreamHandler()
    debug_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    debug_handler.setFormatter(formatter)
    logger.addHandler(debug_handler)

    #  VERIFICAR VARIABLES DE ENTORNO
    required_env_vars = ['DB_SERVER', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]

    if missing_vars:
        logger.error(f"❌ Faltan variables de entorno críticas: {missing_vars}")
        logger.error("Asegúrate de configurar el archivo .env correctamente")

        # Crear una UI mínima para mostrar el error
        ctk.set_appearance_mode("dark")
        root = ctk.CTk()
        root.title("ERROR - Configuración")
        root.geometry("600x300")

        error_frame = ctk.CTkFrame(root)
        error_frame.pack(pady=50, padx=50, fill="both", expand=True)

        ctk.CTkLabel(
            error_frame,
            text="❌ ERROR DE CONFIGURACIÓN",
            font=("Arial", 24, "bold"),
            text_color="red"
        ).pack(pady=20)

        ctk.CTkLabel(
            error_frame,
            text=f"Faltan variables de entorno: {', '.join(missing_vars)}",
            font=("Arial", 16),
            wraplength=500
        ).pack(pady=10)

        ctk.CTkLabel(
            error_frame,
            text="Verifica el archivo .env en la carpeta del proyecto",
            font=("Arial", 14),
            text_color="yellow"
        ).pack(pady=10)

        root.mainloop()
    else:
        logger.info("✅ Todas las variables de entorno críticas están configuradas")

        #  INICIAR EN MODO DEBUG PARA DIAGNÓSTICO
        logger.info("🔧 Iniciando en modo debug...")
        logger.info(f"Python version: {sys.version}")

        # Iniciar thread asíncrono
        t = threading.Thread(target=start_async, daemon=True)
        t.start()
        logger.info("🧵 Thread asíncrono iniciado")

        # Iniciar UI
        logger.info("🖥️  Iniciando interfaz de usuario...")
        app = ModernDashboardUI()
        app.run()