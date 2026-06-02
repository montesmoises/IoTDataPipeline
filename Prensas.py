import pandas as pd
import datetime
import time as system_time
from pymcprotocol import Type3E
from datetime import datetime, time, timedelta, date
from itertools import product, cycle, chain
from collections import namedtuple, defaultdict
import asyncio, hashlib, pyodbc
import logging
import os
import sys
from pathlib import Path
from typing import Optional, Dict, Any, Tuple
from abc import ABC, abstractmethod
import threading
import re
import json
import traceback
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler

# CustomTkinter para la nueva UI
import customtkinter as ctk
from tkinter import ttk, StringVar

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
        """Obtiene o crea conexión SQL reutilizable"""
        with cls._lock:
            current_time = datetime.now()
            connection_key = "default_sql"

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
        """Obtiene o crea conexión AS400 reutilizable"""
        with cls._lock:
            current_time = datetime.now()
            connection_key = "default_as400"

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

        logger.info(f"✅ Turnos cargados: {len(shifts)} turnos")
        for shift_id, data in shifts.items():
            logger.info(f"   Turno {shift_id} ({data['name']}): {data['start'].strftime('%H:%M')} - {data['end'].strftime('%H:%M')}")

        return shifts

    except Exception as e:
        logger.error(f"❌ Error cargando turnos: {e}")
        logger.error(traceback.format_exc())
        return {}
    # NOTA: No cerramos la conexión aquí, el pool la maneja

def refresh_shifts_config():
    """Recarga la configuración de turnos y actualiza variable global"""
    global SHIFTS_CONFIG
    new_config = load_shifts_config()

    if new_config:
        SHIFTS_CONFIG = new_config
        logger.info("🔄 Configuración de turnos actualizada desde BD")
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
    from datetime import date, timedelta

    if not SHIFTS_CONFIG:
        # Si no hay configuración, usar valores por defecto
        if time(8, 0) <= current_time < time(20, 0):
            return 1, date.today()
        else:
            turno = 2
            fecha_plan = date.today() if current_time >= time(20, 0) else date.today() - timedelta(days=1)
            return turno, fecha_plan

    # Ordenar turnos por hora de inicio
    sorted_shifts = sorted(SHIFTS_CONFIG.items(), key=lambda x: x[1]['start'])

    # Si tenemos 2 turnos (el común)
    if len(sorted_shifts) == 2:
        shift1_id, shift1_data = sorted_shifts[0]
        shift2_id, shift2_data = sorted_shifts[1]

        # Turno 1: desde su inicio hasta antes del turno 2
        if shift1_data['start'] <= current_time < shift2_data['start']:
            turno = shift1_id
            fecha_plan = date.today()
        # Turno 2: desde su inicio hasta antes del turno 1 del siguiente día
        elif current_time >= shift2_data['start']:
            turno = shift2_id
            fecha_plan = date.today()
        else:  # Horas antes del inicio del turno 1 (pertenece al turno 2 del día anterior)
            turno = shift2_id
            fecha_plan = date.today() - timedelta(days=1)

    else:
        # Lógica para múltiples turnos
        for shift_id, shift_data in sorted_shifts:
            if shift_data['start'] <= current_time:
                turno = shift_id
                fecha_plan = date.today()
                break
        else:
            # Si no encontramos, usar el último turno del día anterior
            turno = sorted_shifts[-1][0]
            fecha_plan = date.today() - timedelta(days=1)

    return turno, fecha_plan

def has_shift_changed(previous_time: time, current_time: time) -> bool:
    """
    Verifica si hubo un cambio de turno entre dos horas.

    Args:
        previous_time: Hora anterior
        current_time: Hora actual

    Returns:
        True si hubo cambio de turno
    """
    if not SHIFTS_CONFIG:
        # Sin configuración, usar horarios por defecto
        return (previous_time < time(8, 0) <= current_time) or (previous_time < time(20, 0) <= current_time)

    # Verificar si cruzamos el inicio de algún turno
    for shift_data in SHIFTS_CONFIG.values():
        if previous_time < shift_data['start'] <= current_time:
            return True

    return False

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

# Cache para multiplicadores (evita consultas repetidas)
_multiplicador_cache = {}
_cache_lock = threading.RLock()
_CACHE_TIMEOUT = 300  # 5 minutos

def obtener_multiplicador_as400(numero_parte: str, estacion: str, estacion_logger=None) -> int:
    """
    Obtiene el multiplicador desde AS400 solo para el área de Estampado.
    Para otras áreas, retorna 1 directamente.
    CON CACHE para evitar consultas repetidas.
    """
    log = estacion_logger if estacion_logger else logger

    # Verificar si la estación pertenece al área de Estampado
    if estacion in system_monitor['estaciones']:
        area = system_monitor['estaciones'][estacion].get('area', '').lower()

        # Solo consultar AS400 si el área es 'estampado'
        if 'estampado' not in area:
            return 1
    else:
        return 1

    # Verificar cache primero
    cache_key = f"{numero_parte}_{estacion}"
    current_time = datetime.now()

    with _cache_lock:
        if cache_key in _multiplicador_cache:
            value, timestamp = _multiplicador_cache[cache_key]
            # Verificar si el cache no ha expirado (5 minutos)
            if (current_time - timestamp).total_seconds() < _CACHE_TIMEOUT:
                log.debug(f"📦 Multiplicador desde cache: {value}")
                return value

        # No en cache o expirado, consultar AS400
        conn_as400 = crear_conexion_as400()
        if not conn_as400:
            log.warning(f"No se pudo conectar a AS400 para {estacion}, usando multiplicador=1")
            return 1

        try:
            cursor = conn_as400.cursor()
            sql = "SELECT I.IUFD11 FROM LX834F01.IIU AS I WHERE RTRIM(I.IUPROD) = ? AND I.IUSEQN = 1"
            cursor.execute(sql, (numero_parte,))
            result = cursor.fetchone()

            if result and result[0] is not None:
                multiplicador = int(result[0])
                # Guardar en cache
                _multiplicador_cache[cache_key] = (multiplicador, current_time)
                log.debug(f"✅ Multiplicador AS400 para {numero_parte}: {multiplicador}")
                return multiplicador

            log.debug(f"No se encontró multiplicador en AS400 para {numero_parte}, usando 1")
            # Guardar 1 en cache también para no consultar repetidamente
            _multiplicador_cache[cache_key] = (1, current_time)
            return 1

        except Exception as e:
            log.error(f"Error consultando multiplicador en AS400: {e}")
            return 1

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

def actualizar_registro(cursor, contador, fecha_fmt, record_id, status, log, start_fmt=None, necesita_start=False):
    """
    Actualiza el registro de producción.
    """
    sql_parts = ["produced_quantity=?", "production_end=?", "status_id=?"]
    params = [contador, fecha_fmt, status]

    if start_fmt:
        sql_parts.insert(0, "production_start=?")
        params.insert(0, start_fmt)
    elif necesita_start:
        sql_parts.insert(0, "production_start=?")
        params.insert(0, fecha_fmt)

    sql = "UPDATE production_records SET " + ", ".join(sql_parts) + " WHERE id=?"
    params.append(record_id)

    cursor.execute(sql, tuple(params))

def obtener_id_registro_activo(cursor, estacion, fecha_ajustada, turno, numero_parte, log):
    sql = '''SELECT TOP(1) pr.id, pr.planned_quantity, pr.produced_quantity, pr.status_id, pr.production_start
             FROM production_records pr
             JOIN part_numbers pn ON pr.part_number_id = pn.id
             JOIN work_centers wc ON pn.work_center_id = wc.id
             WHERE wc.name=? AND REPLACE(pn.number, ' ', '')=? AND pr.planned_date=? AND pr.shift_id=? AND pr.status_id IN (3, 7, 8) AND pr.synced_to_infor != 1
             ORDER BY pr.status_id DESC, pr.id DESC'''
    cursor.execute(sql, (estacion, numero_parte, fecha_ajustada, turno))
    res = cursor.fetchone()
    if res:
        mult = obtener_multiplicador_as400(numero_parte, estacion, log)
        return res[0], res[1], res[2], res[3], res[4], mult
    return None, None, None, None, None, None

def crear_nuevo_registro(cursor, numero_parte, estacion, contador, turno, fecha_fmt, fecha_ajustada, num_orig, log):
    #  NUEVO: Verificación previa para diagnóstico
    try:
        sql_check = """
            SELECT pn.id, pn.number, wc.name, pn.is_obsolete
            FROM part_numbers pn
            JOIN work_centers wc ON pn.work_center_id = wc.id
            WHERE REPLACE(pn.number, ' ', '')=? AND wc.name=?
        """
        cursor.execute(sql_check, (numero_parte, estacion))
        check_result = cursor.fetchone()

        if check_result:
            log.info(f"✅ Número de parte encontrado en BD: ID={check_result[0]}, number={check_result[1]}, estacion={check_result[2]}, obsolete={check_result[3]}")
            if check_result[3] == 1:
                log.warning(f"⚠️ El número de parte {numero_parte} está marcado como OBSOLETO")
                return None, None, None, None, "PART_NUMBER_OBSOLETO"
        else:
            log.warning(f"⚠️ Número de parte {numero_parte} NO existe en part_numbers para estación {estacion}")
            log.warning(f"   Intentando buscar sin remover espacios...")
            # Intentar sin remover espacios
            cursor.execute("SELECT pn.number FROM part_numbers pn JOIN work_centers wc ON pn.work_center_id = wc.id WHERE pn.number=? AND wc.name=?", (numero_parte, estacion))
            alt_result = cursor.fetchone()
            if alt_result:
                log.info(f"   Encontrado con espacios: {alt_result[0]}")
            else:
                log.warning(f"   Tampoco encontrado con espacios originales")
                return None, None, None, None, "PART_NUMBER_NO_EXISTE_BD"
    except Exception as e:
        log.error(f"Error en verificación previa: {e}")

    sql = '''INSERT INTO production_records (part_number_id, produced_quantity, shift_id, production_start, status_id, planned_date)
             OUTPUT INSERTED.id, INSERTED.planned_quantity, INSERTED.produced_quantity
             SELECT pn.id, ?, ?, ?, 3, ? FROM part_numbers pn
             JOIN work_centers wc ON pn.work_center_id = wc.id
             WHERE REPLACE(pn.number, ' ', '')=? AND wc.name=? AND pn.is_obsolete=0'''

    try:
        #  NUEVO: Logging detallado antes del insert
        log.info(f"🔍 Intentando crear registro para: numero_parte={numero_parte}, estacion={estacion}, turno={turno}")

        cursor.execute(sql, (contador, turno, fecha_fmt, fecha_ajustada, numero_parte, estacion))
        res = cursor.fetchone()
        if res:
            mult = obtener_multiplicador_as400(numero_parte, estacion, log)
            log.info(f"✅ Registro creado exitosamente: ID={res[0]}, numero_parte={numero_parte}")
            return res[0], res[1], res[2], mult, None
        else:
            log.warning(f"⚠️ No se pudo crear registro para {numero_parte} - La consulta no retornó resultados")
            log.warning(f"   Esto significa que el número de parte no existe en part_numbers o no está asociado a {estacion}")
            return None, None, None, None, "SQL_NO_ENCONTRADO"
    except Exception as e:
        log.error(f"❌ Error crear registro para {numero_parte}: {e}")
        log.error(f"   Parámetros: contador={contador}, turno={turno}, estacion={estacion}")
        return None, None, None, None, "DB_ERROR"

def obtener_part_number_id(cursor, numero_parte, estacion):
    sql = "SELECT pn.id FROM part_numbers pn JOIN work_centers wc ON pn.work_center_id = wc.id WHERE REPLACE(pn.number, ' ', '')=? AND wc.name=?"
    cursor.execute(sql, (numero_parte, estacion))
    res = cursor.fetchone()
    return res[0] if res else None

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

# ═══════════════════════════ 🆕 NUEVA FUNCIÓN: VALIDACIÓN ESTAMPADO ═══════════════════════════

# Cache global para almacenar números de parte validados por MDI y estación
# Estructura: {(estacion, mdi): [lista_de_numeros_parte_validados]}
validacion_estampado_cache = {}

def validar_numeros_parte_estampado(mdi: str, estacion: str, log) -> list:
    """
    Valida números de parte para el área de ESTAMPADO.

    Args:
        mdi: Número MDI (receta del troquel) que viene del PLC
        estacion: Nombre de la estación
        log: Logger para la estación

    Returns:
        Lista de números de parte válidos para la estación
    """
    # Crear clave de cache
    cache_key = (estacion, mdi)

    # Si ya tenemos el resultado en cache, retornarlo
    if cache_key in validacion_estampado_cache:
        log.info(f"✅ Usando cache para MDI={mdi} en {estacion}")
        return validacion_estampado_cache[cache_key]

    log.info(f"🔍 Validando números de parte para MDI={mdi} en {estacion}")

    # Conectar a AS400 para obtener los números de parte posibles
    conn_as400 = crear_conexion_as400()
    if not conn_as400:
        log.error(f"❌ No se pudo conectar a AS400 para validar MDI={mdi}")
        return []

    numeros_parte_posibles = []

    try:
        cursor_as400 = conn_as400.cursor()
        # Consulta AS400 para obtener números de parte según el MDI
        sql_as400 = "SELECT REPLACE(IUPROD, ' ', '') FROM LX834F01.IIU WHERE REPLACE(IUFD05, ' ', '') = ? AND IUSEQN = 2"

        #  DEBUG: Mostrar consulta
        mdi_sin_espacios = mdi.replace(' ', '')
        log.info(f"   🔎 Consultando AS400 con MDI (sin espacios): '{mdi_sin_espacios}'")

        cursor_as400.execute(sql_as400, (mdi_sin_espacios,))

        rows = cursor_as400.fetchall()
        numeros_parte_posibles = [row[0] for row in rows if row[0]]

        log.info(f"📋 AS400 retornó {len(numeros_parte_posibles)} números de parte para MDI={mdi}")

        #  DEBUG: Mostrar números encontrados
        if numeros_parte_posibles:
            log.info(f"   Números encontrados en AS400: {numeros_parte_posibles}")

    except Exception as e:
        log.error(f"❌ Error consultando AS400 para MDI={mdi}: {e}")
        return []
    finally:
        # NOTA: No cerramos la conexión aquí, el pool la maneja
        pass

    # Si no hay números de parte posibles, retornar lista vacía (el error se consolida arriba)
    if not numeros_parte_posibles:
        log.warning(f"⚠️ No se encontraron números de parte para MDI={mdi} en AS400")
        res = ([], "MDI_NO_ENCONTRADO_AS400")
        validacion_estampado_cache[cache_key] = res
        return res

    # Ahora validar contra SQL Server para filtrar por estación
    conn_sql = create_connection()
    if not conn_sql:
        log.error(f"❌ No se pudo conectar a SQL Server para validar MDI={mdi}")
        return []

    numeros_parte_validados = []

    try:
        cursor_sql = conn_sql.cursor()

        # Crear placeholders para la consulta IN
        placeholders = ','.join(['?' for _ in numeros_parte_posibles])

        #  CORRECCIÓN: Limpiar espacios de los números de AS400 antes de comparar
        numeros_sin_espacios = [num.replace(' ', '') for num in numeros_parte_posibles]

        #  LOGGING: Mostrar números que se van a buscar
        log.info(f"🔍 Buscando en SQL Server los siguientes números (sin espacios): {numeros_sin_espacios}")

        # FIX: incluir is_obsolete para filtrar en este mismo paso y no depender
        # de crear_nuevo_registro para rechazar obsoletos uno a uno.
        sql_check = f"""
            SELECT REPLACE(pn.number, ' ', ''), pn.is_obsolete FROM part_numbers pn
            JOIN work_centers wc ON pn.work_center_id = wc.id
            WHERE wc.name = ? AND REPLACE(pn.number, ' ', '') IN ({placeholders})
        """

        # Ejecutar consulta con números sin espacios
        params = [estacion] + numeros_sin_espacios
        cursor_sql.execute(sql_check, params)

        rows = cursor_sql.fetchall()

        # Separar todos los encontrados de los activos (no obsoletos)
        todos_encontrados       = [row[0] for row in rows if row[0]]
        obsoletos               = [row[0] for row in rows if row[0] and row[1] == 1]
        numeros_parte_validados = [row[0] for row in rows if row[0] and row[1] != 1]

        log.info(f"✅ SQL Server validó {len(todos_encontrados)} números de parte para estación {estacion}")
        log.info(f"   Números encontrados (total): {todos_encontrados}")

        if obsoletos:
            log.warning(
                f"⚠️ Filtrados {len(obsoletos)} número(s) OBSOLETO(S) en validación de MDI={mdi} "                f"estacion={estacion}: {obsoletos}. Solo se usarán los activos: {numeros_parte_validados}"
            )

        if numeros_parte_validados:
            log.info(f"   Números activos (no obsoletos): {numeros_parte_validados}")
        else:
            log.warning(f"   Números buscados: {numeros_sin_espacios}")
            log.warning(f"   Números encontrados: NINGUNO activo")

        # Si todos eran obsoletos o no existían en SQL, retornar vacío con motivo detallado
        if not numeros_parte_validados:
            if obsoletos and not (set(todos_encontrados) - set(obsoletos)):
                log.warning(
                    f"⚠️ Todos los números para MDI={mdi} en {estacion} son OBSOLETOS: {obsoletos}"
                )
                res = ([], "TODOS_OBSOLETOS")
            else:
                numeros_str = ", ".join(numeros_parte_posibles[:5])
                if len(numeros_parte_posibles) > 5:
                    numeros_str += f" (y {len(numeros_parte_posibles)-5} más)"
                log.warning(
                    f"⚠️ AS400 retornó {len(numeros_parte_posibles)} números pero ninguno válido "                    f"para estación {estacion}. MDI={mdi}, números={numeros_str}"
                )
                res = ([], "NUMEROS_NO_VALIDOS_ESTACION_SQL")
            validacion_estampado_cache[cache_key] = res
            return res

    except Exception as e:
        log.error(f"❌ Error validando en SQL Server para MDI={mdi}: {e}")
        # En caso de error de conexión o consulta, no bloquear AS400
        return ([], "ERROR_CONSULTA_SQL_SERVER")
    finally:
        # NOTA: No cerramos la conexión aquí, el pool la maneja
        pass

    res = (numeros_parte_validados, None)
    # Guardar en cache
    validacion_estampado_cache[cache_key] = res

    return res

# ═══════════════════════════ PATRONES STRATEGY & FACTORY ═══════════════════════════

class DBStrategy(ABC):
    """Estrategia base para operaciones de BD específicas por área"""
    def __init__(self, logger):
        self.log = logger

    @abstractmethod
    def insertar_history(self, cursor, part_number_id, cantidad, fecha_fmt, tiempo, extras=None):
        pass

class DefaultStrategy(DBStrategy):
    """Estrategia por defecto (Carrocería, Ensamble, etc.)"""
    def insertar_history(self, cursor, part_number_id, cantidad, fecha_fmt, tiempo, extras=None):
        sql = '''INSERT INTO histories (part_number_id, quantity, created_at, production_per_cycle) VALUES (?, ?, ?, ?)'''
        try:
            cursor.execute(sql, (part_number_id, cantidad, fecha_fmt, tiempo))
        except Exception as e:
            self.log.error(f"Error insert history: {e}")

class EstampadoStrategy(DBStrategy):
    """
    Estrategia para Estampado.
    Recibe el 'Troquel ID' y lo guarda en la columna 'sequence'.
    """
    def insertar_history(self, cursor, part_number_id, cantidad, fecha_fmt, tiempo, extras=None):
        troquel_id = extras.get('troquel_id', 0) if extras else 0
        sql = '''INSERT INTO histories (part_number_id, quantity, created_at, production_per_cycle, sequence) VALUES (?, ?, ?, ?, ?)'''
        try:
            cursor.execute(sql, (part_number_id, cantidad, fecha_fmt, tiempo, troquel_id))
        except Exception as e:
            self.log.error(f"Error insert history estampado: {e}")

class DBStrategyFactory:
    """Fábrica que decide qué estrategia usar según el nombre del área"""
    @staticmethod
    def get_strategy(area_name, logger):
        if not area_name: return DefaultStrategy(logger)
        nombre = str(area_name).lower().strip()
        if "estampado" in nombre:
            return EstampadoStrategy(logger)
        else:
            return DefaultStrategy(logger)

# ═══════════════════════════ RECOLECTOR DINÁMICO ═══════════════════════════

class IPDataCollector:
    def __init__(self, ip, estaciones, area):
        self.ip = ip
        self.estaciones = estaciones
        self.area = area

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

            # 🆕 NUEVO: Si es área de ESTAMPADO y tenemos un número original (MDI)
            if "estampado" in str(self.area).lower() and partes['orig'] and partes['orig'].strip():
                mdi = partes['orig'].strip()

                # Validar números de parte usando la nueva función
                numeros_validados, error_reason = validar_numeros_parte_estampado(mdi, estacion, log)

                #  NUEVO: Logging detallado de lo que se encontró
                log.info(f"📦 Procesando estampado: MDI={mdi}, estacion={estacion}")
                log.info(f"   Números validados: {numeros_validados}")
                log.info(f"   Contador actual: {data['contador']}, Troquel: {troquel_id}")

                if numeros_validados:
                    # Agregar cada número de parte validado con su contador
                    for num_parte in numeros_validados:
                        datos_estacion.append({
                            'parte': num_parte,
                            'original': partes['orig'],
                            'contador': data['contador'],
                            'tiempo': data.get('tiempo', 0.0),
                            'troquel_id': troquel_id,
                            'validado': True,
                            'lado': grp  # 🆕 Identificar el lado/grupo
                        })
                    log.info(f"✅ Agregados {len(numeros_validados)} números de parte validados a datos_estacion")
                else:
                    # No se encontraron números de parte válidos
                    log.warning(f"❌ No se encontraron números válidos para MDI={mdi}")
                    log.warning(f"   Se agregará como NO VALIDADO para que aparezca en la interfaz")

                    error_msg = error_reason if error_reason else 'NO_PART_NUMBER_ESTAMPADO'

                    datos_estacion.append({
                        'parte': None,
                        'original': partes['orig'],
                        'contador': data['contador'],
                        'tiempo': data.get('tiempo', 0.0),
                        'troquel_id': troquel_id,
                        'validado': False,
                        'error_validacion': error_msg,
                        'lado': grp  # 🆕 Identificar el lado/grupo
                    })
            else:
                # Para otras áreas, mantener el comportamiento original
                if partes['list']:
                    for p_nombre in partes['list']:
                        if not p_nombre:
                            continue
                        datos_estacion.append({
                        'parte': p_nombre,
                        'original': partes['orig'],
                        'contador': data['contador'],
                        'tiempo': data.get('tiempo', 0.0),
                        'troquel_id': troquel_id,
                        'validado': None,  # ⏳ Era True, se forzó a None para requerir validación DB
                        'error_validacion': None,
                        'lado': grp  # 🆕 Identificar el lado/grupo
                    })
                elif partes['orig'] and partes['orig'].strip():
                    datos_estacion.append({
                        'parte': None,
                        'original': partes['orig'],
                        'contador': data['contador'],
                        'tiempo': data.get('tiempo', 0.0),
                        'troquel_id': troquel_id,
                        'validado': False,
                        'error_validacion': 'NO_PART_NUMBER',
                        'lado': grp  # 🆕 Identificar el lado/grupo
                    })

        return datos_estacion

    async def collect_and_enqueue(self, plc, group_info, _blocks=None):
        """Método optimizado con lectura merge-batchread por bloques."""
        try:
            blocks = _blocks if _blocks is not None else list(group_info.get('all_addresses', []))
            merged_reqs = self._merge_blocks(blocks, max_gap=15)
            block_data = {}
            for req in merged_reqs:
                head = req['head']
                length = req['len']
                try:
                    vals = plc.batchread_wordunits(headdevice=head, readsize=length)
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
                datos_estacion = self._process_station_data(est, cfg, block_data, now)
                if datos_estacion:
                    batch.append({'estacion': est, 'datos': datos_estacion, 'ts': now, 'area': self.area, 'plc_ok': True})
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
                    batch.append({'estacion': est, 'datos': [], 'ts': now, 'area': self.area, 'plc_ok': True})
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
        """Carga el estado previo desde el archivo JSON específico para esta IP."""
        if not self.state_file.exists():
            return

        try:
            with open(self.state_file, 'r') as f:
                data = json.load(f)

            for clave, record in data.items():
                try:
                    h_str = record.get('hora_cambio', '00:00:00')
                    h_obj = datetime.strptime(h_str, "%H:%M:%S").time()
                    record['hora_cambio'] = h_obj

                    # 🛡️ MIGRACIÓN DE JSON ANTIGUO: Añadir '_GLOBAL' si la llave no tiene lado
                    if not any(clave.endswith(suf) for suf in ['_GLOBAL', '_RH', '_LH', '_RH REAR', '_LH REAR', '_--']):
                        clave = f"{clave}_GLOBAL"

                    #  RESTAURAR número original desde el estado
                    if 'numero_original' not in record:
                        # Intentar inferir del número validado (para compatibilidad)
                        validated_part = clave.split('_', 1)[1] if '_' in clave else ''
                        record['numero_original'] = validated_part

                    self.active_records[clave] = record
                except Exception as e:
                    logger.error(f"Error al deserializar registro {clave}: {e}")
                    continue

            logger.info(f"Estado recuperado para {self.ip}: {len(self.active_records)} registros cargados.")
        except Exception as e:
            logger.error(f"Error cargando estado para {self.ip}: {e}")

    def save_state(self):
        """Guarda el estado actual en JSON específico para esta IP."""
        try:
            serializable_data = {}
            for clave, record in self.active_records.items():
                rec_copy = record.copy()
                if isinstance(rec_copy.get('hora_cambio'), time):
                    rec_copy['hora_cambio'] = rec_copy['hora_cambio'].strftime("%H:%M:%S")
                if rec_copy.get('id_registro') is None:
                    rec_copy['id_registro'] = 0
                serializable_data[clave] = rec_copy

            with open(self.state_file, 'w') as f:
                json.dump(serializable_data, f, indent=4)
        except Exception as e:
            logger.error(f"Error guardando estado para {self.ip}: {e}")

    def _ensure_active_record(self, cursor, estacion, fecha_plan, turno, num, num_orig, cnt, log, fecha_fmt, force_offset=None, lado='--'):
        """
        Garantiza que exista un registro activo (Status 7) para la estación y parte.
        Hybrid Logic:
        - Normal (force_offset=None): Offset=0 (Absolute).
        - Shift Change (force_offset=X): Offset=X (Relative).
        """
        id_reg, q_plan, q_prod, status, prod_start_db, mult = obtener_id_registro_activo(
            cursor, estacion, fecha_plan, turno, num, log
        )
        
        mult = mult or 1
        
        # Lógica de Offset y Recuperación
        # Si ya existe registro con producción, el offset original debió ser (cnt_actual - produccion / mult)
        # Esto asume que NO hubo resets intermedios. Si hubo, el offset recuperado será aproximado al último reset.
        # Para mayor precisión en reinicios, idealmente el JSON persiste. Esto es el fallback.
        offset_calculado = 0
        corrida_previa_recuperada = 0

        #  NUEVO: Lógica de recuperación ajustada para Lados Compartidos
        # Si compartimos ID con otro lado, la producción DB será la SUMA total.
        # No podemos usar production_actual_db para calcular MI offset individual confiablemente.
        # SOLUCIÓN: Si es una recuperación desde cero (sin JSON), asumimos Offset=0 (Absoluto) o Offset=Cnt (Relativo)
        # dependiendo de la política. Por seguridad en fallo eléctrico, intentamos recuperar lo que podemos.
        
        if id_reg and (q_prod or 0) > 0:
            # Recuperación Inversa: Offset = PLC_Actual - Producción_DB
            # Nota: Si hubo resets, esto nos da el "Offset Efectivo" actual.
            produccion_actual_db = q_prod or 0
            
            #  ADVERTENCIA: Si hay otro lado sumando, produccion_actual_db > mi_produccion.
            # Esto haría que (prod_db / mult) sea grande y el offset calculado sea muy pequeño o negativo.
            # Si el offset calculado es negativo, es señal clara de que hay otro lado aportando.
            # En ese caso, la recuperación inversa NO ES FIABLE para separar lados.
            # FALLBACK: Si no hay JSON, asumir Offset=0 (conteo absoluto del PLC actual)
            # Esto puede duplicar pocas piezas si el PLC no se reseteó, pero es más seguro que corromper el offset.
            
            offset_calculado = 0 # Default seguro
            
            # Intento de recuperación inteligente solo si parece razonable (PLC > DB)
            if cnt * mult >= produccion_actual_db:
                 offset_calculado = cnt - (produccion_actual_db / mult)
            else:
                 # PLC < DB (Reseteo o Múltiples Lados sumando)
                 log.warning(f"⚠️ Inconsistencia/Lados Múltiples al recuperar {lado}: PLC ({cnt}) < BD ({produccion_actual_db}). Asumiendo Offset=0.")
                 offset_calculado = 0
        
        elif id_reg:
            # Registro existe pero en 0.
            offset_calculado = force_offset if force_offset is not None else 0

        # Casos de Retorno (Diccionario de estado)
        reg_state = {
            'id_registro': None,
            'quantity_planeada': 0,
            'corrida_previa': corrida_previa_recuperada, 
            'multiplicador': mult,
            'contador_registro': cnt,
            'offset_variable': offset_calculado,
            'hora_cambio': datetime.now().time().replace(microsecond=0),
            'numero_original': num_orig,
            'lado': lado,  # 🆕 Persistir el lado
            'necesita_production_start': False,
            'error_bd': None # 🆕 Para propagar el error de BD
        }

        # CASO 1: CREAR NUEVO
        if id_reg is None:
            # Hybrid Logic Decision:
            if force_offset is not None:
                # Caso Cambio de Turno: Relativo al contador previo del turno anterior.
                # Protección: si el PLC ya reinició o bajó su contador, jamás crear un registro negativo.
                offset_calculado = force_offset
                delta_inicial = cnt - offset_calculado

                if delta_inicial < 0:
                    log.warning(
                        f"⚠️ Cambio de turno con delta negativo detectado en {estacion}/{num}/{lado}: "
                        f"cnt_actual={cnt}, contador_previo_turno={offset_calculado}, mult={mult}. "
                        f"Se fuerza qty_inicial=0 para evitar negativos en production_records."
                    )
                    qty_inicial = 0
                else:
                    qty_inicial = delta_inicial * mult

                log.info(
                    f"🕒 Nuevo registro por cambio de turno en {estacion}/{num}/{lado}: "
                    f"cnt_actual={cnt}, offset_turno={offset_calculado}, delta_inicial={max(delta_inicial, 0)}, "
                    f"qty_inicial={qty_inicial}, turno={turno}"
                )
            else:
                # Caso Normal (Parte Nueva): Absoluto
                offset_calculado = 0
                if mult == 1: 
                    mult = obtener_multiplicador_as400(num, estacion, log) or 1
                qty_inicial = cnt * mult

            id_reg, q_plan, q_prod, mult_new, error_bd = crear_nuevo_registro(
                cursor, num, estacion, qty_inicial, turno, fecha_fmt, fecha_plan, num_orig, log
            )
            
            if id_reg is None: 
                reg_state['error_bd'] = error_bd
                return reg_state
            
            reg_state.update({
                'id_registro': id_reg, 
                'quantity_planeada': q_plan,
                'corrida_previa': 0, 
                'multiplicador': mult_new or mult,
                'offset_variable': offset_calculado,
                'necesita_production_start': False  # ✅ Corregido: Ya tiene start del INSERT
            })
            return reg_state

        # CASO 2: REACTIVAR (Status 8)
        if status == 8:
            try:
                cursor.execute("UPDATE production_records SET status_id = 7 WHERE id = ? AND status_id = 8", (id_reg,))
                log.info(f"✅ Registro {id_reg} reactivado (status 8 → 7)")
            except Exception as e:
                log.error(f"❌ Error reactivando registro {id_reg}: {e}")
            
            reg_state.update({
                'id_registro': id_reg,
                'quantity_planeada': q_plan,
                'multiplicador': mult,
                'offset_variable': offset_calculado, # Asegurar que el offset se propague
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
            'offset_variable': offset_calculado, # Asegurar que el offset se propague
        })
        return reg_state

    async def process_continuously(self):
        if self.ip not in ip_data_queues: return
        while True:
            try:
                #  MODIFICACIÓN: Procesar todo el batch de una vez
                batch = await ip_data_queues[self.ip].get()

                #  NUEVO: Procesar todas las estaciones del batch concurrentemente
                # pero con un semáforo para no saturar la BD
                semaphore = asyncio.Semaphore(10)  # Máximo 10 estaciones simultáneas

                async def process_with_semaphore(pkg):
                    async with semaphore:
                        await self._process_estacion(pkg)

                tasks = [process_with_semaphore(pkg) for pkg in batch]
                await asyncio.gather(*tasks, return_exceptions=True)

                ip_data_queues[self.ip].task_done()

            except Exception as e:
                logger.error(f"Error en loop de procesamiento para {self.ip}: {e}")
                await asyncio.sleep(0.5)

    async def _process_estacion(self, pkg):
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

        db_strategy = DBStrategyFactory.get_strategy(area, log)

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

                if plc_ok and not datos:
                    cursor.execute("""
                        UPDATE pr SET pr.status_id = 8, pr.production_end=?
                        FROM production_records pr
                        JOIN part_numbers pn ON pr.part_number_id = pn.id
                        JOIN work_centers wc ON pn.work_center_id = wc.id
                        WHERE wc.name=? AND pr.planned_date=? AND pr.shift_id=? AND pr.status_id=7
                    """, (fecha_fmt, estacion, fecha_plan, turno))

                    keys_to_delete = [k for k in self.active_records if k.startswith(f"{estacion}_")]
                    for k in keys_to_delete:
                        del self.active_records[k]
                        state_changed = True

                    conn.commit()
                    if state_changed: self.save_state()
                    return

                if not plc_ok:
                    log.warning(f"⚠️ PLC desconectado para {estacion}. Manteniendo estado en caché sin cambios.")
                    return

                claves_actuales_en_plc = set()
                for d in datos:
                    if d['parte']:  # Solo agregar si tiene parte válida
                        # 🆕 CLAVE ÚNICA POR LADO: estacion_parte_lado
                        lado = d.get('lado', '--')
                        claves_actuales_en_plc.add(f"{estacion}_{d['parte']}_{lado}")

                claves_obsoletas = []
                for k in self.active_records:
                    # 🆕 Verificar prefijo y ausencia en claves actuales
                    if k.startswith(f"{estacion}_") and k not in claves_actuales_en_plc:
                        claves_obsoletas.append(k)

                for k in claves_obsoletas:
                    record_id = self.active_records[k].get('id_registro')
                    if record_id:
                         try:
                            cursor.execute(
                                "UPDATE production_records SET status_id = 8, production_end=? WHERE id=? AND status_id=7",
                                (fecha_fmt, record_id)
                            )
                         except Exception as e:
                            log.error(f"Error cerrando registro obsoleto {record_id}: {e}")

                    del self.active_records[k]
                    state_changed = True

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
                        new_record = self._ensure_active_record(cursor, estacion, fecha_plan, turno, num, num_orig, cnt, log, fecha_fmt, lado=d.get('lado', '--'))
                        
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

                        # FIX histories: el primer tramo (desde el offset hasta cnt actual)
                        # nunca llega al bloque if cnt != prev porque contador_registro se
                        # inicializa igual a cnt. Lo escribimos aquí directamente.
                        _offset_nuevo  = new_record.get('offset_variable', 0)
                        _mult_nuevo    = new_record.get('multiplicador', 1)
                        # _corrida_nueva siempre es 0 en registro nuevo (condición redundante eliminada)
                        _delta_inicial = cnt - _offset_nuevo

                        if _delta_inicial > 0:
                            # Solo registramos si hay piezas reales en este primer tramo
                            pid_nuevo = obtener_part_number_id(cursor, num, estacion)
                            if pid_nuevo:
                                _qty_hist_ini = _delta_inicial  # estampado guarda golpes sin multiplicar
                                extras_ini = {'troquel_id': troquel_id}
                                try:
                                    db_strategy.insertar_history(
                                        cursor, pid_nuevo, _qty_hist_ini, fecha_fmt,
                                        d.get('tiempo', 0.0), extras_ini
                                    )
                                    log.info(
                                        f"📝 History inicial registrado al crear nuevo registro {num}: "                                        f"delta={_delta_inicial}, offset={_offset_nuevo}, turno={turno}"
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
                        prev_offset = reg.get('offset_variable', 0)
                        prev_corrida = reg.get('corrida_previa', 0)

                        log.warning(
                            f"🕒 Cambio de turno detectado en {estacion}/{num}/{d.get('lado', '--')}: "
                            f"hora_anterior={reg.get('hora_cambio')}, hora_actual={hora}, "
                            f"contador_previo={prev_counter}, contador_actual={cnt}, "
                            f"offset_actual={prev_offset}, corrida_previa={prev_corrida}, old_id={old_id}"
                        )

                        try:
                            cursor.execute(
                                "UPDATE production_records SET status_id = 8, production_end=? WHERE id=? AND status_id=7",
                                (fecha_fmt, old_id)
                            )
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
                            f"turno_nuevo={turno}, fecha_plan={fecha_plan}, force_offset={prev_counter}, cnt_actual={cnt}"
                        )

                        # Crear NUEVO registro de TURNO
                        # HYBRID LOGIC: Cambio de turno -> Relativo al contador PREVIO
                        new_reg_data = self._ensure_active_record(
                            cursor, estacion, fecha_plan, turno, num, num_orig, cnt, log, fecha_fmt, 
                            force_offset=prev_counter,
                            lado=d.get('lado', '--')
                        )
                        
                        if new_reg_data:
                            reg.update(new_reg_data)
                            log.info(
                                f"✅ Nuevo estado tras cambio de turno en {estacion}/{num}/{d.get('lado', '--')}: "
                                f"nuevo_id={reg.get('id_registro')}, offset={reg.get('offset_variable')}, "
                                f"contador_registro={reg.get('contador_registro')}, corrida_previa={reg.get('corrida_previa', 0)}"
                            )
                            
                            # FIX 2a: El delta inicial del nuevo turno nunca llega al bloque
                            # "if cnt != prev" porque contador_registro ya se inicializó en cnt.
                            # Se registra aquí directamente para no perder ese primer incremento.
                            _offset_ct = reg.get('offset_variable', 0)
                            _delta_ct  = cnt - _offset_ct
                            if _delta_ct > 0:
                                _pid_ct = obtener_part_number_id(cursor, num, estacion)
                                if _pid_ct:
                                    _extras_ct = {'troquel_id': troquel_id}
                                    try:
                                        db_strategy.insertar_history(
                                            cursor, _pid_ct, _delta_ct, fecha_fmt,
                                            d.get('tiempo', 0.0), _extras_ct
                                        )
                                        log.info(
                                            f"📝 History cambio de turno: {num} "
                                            f"delta={_delta_ct}, offset={_offset_ct}, turno={turno}"
                                        )
                                    except Exception as _eh_ct:
                                        log.error(f"❌ Error history cambio turno {num}: {_eh_ct}")
                        state_changed = True

                    prev = reg.get("contador_registro", cnt)

                    if cnt != prev:
                        multiplicador = reg.get("multiplicador", 1)
                        
                        # 🔍 DIAGNÓSTICO: Log de cambio de contador
                        log.debug(f"📊 Cambio contador en {num}: {prev} → {cnt}")
                        
                        # ⚠️ DETECCIÓN DE RESET (Bajada de contador con respecto al OFFSET o PREV?)
                        # En modelo relativo, el offset es fijo. Solo si CNT baja a menor que Offset, o hubo un salto extraño.
                        # Pero el reset clásico es que CNT se va a 0.
                        offset = reg.get('offset_variable', 0)
                        
                        if cnt < prev: # Bajada detectada
                            log.warning(f"⚠️ Reset detectado en {num}: {prev} -> {cnt}. Offset era {offset}")
                            
                            # Producción lograda hasta antes del reset con el offset viejo
                            # Prod_Tramo = (Prev - Offset) * Mult
                            # Esto se suma a corrida_previa
                            if prev >= offset:
                                lost_production_tramo = (prev - offset) * multiplicador
                                reg['corrida_previa'] = reg.get('corrida_previa', 0) + lost_production_tramo
                                log.info(f"   Acumulado {lost_production_tramo} a corrida_previa.")
                            
                            # Nuevo Offset: 0 (o el nuevo cnt si asumimos que reinició ahi)
                            reg['offset_variable'] = 0
                            offset = 0 # Actualizar localmente para el calculo abajo
                        
                        # MODELO RELATIVO ROBUSTO:
                        # Producción = ((PLC - Offset) * Multi) + Corrida_Previa
                        # Nota: Si PLC < Offset (ej. después de reset mal detectado), esto daría negativo.
                        # Protección básica:
                        if cnt >= offset:
                            prod_tramo_actual = (cnt - offset) * multiplicador
                        else:
                            # Caso raro: CNT bajó pero no entró en el if detect de arriba (??) O offset quedó alto.
                            # Si entramos aquí es que offset > cnt. Asumimos producción 0 del tramo y forzamos reset?
                            prod_tramo_actual = 0
                        
                        prod_absoluta = prod_tramo_actual + reg.get('corrida_previa', 0)
                        
                        necesita_start = reg.get('necesita_production_start', False)

                        actualizar_registro(
                            cursor,
                            prod_absoluta,
                            fecha_fmt,
                            reg['id_registro'],
                            7,
                            log,
                            necesita_start=necesita_start
                        )

                        pid = obtener_part_number_id(cursor, num, estacion)
                        if pid:
                            if cnt >= prev:
                                incremento_ciclo = cnt - prev
                            else:
                                incremento_ciclo = cnt # Asumiendo reset a 0

                            cantidad_historial = incremento_ciclo * multiplicador
                            extras = {'troquel_id': troquel_id}
                            # En area estampado se guarda el valor absoluto del contador en history, en otras el incremento
                            # cantidad_history = cnt if "estampado" in str(area).lower() else cantidad_historial
                            # En area estampado guardad el incremento sin multiplicar por el multiplicador
                            cantidad_history = incremento_ciclo if "estampado" in str(area).lower() else incremento_ciclo

                            if cantidad_history > 0:
                                db_strategy.insertar_history(cursor, pid, cantidad_history, fecha_fmt, tiempo, extras)

                        if necesita_start:
                            reg['necesita_production_start'] = False

                        reg['contador_registro'] = cnt
                        reg['hora_cambio'] = hora
                        state_changed = True

                conn.commit()

                if state_changed:
                    self.save_state()

        except Exception as e:
            log.error(f"❌ Error procesando {estacion}: {e}")
            log.error(traceback.format_exc())
        # NOTA: No cerramos la conexión aquí, el pool la maneja

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
    """Lector optimizado para PLC con manejo de errores mejorado"""

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

async def supervisor():
    tasks = {}
    last_successful_config = {}
    config_failures = 0
    max_config_failures = 5
    last_status_log = datetime.now()

    # 🔄 Cargar turnos al inicio
    refresh_shifts_config()

    logger.info("🚀 Supervisor iniciado")

    while True:
        try:
            config = load_config()

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
                    reader_task = asyncio.create_task(plc_reader(ip, port, config[ip]))
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

        items = sorted(system_monitor['estaciones'].items(), key=lambda x: x[0])

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
        n_ips = sum(1 for ip in system_monitor['ips'].values() if ip.get('conectado'))
        total_estaciones = len(system_monitor['estaciones'])

        n_online = 0
        n_offline = 0

        for est, info in system_monitor['estaciones'].items():
            diff = (datetime.now() - info.get('ultima_actualizacion', datetime.min)).total_seconds()
            ip = info.get('ip', '')
            plc_connected = system_monitor['ips'].get(ip, {}).get('conectado', False)

            if plc_connected and diff < 60:
                n_online += 1
            else:
                n_offline += 1

        self.lbl_ips.configure(text=f"🌐 IPs Online: {n_ips}/{len(system_monitor['ips'])}")
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