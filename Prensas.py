import pandas as pd
import datetime
import time as system_time
from pymcprotocol import Type3E
from datetime import datetime, time, timedelta
from itertools import product, cycle, chain
from collections import namedtuple, defaultdict
import asyncio, hashlib, pyodbc
import logging
import os
from pathlib import Path
from typing import Optional, Dict, Any, Tuple
from abc import ABC, abstractmethod
import threading
import re
import json
import traceback
from dotenv import load_dotenv  # Para cargar variables del entorno

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

# Configurar logging
logging.basicConfig(level=logging.NOTSET, handlers=[])
logger = logging.getLogger("supervisor")
logger.setLevel(logging.INFO)
logger.propagate = False

station_loggers = {}

# Intervalo (en segundos) para volver a leer la configuración de la BD
POLL_INTERVAL = 5

# Archivo CSV de números de parte no encontrados
CSV_FILE = CSV_DIR / "parts_not_found.csv"

# 🔥 NUEVO: Función helper para guardar errores de validación
def registrar_error_validacion(estacion, numero_original, tipo_error):
    """
    Registra en CSV los números de parte que no pasan la validación.
    Solo registra UNA VEZ por día (no se repite el mismo número en la misma fecha).

    Args:
        estacion: Nombre de la estación
        numero_original: Número original del PLC
        tipo_error: 'AS400_NO_ENCONTRADO', 'SQL_NO_ENCONTRADO', etc.
    """
    try:
        ts = datetime.now()
        fecha_hoy = ts.strftime('%Y-%m-%d')

        # Crear DataFrame con el nuevo registro
        df_nuevo = pd.DataFrame([{
            'estacion': estacion,
            'numero_original_plc': numero_original,
            'tipo_error': tipo_error,
            'fecha': fecha_hoy,
            'timestamp': ts.strftime('%Y-%m-%d %H:%M:%S')
        }])

        file_exists = CSV_FILE.exists()

        # 🔥 CAMBIO: Verificar si ya existe en la misma FECHA (día completo)
        if file_exists:
            try:
                existing_df = pd.read_csv(CSV_FILE)

                # Buscar duplicados en la misma FECHA (sin importar la hora)
                duplicates = existing_df[
                    (existing_df['estacion'] == estacion) &
                    (existing_df['numero_original_plc'] == numero_original) &
                    (existing_df['tipo_error'] == tipo_error) &
                    (existing_df['fecha'] == fecha_hoy)  # ← Solo compara la fecha (día)
                ]

                if not duplicates.empty:
                    # Ya existe un registro para este número de parte HOY
                    logger.info(f"ℹ️ Error ya registrado hoy: {estacion} - {numero_original} - {tipo_error}")
                    return
            except Exception as e:
                # Si hay error leyendo el CSV, continuar con el guardado
                logger.warning(f"⚠️ Error verificando duplicados en CSV: {e}")

        # Guardar nuevo registro
        df_nuevo.to_csv(CSV_FILE, mode='a', header=not file_exists, index=False)
        logger.info(f"📝 Error de validación registrado: {estacion} - {numero_original} - {tipo_error}")

    except Exception as e:
        logger.error(f"❌ Error guardando CSV de validación: {e}")

# ═══════════════════════════ HELPERS LOGGING ═══════════════════════════

def get_station_logger(estacion):
    """Obtiene o crea un logger específico para una estación"""
    if estacion in station_loggers:
        return station_loggers[estacion]

    station_logger = logging.getLogger(f"station.{estacion}")
    # 🔥 CAMBIO: Solo WARNING y ERROR
    station_logger.setLevel(logging.WARNING)  # Solo WARNING, ERROR y CRITICAL
    station_logger.propagate = False

    log_path = LOGS_DIR / f"{estacion}.log"
    file_handler = logging.FileHandler(log_path, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.WARNING)  # Solo WARNING y ERROR
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    station_logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)  # Solo WARNING y ERROR en consola
    console_handler.setFormatter(formatter)
    station_logger.addHandler(console_handler)

    station_loggers[estacion] = station_logger
    return station_logger

# ═══════════════════════════ CONEXIONES BD ═══════════════════════════

def create_connection():
    """Crea conexión a SQL Server usando variables del entorno"""
    server = os.getenv('DB_SERVER')
    database = os.getenv('DB_NAME')
    username = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')

    return pyodbc.connect(
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'UID={username};'
        f'PWD={password}'
    )

def crear_conexion_as400(host: str = None, user: str = None,
                        password: str = None, database: str = "") -> Optional[pyodbc.Connection]:
    """Crea conexión a AS400 usando variables del entorno"""

    host = host or os.getenv('AS400_HOST')
    user = user or os.getenv('AS400_USER')
    password = password or os.getenv('AS400_PASSWORD')

    conn_str = f"DRIVER={{iSeries Access ODBC Driver}};SYSTEM={host};UID={user};PWD={password};"
    if database:
        conn_str += f"DBQ={database};"
    try:
        return pyodbc.connect(conn_str)
    except pyodbc.Error:
        return None

# ═══════════════════════════ FUNCIONES BASE DE DATOS (COMUNES) ═══════════════════════════

def obtener_multiplicador_as400(numero_parte: str, estacion_logger=None) -> int:
    log = estacion_logger if estacion_logger else logger
    conn_as400 = crear_conexion_as400()
    if not conn_as400:
        return 1
    try:
        cursor = conn_as400.cursor()
        sql = "SELECT I.IUFD11 FROM LX834F01.IIU AS I WHERE RTRIM(I.IUPROD) = ? AND I.IUSEQN = 2"
        cursor.execute(sql, (numero_parte,))
        result = cursor.fetchone()
        if result and result[0] is not None:
            return int(result[0])
        return 1
    except Exception as e:
        log.error(f"Error AS400: {e}")
        return 1
    finally:
        if conn_as400:
            conn_as400.close()

def load_config():
    """Carga configuración incluyendo el NOMBRE DEL ÁREA"""
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
    conn = create_connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    conn.close()

    ip_groups = defaultdict(lambda: {
        'estaciones': [], 'port': 1025, 'serie': 'Q',
        'all_addresses': set(), 'station_configs': {}, 'area': 'Default'
    })

    global system_monitor

    for wc, ip, tag, addr, lng, area in rows:
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
            ip_groups[ip]['all_addresses'].update(expand_block(addr, int(lng)))

        if wc not in ip_groups[ip]['estaciones']:
            ip_groups[ip]['estaciones'].append(wc)

    return dict(ip_groups)

def actualizar_registro(cursor, contador, fecha_fmt, record_id, status, log, start_fmt=None):
    """
    Actualiza el registro de producción.
    """
    sql_parts = ["produced_quantity=?", "production_end=?", "status_id=?"]
    params = [contador, fecha_fmt, status]

    if start_fmt:
        sql_parts.insert(0, "production_start=?")
        params.insert(0, start_fmt)

    sql = "UPDATE production_records SET " + ", ".join(sql_parts) + " WHERE id=?"
    params.append(record_id)

    cursor.execute(sql, tuple(params))

def obtener_id_registro_activo(cursor, estacion, fecha_ajustada, turno, numero_parte, log):
    sql = '''SELECT TOP(1) pr.id, pr.planned_quantity, pr.produced_quantity, pr.status_id, pr.production_start
             FROM production_records pr
             JOIN part_numbers pn ON pr.part_number_id = pn.id
             JOIN work_centers wc ON pn.work_center_id = wc.id
             WHERE wc.name=? AND pn.number=? AND pr.planned_date=? AND pr.shift_id=? AND pr.status_id IN (3, 7, 8) AND pr.synced_to_infor != 1
             ORDER BY pr.status_id DESC, pr.id DESC'''
    cursor.execute(sql, (estacion, numero_parte, fecha_ajustada, turno))
    res = cursor.fetchone()
    if res:
        mult = obtener_multiplicador_as400(numero_parte, log)
        return res[0], res[1], res[2], res[3], res[4], mult
    return None, None, None, None, None, None

def crear_nuevo_registro(cursor, numero_parte, estacion, contador, turno, fecha_fmt, fecha_ajustada, num_orig, log):
    sql = '''INSERT INTO production_records (part_number_id, produced_quantity, shift_id, production_start, status_id, planned_date)
             OUTPUT INSERTED.id, INSERTED.planned_quantity, INSERTED.produced_quantity
             SELECT pn.id, ?, ?, ?, 3, ? FROM part_numbers pn
             JOIN work_centers wc ON pn.work_center_id = wc.id
             WHERE pn.number=? AND wc.name=? AND pn.is_obsolete=0'''
    try:
        cursor.execute(sql, (contador, turno, fecha_fmt, fecha_ajustada, numero_parte, estacion))
        res = cursor.fetchone()
        if res:
            mult = obtener_multiplicador_as400(numero_parte, log)
            return res[0], res[1], res[2], mult
        else:
            log.warning(f"No se pudo crear registro para {numero_parte}")
            try:
                ts = datetime.now()
                df = pd.DataFrame([{
                    'estacion': estacion, 'numero_parte': numero_parte,
                    'numero_parte_original': num_orig, 'fecha': ts.strftime('%Y-%m-%d'),
                    'timestamp': ts.strftime('%Y-%m-%d %H:%M:%S')
                }])
                hdr = not CSV_FILE.exists()
                df.to_csv(CSV_FILE, mode='a', header=hdr, index=False)
            except Exception as e:
                log.error(f"Error CSV: {e}")
            return None, None, None, None
    except Exception as e:
        log.error(f"Error crear registro: {e}")
        return None, None, None, None

def obtener_part_number_id(cursor, numero_parte, estacion):
    sql = "SELECT pn.id FROM part_numbers pn JOIN work_centers wc ON pn.work_center_id = wc.id WHERE pn.number=? AND wc.name=?"
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

def decodificar_bloque(bloque, estacion=None, area=None):
    """
    Decodifica el bloque del PLC.
    SOLO si el área es 'Estampado' realiza la validación cruzada (AS400 + SQL).
    Para cualquier otra área, devuelve el valor limpio directo.

    Returns:
        tuple: (original, partes_validadas, metadata)
        - original: String crudo del PLC
        - partes_validadas: Lista de números validados
        - metadata: Dict con info de validación {'error': str, 'candidatos_as400': list}
    """
    if not bloque:
        return None, None, {}

    chars = [chr(v & 0xFF) + chr((v >> 8) & 0xFF) for v in bloque]
    original = "".join(chars).replace("\x00", "")
    limpia = original.strip()

    if not limpia:
        return original, [], {}

    if not area or "estampado" not in str(area).lower():
        return original, [limpia], {}

    if not estacion:
        return original, [limpia], {}

    candidatos = []
    partes_finales = []
    metadata = {'error': None, 'candidatos_as400': []}

    try:
        conn_as400 = crear_conexion_as400()
        if conn_as400:
            try:
                cursor_as400 = conn_as400.cursor()
                sql_as400 = "SELECT TRIM(IUPROD) FROM LX834F01.IIU WHERE TRIM(IUFD05) = ? AND IUSEQN = 2"
                cursor_as400.execute(sql_as400, (limpia,))
                candidatos = [row[0].strip() for row in cursor_as400.fetchall() if row[0]]
                metadata['candidatos_as400'] = candidatos
            except Exception as e:
                print(f"Error AS400: {e}")
                metadata['error'] = f"AS400_ERROR: {str(e)}"
            finally:
                if conn_as400:
                    conn_as400.close()

        if not candidatos:
            metadata['error'] = "AS400_NO_ENCONTRADO"
            registrar_error_validacion(estacion, original, "AS400_NO_ENCONTRADO")
            return original, [], metadata

        conn_sql = create_connection()
        if conn_sql:
            try:
                cursor_sql = conn_sql.cursor()
                placeholders = ', '.join(['?'] * len(candidatos))
                sql_check = f"""
                    SELECT pn.number FROM part_numbers pn 
                    JOIN work_centers wc ON pn.work_center_id = wc.id 
                    WHERE wc.name = ? AND pn.number IN ({placeholders})
                """
                params = [estacion] + candidatos
                cursor_sql.execute(sql_check, params)
                partes_finales = [row[0] for row in cursor_sql.fetchall()]
            except Exception as e:
                print(f"Error SQL Local: {e}")
                metadata['error'] = f"SQL_ERROR: {str(e)}"
            finally:
                if conn_sql:
                    conn_sql.close()

        if not partes_finales:
            metadata['error'] = "SQL_NO_ENCONTRADO"
            registrar_error_validacion(estacion, original, "SQL_NO_ENCONTRADO")

    except Exception as e:
        print(f"Error validación estampado: {e}")
        metadata['error'] = f"VALIDATION_ERROR: {str(e)}"

    return original, partes_finales, metadata

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
        parts = tag_name.split()

        grupo = "GLOBAL"
        if len(parts) > 1:
            possible_suffix = parts[-1].upper()
            if possible_suffix in ['LH', 'RH']:
                grupo = possible_suffix

        tipo = "otro"
        if "contador" in lower: tipo = "contador"
        elif "tiempo" in lower or "ciclo" in lower: tipo = "tiempo"
        elif "parte" in lower or "part" in lower: tipo = "parte"
        elif "troquel" in lower or "die" in lower: tipo = "troquel"

        return tipo, grupo

    async def collect_and_enqueue(self, plc, group_info):
        try:
            addrs = list(group_info['all_addresses'])
            if not addrs: return

            vals, _ = plc.randomread(word_devices=addrs, dword_devices=[])
            val_map = dict(zip(addrs, vals))

            batch = []
            now = datetime.now()

            for est in self.estaciones:
                cfg = group_info['station_configs'].get(est, {})
                groups_data = defaultdict(dict)

                for tag_name, info in cfg.items():
                    tipo, grupo = self._parse_tag(tag_name)
                    block_vals = [val_map.get(a, 0) for a in expand_block(info['address'], info['long'])]

                    val = None
                    if tipo == "contador": val = block_vals[0] if block_vals else 0
                    elif tipo == "tiempo":
                        try: val = abs(int(block_vals[0])/1000.0)
                        except: val = 0.0
                    elif tipo == "parte":
                        orig, limpios, meta = decodificar_bloque(block_vals, estacion=est, area=self.area)
                        val = {'orig': orig, 'list': limpios, 'meta': meta}
                    elif tipo == "troquel": val = block_vals[0] if block_vals else 0

                    if val is not None: groups_data[grupo][tipo] = val

                datos_estacion = []
                for grp, data in groups_data.items():
                    if 'contador' not in data: continue

                    partes = data.get('parte', {'orig': '', 'list': [], 'meta': {}})
                    troquel_id = data.get('troquel', None)

                    if partes['list']:
                        for p_nombre in partes['list']:
                            if not p_nombre: continue
                            datos_estacion.append({
                                'parte': p_nombre,
                                'original': partes['orig'],
                                'contador': data['contador'],
                                'tiempo': data.get('tiempo', 0.0),
                                'troquel_id': troquel_id,
                                'validado': True
                            })
                    elif partes['orig'] and partes['orig'].strip():
                        datos_estacion.append({
                            'parte': None,
                            'original': partes['orig'],
                            'contador': data['contador'],
                            'tiempo': data.get('tiempo', 0.0),
                            'troquel_id': troquel_id,
                            'validado': False,
                            'error_validacion': partes['meta'].get('error', 'UNKNOWN')
                        })

                if datos_estacion:
                    batch.append({
                        'estacion': est,
                        'datos': datos_estacion,
                        'ts': now,
                        'area': self.area,
                        'plc_ok': True
                    })

                    last = datos_estacion[0]
                    if est in system_monitor['estaciones']:
                        status_validacion = " ✅" if last.get('validado', True) else " ⚠️"
                        system_monitor['estaciones'][est].update({
                            'parte_actual': last['original'] + status_validacion,
                            'contador': last['contador'],
                            'tiempo_ciclo': last['tiempo'],
                            'ultima_actualizacion': now,
                            'ip': self.ip,
                            'validado': last.get('validado', True),
                            'error_validacion': last.get('error_validacion', None)
                        })
                else:
                    batch.append({
                        'estacion': est,
                        'datos': [],
                        'ts': now,
                        'area': self.area,
                        'plc_ok': True
                    })

                    if est in system_monitor['estaciones']:
                        system_monitor['estaciones'][est].update({
                            'parte_actual': '--',
                            'contador': 0,
                            'tiempo_ciclo': 0.0,
                            'ultima_actualizacion': now,
                            'validado': True,
                            'error_validacion': None
                        })

            if batch and self.ip in ip_data_queues:
                try: ip_data_queues[self.ip].put_nowait(batch)
                except asyncio.QueueFull: pass

                if self.ip in system_monitor['ips']:
                    system_monitor['ips'][self.ip].update({'conectado': True, 'ultima_lectura': now})

        except Exception as e:
            system_monitor['estadisticas']['errores_conexion'] += 1
            raise

# ═══════════════════════════ PROCESADOR (CONSUMIDOR) ═══════════════════════════

class IPDataProcessor:
    def __init__(self, ip):
        self.ip = ip
        self.active_records = {}
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
                if rec_copy.get('id_registro') is None: rec_copy['id_registro'] = 0
                serializable_data[clave] = rec_copy

            with open(self.state_file, 'w') as f:
                json.dump(serializable_data, f, indent=4)
        except Exception as e:
            logger.error(f"Error guardando estado para {self.ip}: {e}")

    async def process_continuously(self):
        if self.ip not in ip_data_queues: return
        while True:
            try:
                batch = await ip_data_queues[self.ip].get()
                for pkg in batch:
                    await self._process_estacion(pkg)
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
        conn = create_connection()
        db_strategy = DBStrategyFactory.get_strategy(area, log)

        state_changed = False

        try:
            with conn.cursor() as cursor:
                hora = now.time().replace(microsecond=0)

                if time(8,0) <= hora < time(20,0):
                    turno = 1
                    fecha_plan = now.date()
                elif hora >= time(20,0):
                    turno = 2
                    fecha_plan = now.date()
                else:
                    turno = 2
                    fecha_plan = now.date() - timedelta(days=1)

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
                    claves_actuales_en_plc.add(f"{estacion}_{d['parte']}")

                claves_obsoletas = []
                for k in self.active_records:
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
                    validado = d.get('validado', True)
                    error_val = d.get('error_validacion', None)

                    if not validado:
                        log.warning(f"⚠️ Número de parte NO VALIDADO: {num_orig} - Error: {error_val}")
                        continue

                    clave = f"{estacion}_{num}"

                    if clave not in self.active_records:
                        id_reg, q_plan, q_prod, status, prod_start_db, mult = obtener_id_registro_activo(
                            cursor, estacion, fecha_plan, turno, num, log
                        )

                        corrida_previa = 0
                        if id_reg and status == 8:
                            corrida_previa = q_prod or 0
                            q_prod = 0

                        if id_reg is None or status == 8:
                            id_reg, q_plan, q_prod, mult = crear_nuevo_registro(
                                cursor, num, estacion, 0, turno, fecha_fmt, fecha_plan, num_orig, log
                            )
                            if id_reg is not None and status != 8:
                                 corrida_previa = 0
                            prod_start_db = now

                        if id_reg is None:
                            continue

                        mult = mult or 1

                        if (q_prod or 0) > 0:
                            cnt_start_init = cnt
                        else:
                            cnt_start_init = 0

                        self.active_records[clave] = {
                            'id_registro': id_reg,
                            'quantity_planeada': q_plan,
                            'corrida_previa': corrida_previa,
                            'multiplicador': mult,
                            'contador_registro': cnt,
                            'cnt_turn_start': cnt_start_init,
                            'hora_cambio': hora,
                            'numero_original': num_orig
                        }
                        state_changed = True

                    reg = self.active_records[clave]
                    if reg['id_registro'] is None:
                        continue

                    cambio_turno = (
                        (reg["hora_cambio"] < time(8, 0) <= hora) or
                        (reg["hora_cambio"] < time(20, 0) <= hora)
                    )

                    if cambio_turno:
                        cnt_turn_start_base = reg.get('contador_registro', cnt)

                        try:
                            cursor.execute(
                                "UPDATE production_records SET status_id=8, production_end=? WHERE id=?",
                                (fecha_fmt, reg['id_registro'])
                            )
                        except Exception as e:
                            log.error(f"Error cerrando registro en cambio de turno: {e}")

                        id_reg2, q_plan2, q_prod2, status2, prod_start_db2, mult2 = obtener_id_registro_activo(
                            cursor, estacion, fecha_plan, turno, num, log
                        )

                        corrida_previa2 = 0
                        if id_reg2 and status2 == 8:
                            corrida_previa2 = q_prod2 or 0
                            q_prod2 = 0

                        if id_reg2 is None or status2 == 8:
                            id_reg2, q_plan2, q_prod2, mult2 = crear_nuevo_registro(
                                cursor, num, estacion, 0, turno, fecha_fmt, fecha_plan, num_orig, log
                            )
                            if id_reg2 is not None and status2 != 8:
                                 corrida_previa2 = 0

                        if id_reg2 is None:
                            log.error(f"No se pudo crear registro para nuevo turno: {num}")
                            continue

                        reg['id_registro'] = id_reg2
                        reg['quantity_planeada'] = q_plan2
                        reg['corrida_previa'] = corrida_previa2
                        reg['multiplicador'] = mult2 or 1
                        reg['hora_cambio'] = hora
                        reg['cnt_turn_start'] = cnt_turn_start_base

                        state_changed = True

                    prev = reg.get("contador_registro", 0)

                    if cnt != prev:
                        multiplicador = reg.get("multiplicador", 1)
                        corrida_previa = reg.get("corrida_previa", 0)

                        golpes_turno_acumulados = cnt - reg.get('cnt_turn_start', cnt)

                        if golpes_turno_acumulados < 0:
                            log.warning(f"⚠️ Reset o error de contador detectado en {num}: {prev} -> {cnt}. Reestableciendo base.")
                            reg["cnt_turn_start"] = 0
                            golpes_turno_acumulados = cnt

                        prod_turno = golpes_turno_acumulados * multiplicador
                        qty_upd = prod_turno + corrida_previa

                        actualizar_registro(cursor, qty_upd, fecha_fmt, reg['id_registro'], 7, log)

                        pid = obtener_part_number_id(cursor, num, estacion)
                        if pid:
                            incremento_ciclo = (cnt - prev)
                            cantidad_historial = incremento_ciclo * multiplicador

                            extras = {'troquel_id': troquel_id}
                            cantidad_history = cnt if "estampado" in str(area).lower() else cantidad_historial

                            if cantidad_history > 0:
                                db_strategy.insertar_history(cursor, pid, cantidad_history, fecha_fmt, tiempo, extras)

                        if reg.get('corrida_previa', 0) > 0:
                            reg['corrida_previa'] = 0

                        reg['contador_registro'] = cnt
                        reg['hora_cambio'] = hora
                        state_changed = True

                conn.commit()

                if state_changed:
                    self.save_state()

        except Exception as e:
            log.error(f"❌ Error procesando {estacion}: {e}")
            log.error(traceback.format_exc())
        finally:
            conn.close()

# ═══════════════════════════ ASYNCIO & MAIN LOOPS ═══════════════════════════

ip_data_queues = {}
ip_processors = {}
system_monitor = {'ips': {}, 'estaciones': {}, 'estadisticas': {'errores_conexion': 0, 'inicio_sistema': datetime.now()}}

async def plc_reader(ip, port, group_info):
    plc = Type3E()
    plc.network = 0
    plc.pc = 0xFF
    plc.timer = 30  # 🔥 AUMENTADO: Tiempo de espera para respuesta del PLC (30 segundos)

    # 🔥 NUEVO: Configurar socket timeout directamente
    try:
        plc.soc_timeout = 10.0  # Timeout del socket en segundos
    except:
        pass

    connected = False
    collector = IPDataCollector(ip, group_info['estaciones'], group_info.get('area', 'Default'))

    if ip not in ip_data_queues:
        ip_data_queues[ip] = asyncio.Queue(maxsize=1000)

    while True:
        try:
            if not connected:
                for est in group_info['estaciones']:
                    get_station_logger(est).warning(f"Conectando PLC {ip}...")

                await asyncio.to_thread(plc.connect, ip, port)
                connected = True
                system_monitor['ips'][ip] = {
                    'conectado': True,
                    'estaciones': group_info['estaciones'],
                    'procesando': False,
                    'ultima_lectura': datetime.now()
                }

            await collector.collect_and_enqueue(plc, group_info)
            await asyncio.sleep(1)  # 🔥 CAMBIADO: De 0.1 a 1 segundo entre lecturas

        except Exception as e:
            connected = False
            error_msg = str(e)

            # 🔥 NUEVO: Mensajes de error más descriptivos
            if "Invalid device" in error_msg:
                for est in group_info['estaciones']:
                    get_station_logger(est).error(f"❌ PLC {ip} - DIRECCIÓN INVÁLIDA: {error_msg}")
            elif "timed out" in error_msg:
                for est in group_info['estaciones']:
                    get_station_logger(est).error(f"⏱️ PLC {ip} - TIMEOUT: No responde en el tiempo límite")
            else:
                for est in group_info['estaciones']:
                    get_station_logger(est).error(f"⚠️ PLC OFFLINE {ip}: {e}")

            if ip in system_monitor['ips']:
                system_monitor['ips'][ip]['conectado'] = False

            if ip in ip_data_queues:
                for est in group_info['estaciones']:
                    try:
                        ip_data_queues[ip].put_nowait([{
                            'estacion': est,
                            'datos': [],
                            'ts': datetime.now(),
                            'area': group_info.get('area', 'Default'),
                            'plc_ok': False
                        }])
                    except asyncio.QueueFull:
                        pass

            await asyncio.sleep(5)

async def supervisor():
    tasks = {}
    while True:
        ip_groups = load_config()

        for ip, info in ip_groups.items():
            if ip not in tasks:
                tasks[ip] = asyncio.create_task(plc_reader(ip, info['port'], info))
                if ip not in ip_processors:
                    proc = IPDataProcessor(ip)
                    ip_processors[ip] = proc
                    asyncio.create_task(proc.process_continuously())

        await asyncio.sleep(POLL_INTERVAL)

# ═══════════════════════════ UI MODERNA ═══════════════════════════

class ModernDashboardUI:
    def __init__(self):
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("dark-blue")

        self.root = ctk.CTk()
        self.root.title("🏭 IOT MONITOR - FACTORY DASHBOARD")
        self.root.geometry("1600x900")

        self.root.grid_columnconfigure(1, weight=1)
        self.root.grid_rowconfigure(0, weight=1)

        self.search_var = StringVar()
        self.search_var.trace("w", lambda *args: self.filter_stations())

        # 🔥 NUEVO: Diccionario para mantener referencias a items del tree
        self.station_items = {}

        self.create_sidebar()
        self.create_main()
        self.update_loop()

    def create_sidebar(self):
        sb = ctk.CTkFrame(self.root, width=240, corner_radius=0)
        sb.grid(row=0, column=0, sticky="nsew")
        sb.grid_propagate(False)

        header_frame = ctk.CTkFrame(sb, fg_color="transparent")
        header_frame.pack(pady=20, padx=10)

        ctk.CTkLabel(header_frame, text="🏭", font=("Arial", 40)).pack()
        ctk.CTkLabel(header_frame, text="MONITOR", font=("Arial", 22, "bold")).pack()
        ctk.CTkLabel(header_frame, text="PRENSAS", font=("Arial", 18)).pack()

        separator = ctk.CTkFrame(sb, height=2, fg_color="#3a3a3a")
        separator.pack(fill="x", padx=20, pady=10)

        stats_frame = ctk.CTkFrame(sb, fg_color="transparent")
        stats_frame.pack(pady=10, padx=15, fill="x")

        self.lbl_ips = ctk.CTkLabel(
            stats_frame,
            text="🌐 IPs: 0",
            font=("Arial", 16),
            anchor="w"
        )
        self.lbl_ips.pack(pady=8, fill="x")

        self.lbl_est = ctk.CTkLabel(
            stats_frame,
            text="📍 Estaciones: 0",
            font=("Arial", 16),
            anchor="w"
        )
        self.lbl_est.pack(pady=8, fill="x")

        self.lbl_online = ctk.CTkLabel(
            stats_frame,
            text="✅ Online: 0",
            font=("Arial", 16),
            anchor="w"
        )
        self.lbl_online.pack(pady=8, fill="x")

        self.lbl_offline = ctk.CTkLabel(
            stats_frame,
            text="❌ Offline: 0",
            font=("Arial", 16),
            anchor="w"
        )
        self.lbl_offline.pack(pady=8, fill="x")

        footer_frame = ctk.CTkFrame(sb, fg_color="transparent")
        footer_frame.pack(side="bottom", pady=20, padx=15)

        self.lbl_uptime = ctk.CTkLabel(
            footer_frame,
            text="⏱️ Uptime: 00:00:00",
            font=("Arial", 13),
            text_color="#888888"
        )
        self.lbl_uptime.pack()

    def create_main(self):
        main = ctk.CTkFrame(self.root, fg_color="transparent")
        main.grid(row=0, column=1, sticky="nsew", padx=15, pady=15)
        main.grid_rowconfigure(1, weight=1)
        main.grid_columnconfigure(0, weight=1)

        search_frame = ctk.CTkFrame(main)
        search_frame.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        search_frame.grid_columnconfigure(1, weight=1)

        ctk.CTkLabel(
            search_frame,
            text="🔍",
            font=("Arial", 20)
        ).grid(row=0, column=0, padx=(10, 5), pady=10)

        self.search_entry = ctk.CTkEntry(
            search_frame,
            placeholder_text="Buscar por estación o IP...",
            textvariable=self.search_var,
            height=40,
            font=("Arial", 14)
        )
        self.search_entry.grid(row=0, column=1, sticky="ew", padx=(0, 10), pady=10)

        tree_frame = ctk.CTkFrame(main)
        tree_frame.grid(row=1, column=0, sticky="nsew")

        # 🔥 CAMBIO: Sin columna "Detalle"
        cols = ("Estación", "Área", "IP", "Parte Original", "Contador", "Estado")

        scrollbar = ctk.CTkScrollbar(tree_frame)
        scrollbar.pack(side="right", fill="y")

        style = ttk.Style()
        style.theme_use("clam")
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

    def filter_stations(self):
        """Filtrar estaciones por búsqueda"""
        search_text = self.search_var.get().lower()

        # 🔥 CAMBIO: No limpiar, solo actualizar items existentes
        items = sorted(system_monitor['estaciones'].items(), key=lambda x: x[0])

        for est, info in items:
            if search_text and search_text not in est.lower() and search_text not in info.get('ip', '').lower():
                # Ocultar item si no coincide con búsqueda
                if est in self.station_items:
                    self.tree.delete(self.station_items[est])
                    del self.station_items[est]
                continue

            diff = (datetime.now() - info.get('ultima_actualizacion', datetime.min)).total_seconds()

            ip = info.get('ip', '--')
            plc_connected = system_monitor['ips'].get(ip, {}).get('conectado', False)

            # 🔥 CAMBIO: Solo OFFLINE cuando no hay conexión con PLC
            if not plc_connected:
                status = "🔴 PLC OFFLINE"
            elif diff < 60:
                status = "🟢 Online"
            else:
                status = "🟡 Sin datos"

            vals = (
                est,
                info.get('area', 'N/A'),
                ip,
                info.get('parte_actual', '--'),
                info.get('contador', 0),
                status
            )

            # 🔥 CAMBIO: Actualizar item existente o crear nuevo
            if est in self.station_items:
                self.tree.item(self.station_items[est], values=vals)
            else:
                item_id = self.tree.insert("", "end", values=vals)
                self.station_items[est] = item_id

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

        self.filter_stations()

        self.root.after(1000, self.update_loop)

    def run(self):
        self.root.mainloop()

# ═══════════════════════════ MAIN ═══════════════════════════

def start_async():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(supervisor())

if __name__ == '__main__':

    t = threading.Thread(target=start_async, daemon=True)
    t.start()
    app = ModernDashboardUI()
    app.run()