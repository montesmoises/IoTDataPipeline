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
from typing import Optional

# Directorios
LOGS_DIR = Path("logs")
LOGS_DIR.mkdir(exist_ok=True)
CSV_DIR = Path("part_numbers_not_found")
CSV_DIR.mkdir(exist_ok=True)

# Configurar logging - SIN handlers globales para evitar archivos no deseados
logging.basicConfig(level=logging.NOTSET, handlers=[])

# Logger principal solo para mensajes del supervisor (SIN archivo, solo consola)
logger = logging.getLogger("supervisor")
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - SUPERVISOR - %(message)s'))
logger.addHandler(console_handler)
logger.propagate = False

# Diccionario para almacenar loggers por estación
station_loggers = {}

def get_station_logger(estacion):
    """Obtiene o crea un logger específico para una estación (todos los niveles)"""
    if estacion in station_loggers:
        return station_loggers[estacion]

    # Crear nuevo logger para la estación
    station_logger = logging.getLogger(f"station.{estacion}")
    station_logger.setLevel(logging.DEBUG)  # Captura TODOS los niveles
    station_logger.propagate = False  # NO propagar al logger raíz

    # Handler de archivo - TODOS los niveles
    log_path = LOGS_DIR / f"{estacion}.log"
    file_handler = logging.FileHandler(log_path, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)  # TODOS los niveles al archivo

    # Formato sin nombre de logger (ya está en el nombre del archivo)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)

    station_logger.addHandler(file_handler)

    # Handler de consola para ver también en tiempo real (opcional)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)  # Solo INFO+ en consola para no saturar
    console_formatter = logging.Formatter(f'%(asctime)s - %(levelname)s - {estacion} - %(message)s')
    console_handler.setFormatter(console_formatter)
    station_logger.addHandler(console_handler)

    # Guardar en cache
    station_loggers[estacion] = station_logger
    logger.info(f"Logger creado para estación: {estacion}")

    return station_logger

# Intervalo (en segundos) para volver a leer la configuración de la BD
POLL_INTERVAL = 3

# ─────── ESTRUCTURAS AUXILIARES ───────
PARTES = ['LH', 'RH']

# Global dict para comunicación entre readers y processors
plc_data_queues = {}  # {ip: asyncio.Queue()}
plc_data_latest = {}  # {ip: {estacion: datos}}

# Archivo CSV de números de parte no encontrados
CSV_FILE = CSV_DIR / "parts_not_found.csv"

# ────────────────────────────────────────── FUNCIONES DE BD ─────────────────────────────────────────────────────

def create_connection():
    """Crea conexión a SQL Server"""
    # connection = pyodbc.connect(
    #     'DRIVER={ODBC Driver 17 for SQL Server};'
    #     'SERVER=192.168.130.87;'
    #     'DATABASE=test_iot;'
    #     'UID=sa;'
    #     'PWD=Alcala91'
    # )

    connection = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=192.168.130.47;' # cambiar ip de migue
        'DATABASE=IOT_YKM;'
        'UID=sa;'
        'PWD=Password9'
    )
    return connection

def crear_conexion_as400(host: str = "192.168.200.7", user: str = "QSECOFR",
                        password: str = "AS400300", database: str = "") -> Optional[pyodbc.Connection]:
    """Crea conexión a AS400 usando ODBC."""
    conn_str = (
        "DRIVER={iSeries Access ODBC Driver};"
        f"SYSTEM={host};UID={user};PWD={password};"
    )
    if database:
        conn_str += f"DBQ={database};"
    try:
        return pyodbc.connect(conn_str)
    except pyodbc.Error as e:
        logger.error(f"Error en conexión AS400: {e}")
        return None

def obtener_multiplicador_as400(numero_parte: str, estacion_logger=None) -> int:
    """Obtiene el multiplicador desde AS400"""
    log = estacion_logger if estacion_logger else logger

    conn_as400 = crear_conexion_as400()
    if not conn_as400:
        log.warning(f"No se pudo conectar a AS400 para obtener multiplicador de {numero_parte}")
        return 1  # Valor por defecto

    try:
        cursor = conn_as400.cursor()
        sql = """
        SELECT I.IUFD11
        FROM LX834F01.IIU AS I
        WHERE RTRIM(I.IUPROD) = ?
        AND I.IUSEQN = 2
        """
        cursor.execute(sql, (numero_parte,))
        result = cursor.fetchone()

        if result and result[0] is not None:
            multiplicador = int(result[0])
            log.info(f"Multiplicador obtenido de AS400 - Parte: {numero_parte}, Multiplicador: {multiplicador}")
            return multiplicador
        else:
            log.warning(f"No se encontró multiplicador en AS400 para {numero_parte}, usando valor por defecto: 1")
            return 1

    except Exception as e:
        log.error(f"Error obteniendo multiplicador de AS400 para {numero_parte}: {str(e)}")
        return 1
    finally:
        conn_as400.close()

def load_config():
    """
    Carga configuración y la agrupa por IP para optimizar las conexiones PLC
    """
    sql = """
    SELECT
      wc.name          AS work_center_name,
      wc.ip            AS work_center_ip,
      tt.name          AS tag_type_name,
      t.address        AS tag_address,
      t.long           AS tag_long
    FROM work_centers wc
    JOIN tags t        ON wc.id = t.work_center_id
    JOIN tag_types tt  ON t.tag_type_id = tt.id
    """
    conn   = create_connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    conn.close()

    ips      = {}
    ports    = {}
    series   = {}
    configs  = {}
    # Nueva estructura: agrupado por IP
    ip_groups = defaultdict(lambda: {
        'estaciones': [],
        'port': 1025,
        'serie': 'Q',
        'all_addresses': set(),
        'station_configs': {}
    })

    for wc, ip, tag, addr, lng in rows:
        ips.setdefault(wc, ip)
        key = tag.lower()

        if key == "puerto":
            ports[wc] = int(addr)
            ip_groups[ip]['port'] = int(addr)
        elif key == "serie plc":
            series[wc] = addr
            ip_groups[ip]['serie'] = addr
        else:
            configs.setdefault(wc, {})[tag] = {
                "address": addr,
                "long":    int(lng),
            }
            # Agregar direcciones al conjunto global por IP
            ip_groups[ip]['all_addresses'].update(expand_block(addr, int(lng)))

        # Agregar estación al grupo IP si no está ya
        if wc not in ip_groups[ip]['estaciones']:
            ip_groups[ip]['estaciones'].append(wc)

        # Guardar config por estación
        ip_groups[ip]['station_configs'][wc] = configs.get(wc, {})

    return ips, ports, series, configs, dict(ip_groups)

def actualizar_registro(cursor, contador, fecha_formateada, record_id, status, estacion_logger=None):
    """Actualiza el registro de producción con el nuevo contador y fecha de finalización."""
    log = estacion_logger if estacion_logger else logger

    sql_update = '''
        UPDATE production_records
        SET 
            produced_quantity = ?,
            production_end = ?,
            status_id = ?
        WHERE id = ?
    '''
    cursor.execute(sql_update, (contador, fecha_formateada, status, record_id))
    log.info(f"Registro actualizado - ID: {record_id}, Cantidad: {contador}, Status: {status}")

def guardar_parte_no_encontrada(estacion, numero_parte, numero_parte_original, timestamp, estacion_logger=None):
    """
    Guarda números de parte no encontrados en un archivo CSV.
    Solo agrega el número de parte si no existe para esa estación y fecha.
    """
    log = estacion_logger if estacion_logger else logger

    try:
        fecha_str = timestamp.strftime('%Y-%m-%d')

        # Crear registro
        record = {
            'estacion': estacion,
            'numero_parte': numero_parte,
            'numero_parte_original': numero_parte_original,
            'fecha': fecha_str,
            'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S')
        }

        # Si existe el archivo, verificar duplicados; sino crear nuevo
        if CSV_FILE.exists():
            try:
                existing_df = pd.read_csv(CSV_FILE)

                # Verificar si ya existe esta combinación estación-parte-fecha
                duplicated = existing_df[
                    (existing_df['estacion'] == estacion) &
                    (existing_df['numero_parte'] == numero_parte) &
                    (existing_df['fecha'] == fecha_str)
                ]

                if duplicated.empty:
                    # Agregar nueva fila
                    new_row = pd.DataFrame([record])
                    combined_df = pd.concat([existing_df, new_row], ignore_index=True)
                    combined_df.to_csv(CSV_FILE, index=False)
                    log.info(f"Parte no encontrada guardada en CSV - Parte: {numero_parte}, Original: {numero_parte_original}")
                else:
                    log.debug(f"Parte ya existe en CSV para hoy - Parte: {numero_parte}")

            except Exception as e:
                log.error(f"Error leyendo archivo CSV existente: {str(e)}")
                # Crear nuevo archivo
                new_df = pd.DataFrame([record])
                new_df.to_csv(CSV_FILE, index=False)
        else:
            # Crear nuevo archivo
            new_df = pd.DataFrame([record])
            new_df.to_csv(CSV_FILE, index=False)
            log.info(f"Archivo CSV creado con nueva parte - Parte: {numero_parte}, Original: {numero_parte_original}")

    except Exception as e:
        log.error(f"Error guardando parte no encontrada en CSV - Parte: {numero_parte}, Error: {str(e)}")

def obtener_id_registro_activo(cursor, estacion, fecha_ajustada, turno, numero_parte, estacion_logger=None):
    """Busca un registro activo y devuelve su ID, planned_quantity, produced_quantity, status y multiplicador desde AS400 si existe"""
    log = estacion_logger if estacion_logger else logger

    sql_active_record = '''
        SELECT TOP(1)
            pr.id,
            pr.planned_quantity,
            pr.produced_quantity,
            pr.status_id
        FROM production_records pr
        JOIN part_numbers pn ON pr.part_number_id = pn.id
        JOIN work_centers wc ON pn.work_center_id = wc.id
        WHERE
            wc.name           = ?
            AND pn.number     = ?
            AND pr.planned_date = ?
            AND pr.shift_id     = ?
            AND pr.status_id    IN (3, 8)
        ORDER BY
            pr.status_id DESC, pr.id DESC
    '''
    cursor.execute(sql_active_record, (estacion, numero_parte, fecha_ajustada, turno))
    result = cursor.fetchone()
    if result:
        # Obtener multiplicador desde AS400
        multiplicador = obtener_multiplicador_as400(numero_parte, log)

        log.info(f"Registro activo encontrado - Parte: {numero_parte}, ID: {result[0]}, Status: {result[3]}, Multiplicador: {multiplicador}")
        return result[0], result[1], result[2], result[3], multiplicador
    else:
        log.info(f"No se encontró registro activo - Parte: {numero_parte}")
        return (None, None, None, None, None)

def crear_nuevo_registro(cursor, numero_parte, estacion, contador, turno, fecha_formateada, fecha_ajustada, timestamp=None, numero_parte_original=None, estacion_logger=None):
    """Crea un nuevo registro si el número de parte existe en el work center y no es obsoleto. Retorna también el multiplicador desde AS400."""
    log = estacion_logger if estacion_logger else logger

    sql_insert = '''
        INSERT INTO production_records 
            (part_number_id, produced_quantity, shift_id, production_start, status_id, planned_date)
        OUTPUT INSERTED.id, INSERTED.planned_quantity, INSERTED.produced_quantity
        SELECT 
            pn.id, ?, ?, ?, 3, ?
        FROM part_numbers pn
        JOIN work_centers wc ON pn.work_center_id = wc.id
        WHERE 
            pn.number = ?
            AND wc.name = ?
            AND pn.is_obsolete = 0;
    '''

    try:
        cursor.execute(sql_insert, (contador, turno, fecha_formateada, fecha_ajustada, numero_parte, estacion))
        result = cursor.fetchone()
        if result:
            # Obtener multiplicador desde AS400
            multiplicador = obtener_multiplicador_as400(numero_parte, log)

            log.info(f"Nuevo registro creado - Parte: {numero_parte}, ID: {result[0]}, Multiplicador: {multiplicador}")
            return result[0], result[1], result[2], multiplicador  # (id, planned_quantity, produced_quantity, multiplicador)
        else:
            log.warning(f"No se pudo crear registro - Número de parte no existe o está obsoleto: {numero_parte}")

            # Guardar número de parte no encontrado en CSV
            if timestamp is None:
                timestamp = datetime.now()
            if numero_parte_original is None:
                numero_parte_original = numero_parte  # Si no se proporciona, usar el limpiado

            guardar_parte_no_encontrada(estacion, numero_parte, numero_parte_original, timestamp, log)

            return (None, None, None, None)
    except Exception as e:
        log.error(f"Error al crear nuevo registro - Parte: {numero_parte}, Error: {str(e)}")

        # También guardar en CSV cuando hay error
        if timestamp is None:
            timestamp = datetime.now()
        if numero_parte_original is None:
            numero_parte_original = numero_parte

        guardar_parte_no_encontrada(estacion, numero_parte, numero_parte_original, timestamp, log)

        return (None, None, None, None)

def obtener_part_number_id(cursor, numero_parte, estacion):
    """Obtiene el ID del part_number para insertar en histories"""
    sql = '''
        SELECT pn.id
        FROM part_numbers pn
        JOIN work_centers wc ON pn.work_center_id = wc.id
        WHERE pn.number = ? AND wc.name = ?
    '''
    cursor.execute(sql, (numero_parte, estacion))
    result = cursor.fetchone()
    return result[0] if result else None

def insertar_history(cursor, part_number_id, cantidad, fecha_formateada, tiempo, estacion_logger=None):
    """Inserta registro en la tabla histories"""
    log = estacion_logger if estacion_logger else logger

    if part_number_id is None:
        log.warning("No se puede insertar en histories - part_number_id es None")
        return

    sql_insert = '''
        INSERT INTO histories (part_number_id, quantity, created_at, production_per_cycle)
        VALUES (?, ?, ?, ?);
    '''
    try:
        cursor.execute(sql_insert, (part_number_id, cantidad, fecha_formateada, tiempo))
        log.info(f"History insertado - Part ID: {part_number_id}, Cantidad: {cantidad}, Tiempo: {tiempo}")
    except Exception as e:
        log.error(f"Error al insertar history - Part ID: {part_number_id}, Error: {str(e)}")

# ────────────────────────────────────────────────────────────────────────────────────────────────────────────────

def combinar_listas(cadena_limpia, cadenas_originales, contadores, tiempos):
    """
    Combina las listas y suma contadores para números de parte duplicados, manteniendo originales.
    Ahora también guarda y registra la lista de contadores individuales para cada parte duplicada.
    """
    datos_temp = []
    for i, cadena in enumerate(cadena_limpia):
        original = cadenas_originales[i] if i < len(cadenas_originales) else cadena

        if isinstance(cadena, list):
            for subcadena in cadena:
                datos_temp.append((subcadena, original, contadores[i], tiempos[i]))
        elif cadena is not None and cadena != '':
            datos_temp.append((cadena, original, contadores[i], tiempos[i]))

    datos_filtrados = [item for item in datos_temp if item[0] not in (None, '')]

    # datos_combinados: parte -> {'parte':..., 'original':..., 'sum':..., 'counters': [...], 'tiempo': ...}
    datos_combinados = {}
    for parte, original, contador, tiempo in datos_filtrados:
        if parte in datos_combinados:
            datos_combinados[parte]['counters'].append(contador)
            datos_combinados[parte]['sum'] += contador
            # Mantener el tiempo (puede sobreescribirse por último)
            datos_combinados[parte]['tiempo'] = tiempo
            # Loguear con detalle de cada contador
            logger.debug(
                f"Números de parte duplicados combinados - Parte: {parte}, Contador total: {datos_combinados[parte]['sum']}, "
                f"Contadores: {datos_combinados[parte]['counters']}"
            )
        else:
            datos_combinados[parte] = {
                'parte': parte,
                'original': original,
                'sum': contador,
                'counters': [contador],
                'tiempo': tiempo
            }

    # Retornar como lista de tuplas (parte_limpia, parte_original, contador_total, tiempo)
    datos_finales = [(v['parte'], v['original'], v['sum'], v['tiempo']) for v in datos_combinados.values()]
    return datos_finales

def limpiar_cadena(cadena):
    cadena_limpia = cadena.replace("\x00", "")
    if '/' in cadena_limpia:
        partes = [parte.split('/') for parte in cadena_limpia.split(' ')]
        return [''.join(combinacion) for combinacion in product(*partes)]
    else:
        return cadena_limpia.replace(" ", "")

def decodificar_bloque(bloque):
    """
    Dado un bloque (lista de enteros) del PLC, devuelve la cadena ASCII original y limpia
    """
    if bloque is None:
        return None, None

    # Extraer caracteres sin limpiar primero
    chars = [
        chr(valor & 0xFF) + chr((valor >> 8) & 0xFF)
        for valor in bloque
    ]
    cadena_original = "".join(chars).replace("\x00", "")  # Solo quitar nulls
    cadena_limpia = limpiar_cadena(cadena_original)

    return cadena_original, cadena_limpia

def expand_block(address: str, length: int) -> list[str]:
    """Dado un address tipo "D3100" y un length n, devuelve ["D3100","D3101",...,"D3100+(n-1)"]"""
    prefix = ''.join(ch for ch in address if not ch.isdigit())
    num    = int(''.join(ch for ch in address if ch.isdigit()))
    return [f"{prefix}{num + i}" for i in range(length)]

# ────────────────────────────────────────── LÓGICA DE LECTURA PLC ─────────────────────────────────────────────────────

async def plc_reader(ip, port, plctype, ip_group_info):
    """
    Hilo lector que conecta a UN PLC y lee TODOS los tags necesarios para todas las estaciones
    que comparten esa IP. Distribuye los datos a las estaciones correspondientes.
    """
    plc = Type3E(plctype=plctype)
    plc.soc_timeout = 5

    try:
        await asyncio.to_thread(plc.connect, ip, port)
        logger.info(f"PLC Reader conectado - IP: {ip}, Puerto: {port}, Estaciones: {ip_group_info['estaciones']}")
    except Exception as e:
        logger.error(f"Error conectando PLC Reader - IP: {ip}, Error: {str(e)}")
        return

    # Inicializar queue para esta IP
    if ip not in plc_data_queues:
        plc_data_queues[ip] = asyncio.Queue()

    try:
        while True:
            start_time = system_time.time()

            try:
                # Leer TODAS las direcciones necesarias para esta IP de una vez
                all_addresses = list(ip_group_info['all_addresses'])
                if not all_addresses:
                    await asyncio.sleep(1)
                    continue

                # Leer todos los valores de una vez
                all_values, _ = plc.randomread(
                    word_devices=all_addresses,
                    dword_devices=[]
                )

                # Crear diccionario de dirección -> valor
                address_values = dict(zip(all_addresses, all_values))

                # Distribuir datos por estación
                estacion_data = {}
                timestamp = datetime.now()

                for estacion in ip_group_info['estaciones']:
                    station_config = ip_group_info['station_configs'].get(estacion, {})

                    # Procesar datos para esta estación específica
                    grupos = []
                    for parte in PARTES:
                        key = f"Contador {parte}"
                        if key not in station_config:
                            continue
                        addr_c, len_c = station_config[key]["address"], station_config[key]["long"]
                        info_ppc = station_config.get(f"Tiempo Ciclo {parte}", {})
                        addr_p, len_p = info_ppc.get("address"), info_ppc.get("long", 1)
                        info_np = station_config.get(f"Número de Parte {parte}", {})
                        addr_n, len_n = info_np.get("address"), info_np.get("long", 1)
                        grupos.append((parte, addr_c, len_c, addr_p, len_p, addr_n, len_n))

                    # Extraer contadores para esta estación
                    contadores = []
                    for _, addr_c, len_c, _, _, _, _ in grupos:
                        for addr in expand_block(addr_c, len_c):
                            contadores.append(address_values.get(addr, 0))

                    # Extraer tiempos para esta estación
                    tiempos = []
                    for _, _, _, addr_p, len_p, _, _ in grupos:
                        if addr_p:
                            for addr in expand_block(addr_p, len_p):
                                raw = address_values.get(addr, 0)
                                try:
                                    tiempos.append(abs(int(raw) / 1000))
                                except (ValueError, TypeError):
                                    tiempos.append(0.0)
                        else:
                            tiempos.append(0.0)

                    # Extraer números de parte para esta estación
                    cadenas = []
                    cadenas_originales = []
                    for _, _, _, _, _, addr_n, len_n in grupos:
                        if addr_n:
                            bloque_values = []
                            for addr in expand_block(addr_n, len_n):
                                bloque_values.append(address_values.get(addr, 0))

                            if bloque_values:
                                original, limpia = decodificar_bloque(bloque_values)
                                cadenas.append(limpia)
                                cadenas_originales.append(original)
                            else:
                                cadenas.append("")
                                cadenas_originales.append("")
                        else:
                            cadenas.append("")
                            cadenas_originales.append("")

                    # Combinar datos para esta estación (ahora incluye originales)
                    datos = combinar_listas(cadenas, cadenas_originales, contadores, tiempos)

                    estacion_data[estacion] = {
                        'datos': datos,
                        'timestamp': timestamp
                    }

                # Actualizar datos globales
                plc_data_latest[ip] = estacion_data

                # Notificar a processors
                try:
                    plc_data_queues[ip].put_nowait((ip, estacion_data))
                except asyncio.QueueFull:
                    logger.warning(f"Queue full para IP {ip}, descartando datos antiguos")
                    try:
                        plc_data_queues[ip].get_nowait()  # Remover dato viejo
                        plc_data_queues[ip].put_nowait((ip, estacion_data))
                    except asyncio.QueueEmpty:
                        pass

                logger.debug(f"Datos leídos y distribuidos - IP: {ip}, Estaciones: {list(estacion_data.keys())}")

            except Exception as e:
                logger.error(f"Error leyendo datos del PLC - IP: {ip}, Error: {str(e)}")
                await asyncio.sleep(2)
                continue

            # Control de ritmo
            elapsed = system_time.time() - start_time
            await asyncio.sleep(max(1 - elapsed, 1))

    except Exception as e:
        logger.error(f"Error general en PLC Reader - IP: {ip}, Error: {str(e)}")
    finally:
        try:
            if plc and plc._is_connected:
                await asyncio.to_thread(plc.close)
                logger.info(f"PLC Reader cerrado - IP: {ip}")
        except Exception as e:
            logger.error(f"Error cerrando PLC Reader - IP: {ip}, Error: {str(e)}")

async def plc_processor(estacion, ip):
    """
    Procesador que recibe datos del reader correspondiente y los procesa en BD.
    Usa logger específico por estación que escribe en logs/<estacion>.log
    """
    # Obtener logger específico para esta estación
    station_logger = get_station_logger(estacion)

    active_records = {}
    conn = create_connection()
    station_logger.info(f"Processor conectado a BD - IP: {ip}")

    # Variable para controlar si ya se ejecutó la limpieza cuando no hay datos
    limpieza_ejecutada = False

    # Asegurar que existe la queue
    if ip not in plc_data_queues:
        plc_data_queues[ip] = asyncio.Queue()

    try:
        while True:
            try:
                # Esperar datos del reader (timeout de 5 segundos)
                _, estacion_data = await asyncio.wait_for(
                    plc_data_queues[ip].get(),
                    timeout=5.0
                )

                # Verificar si hay datos para esta estación
                if estacion not in estacion_data:
                    continue

                station_info = estacion_data[estacion]
                datos = station_info['datos']
                now = station_info['timestamp']

                # Turno y fecha
                hora = now.time().replace(microsecond=0)
                if time(8,0) <= hora < time(16,0):
                    turno = 1
                    fecha_plan = now.date()
                else:
                    turno = 2
                    fecha_plan = now.date() if hora >= time(16,0) else (now.date() - timedelta(days=1))

                # Si no hay datos -> limpieza (solo una vez hasta que vuelvan a llegar datos)
                with conn.cursor() as cursor:
                    if not datos:
                        if not limpieza_ejecutada:
                            station_logger.info("No hay datos - Ejecutando limpieza una vez")
                            cursor.execute("""
                                UPDATE pr
                                  SET pr.status_id = 8
                                FROM production_records pr
                                JOIN part_numbers pn ON pr.part_number_id = pn.id
                                JOIN work_centers wc ON pn.work_center_id = wc.id
                                WHERE wc.name=? AND pr.planned_date=? AND pr.shift_id=? AND pr.status_id=7
                            """, (estacion, fecha_plan, turno))
                            active_records.clear()
                            conn.commit()
                            limpieza_ejecutada = True
                            station_logger.info("Active records limpiados (una vez)")
                        else:
                            # Ya se ejecutó la limpieza y no hay datos: no volver a ejecutar
                            station_logger.debug("Limpieza ya ejecutada, saltando")
                    else:
                        # Llegaron datos -> resetear bandera para permitir una próxima limpieza futura
                        if limpieza_ejecutada:
                            station_logger.info("Datos recibidos nuevamente - reset limpieza")
                        limpieza_ejecutada = False

                        # Impresión para debug (puedes comentar si no la quieres)
                        if datos:
                            df_data = []
                            for num, num_orig, cnt, t in datos:
                                df_data.append({
                                    'Numero_Parte': num,
                                    'Numero_Parte_Original': num_orig,
                                    'Contador': cnt,
                                    'Tiempo_Ciclo': t
                                })
                            df = pd.DataFrame(df_data)
                            df['Estacion'] = estacion
                            df['Fecha'] = now.strftime('%Y-%m-%d %H:%M:%S')
                            print(df.to_markdown(index=False))

                        # Llenar active_records para números de parte que no están ya cargados
                        for num, num_orig, cnt, t in datos:
                            if num not in active_records:
                                station_logger.debug(f"Procesando nueva parte: {num}")
                                id_reg, q_plan, q_prod, status, multiplicador = obtener_id_registro_activo(
                                    cursor, estacion, fecha_plan, turno, num, station_logger
                                )
                                if id_reg is None:
                                    station_logger.debug(f"No existe registro activo para {num}, creando nuevo")
                                    id_new, q_plan_new, q_prod_new, multiplicador_new = crear_nuevo_registro(
                                        cursor, num, estacion, cnt, turno,
                                        now.strftime('%Y-%m-%d %H:%M:%S'),
                                        fecha_plan, now, num_orig, station_logger
                                    )
                                    id_reg = id_new
                                    q_plan = q_plan_new
                                    q_prod = q_prod_new
                                    multiplicador = multiplicador_new
                                    status = 3 if id_reg is not None else None

                                if id_reg is not None:
                                    corrida_previa = q_prod if status == 8 else 0
                                    q_prod = 0 if status == 8 else q_prod

                                    # Si multiplicador es None, usar 1
                                    if multiplicador is None:
                                        multiplicador = 1

                                    active_records[num] = {
                                        "contador_registro": q_prod if q_prod is not None else 0,
                                        "id_registro":       id_reg,
                                        "quantity_planeada": q_plan if q_plan is not None else 0,
                                        "contador_ct":       None,
                                        "corrida_previa":    corrida_previa,
                                        "multiplicador":     multiplicador,
                                        "numero_original":   num_orig,
                                        "hora_cambio":       hora,
                                    }
                                    station_logger.info(f"Active record creado/actualizado - Parte: {num}, ID: {id_reg}, Corrida previa: {corrida_previa}, Multiplicador: {multiplicador}")
                                    station_logger.debug(f"Detalles del active record - Contador registro: {q_prod}, Quantity planeada: {q_plan}")

                        # Procesar cada dato
                        for num, num_orig, cnt, t in datos:
                            if num not in active_records:
                                station_logger.debug(f"Saltando parte {num} - no está en active_records")
                                continue

                            reg = active_records[num]
                            cambio = ((reg["hora_cambio"] < time(8,0) <= hora)
                                       or (reg["hora_cambio"] < time(16,0) <= hora))
                            prev = reg["contador_registro"]

                            station_logger.debug(f"Procesando parte {num} - Contador actual: {cnt}, Previo: {prev}, Cambio de turno: {cambio}")

                            if cnt > prev or (cambio and cnt >= prev):
                                if cambio:
                                    station_logger.info(f"Cambio de turno detectado - Parte: {num}")
                                    reg["contador_ct"] = prev

                                    id_reg2, q_plan2, q_prod2, status2, multiplicador2 = obtener_id_registro_activo(
                                        cursor, estacion, fecha_plan, turno, num, station_logger
                                    )
                                    if id_reg2 is None:
                                        id_new2, q_plan_new2, q_prod_new2, multiplicador_new2 = crear_nuevo_registro(
                                            cursor, num, estacion, cnt, turno,
                                            now.strftime('%Y-%m-%d %H:%M:%S'),
                                            fecha_plan, now, None, station_logger
                                        )
                                        id_reg2 = id_new2
                                        q_plan2 = q_plan_new2
                                        q_prod2 = q_prod_new2
                                        multiplicador2 = multiplicador_new2
                                        status2 = 3 if id_reg2 is not None else None

                                    if id_reg2 is not None:
                                        corrida_previa = q_prod2 if status2 == 8 else 0

                                        # Si multiplicador es None, usar 1
                                        multiplicador2 = multiplicador2 if (multiplicador2 is not None) else 1

                                        reg["id_registro"]       = id_reg2
                                        reg["quantity_planeada"] = q_plan2 if q_plan2 is not None else 0
                                        reg["corrida_previa"]    = corrida_previa
                                        reg["multiplicador"]     = multiplicador2

                                base_ct = reg["contador_ct"] or 0
                                corrida_previa = reg["corrida_previa"] or 0
                                multiplicador = reg["multiplicador"] or 1

                                # Aplicar el multiplicador al cálculo final
                                qty_upd = (cnt - base_ct + corrida_previa) * multiplicador

                                status = 7

                                station_logger.debug(f"Cálculo final - Base: {base_ct}, Corrida previa: {corrida_previa}, Multiplicador: {multiplicador}, Qty_upd: {qty_upd}")

                                # Obtener part_number_id para histories
                                part_number_id = obtener_part_number_id(cursor, num, estacion)

                                # Insertar en histories
                                if part_number_id:
                                    insertar_history(cursor, part_number_id, cnt, now.strftime('%Y-%m-%d %H:%M:%S'), t, station_logger)

                                # Actualizar registro
                                actualizar_registro(
                                    cursor, qty_upd,
                                    now.strftime('%Y-%m-%d %H:%M:%S'),
                                    reg["id_registro"], status, station_logger
                                )

                                reg["contador_registro"] = cnt
                                reg["hora_cambio"]       = hora

                                station_logger.info(f"Contador actualizado - Parte: {num}, Contador: {cnt}, Base: {base_ct}, Corrida previa: {corrida_previa}, Multiplicador: {multiplicador}, Qty_upd: {qty_upd}")

                    # Commit general al terminar procesamiento con datos (ya hacemos commit en limpieza si se ejecutó)
                    conn.commit()

            except asyncio.TimeoutError:
                # No hay datos nuevos, continuar
                continue
            except Exception as e:
                station_logger.error(f"Error en processor: {str(e)}", exc_info=True)
                await asyncio.sleep(1)

    except Exception as e:
        station_logger.error(f"Error general en processor: {str(e)}", exc_info=True)
    finally:
        conn.close()
        station_logger.info("Processor cerrado")

# ────────────────────────────────────────── GESTIÓN DE TAREAS Y HOT-RELOAD ────────────────────────────────────────────

async def supervisor():
    ips, ports, series, configs, ip_groups = load_config()
    reader_tasks = {}    # {ip: task}
    processor_tasks = {} # {estacion: task}
    last_hash = {}

    # Crear tasks iniciales
    # 1. Crear readers por IP
    for ip, group_info in ip_groups.items():
        port = group_info['port']
        serie = group_info['serie']
        reader_tasks[ip] = asyncio.create_task(
            plc_reader(ip, port, serie, group_info)
        )
        last_hash[f"reader_{ip}"] = hashlib.md5(str(group_info).encode()).hexdigest()
        logger.info(f"Reader iniciado - IP: {ip}, Estaciones: {group_info['estaciones']}")

    # 2. Crear processors por estación
    for estacion in configs.keys():
        ip = ips[estacion]
        processor_tasks[estacion] = asyncio.create_task(
            plc_processor(estacion, ip)
        )
        last_hash[f"processor_{estacion}"] = hashlib.md5(str(configs[estacion]).encode()).hexdigest()
        logger.info(f"Processor iniciado - Estación: {estacion}, IP: {ip}")

    while True:
        await asyncio.sleep(POLL_INTERVAL)
        new_ips, new_ports, new_series, new_configs, new_ip_groups = load_config()

        # Verificar cambios en readers (por IP)
        for ip, group_info in new_ip_groups.items():
            h = hashlib.md5(str(group_info).encode()).hexdigest()
            reader_key = f"reader_{ip}"

            if ip not in reader_tasks:
                # Nuevo reader
                port = group_info['port']
                serie = group_info['serie']
                reader_tasks[ip] = asyncio.create_task(
                    plc_reader(ip, port, serie, group_info)
                )
                last_hash[reader_key] = h
                logger.info(f"Nuevo Reader iniciado - IP: {ip}")
            elif h != last_hash.get(reader_key):
                # Reader cambió, reiniciar
                reader_tasks[ip].cancel()
                try:
                    await reader_tasks[ip]
                except:
                    pass

                port = group_info['port']
                serie = group_info['serie']
                reader_tasks[ip] = asyncio.create_task(
                    plc_reader(ip, port, serie, group_info)
                )
                last_hash[reader_key] = h
                logger.info(f"Reader reiniciado - IP: {ip}")

        # Remover readers que ya no existen
        for ip in list(reader_tasks.keys()):
            if ip not in new_ip_groups:
                reader_tasks[ip].cancel()
                try:
                    await reader_tasks[ip]
                except:
                    pass
                del reader_tasks[ip]
                del last_hash[f"reader_{ip}"]
                logger.info(f"Reader removido - IP: {ip}")

        # Verificar cambios en processors (por estación)
        for estacion, cfg in new_configs.items():
            h = hashlib.md5(str(cfg).encode()).hexdigest()
            processor_key = f"processor_{estacion}"
            ip = new_ips[estacion]

            if estacion not in processor_tasks:
                # Nuevo processor
                processor_tasks[estacion] = asyncio.create_task(
                    plc_processor(estacion, ip)
                )
                last_hash[processor_key] = h
                logger.info(f"Nuevo Processor iniciado - Estación: {estacion}")
            elif h != last_hash.get(processor_key):
                # Processor cambió, reiniciar
                processor_tasks[estacion].cancel()
                try:
                    await processor_tasks[estacion]
                except:
                    pass

                processor_tasks[estacion] = asyncio.create_task(
                    plc_processor(estacion, ip)
                )
                last_hash[processor_key] = h
                logger.info(f"Processor reiniciado - Estación: {estacion}")

        # Remover processors que ya no existen
        for estacion in list(processor_tasks.keys()):
            if estacion not in new_configs:
                processor_tasks[estacion].cancel()
                try:
                    await processor_tasks[estacion]
                except:
                    pass
                del processor_tasks[estacion]
                del last_hash[f"processor_{estacion}"]
                logger.info(f"Processor removido - Estación: {estacion}")

        # Actualizar referencias globales
        ips, ports, series, configs, ip_groups = new_ips, new_ports, new_series, new_configs, new_ip_groups

async def main():
    logger.info("Iniciando aplicación Prensas con PLC compartido")
    await supervisor()

if __name__ == '__main__':
    asyncio.run(main())