import pandas as pd
import pyodbc
import datetime
import time
from pymcprotocol import Type3E
from datetime import datetime, time as datetime_time, timedelta
from itertools import product
import asyncio



# Configura los detalles del PLC, ip y puerto
PLC_IP = {
    # "2500T  TR" : "10.1.1.1",
    # "1500T  TR" : "10.1.1.7",
    # "1500T  TR2" : "10.1.1.3",
    # "HOT STAMPING" : "10.1.1.4",
    # "HOT STAMPING 2" : "10.1.1.5",
    "MK05                          " : "10.1.2.1",
    # "MK25" : "10.1.2.69"
    }
PLC_CONFIG = {
    "2500T  TR": {"codigo_parte_A": "D2830", "No_parte_A": "D3680", "Contador_A": "D17000"},
    "1500T  TR": {"codigo_parte_A": "D2830", "No_parte_A": "D3680", "Contador_A": "D3210"},
    "1500T  TR2": {"codigo_parte_A": "D2830", "No_parte_A": "D3680", "Contador_A": "D3210"},
    "HOT STAMPING" : {"codigo_parte_A": "D2000", "No_parte_A": "W12A0", "Contador_A": "D5103"},
    "HOT STAMPING 2" : {"codigo_parte_A": "D20000", "No_parte_A": "D20412", "Contador_A": "W120A"},
    "MK05                          ": {"No_parte_A": "D3104", "Contador_A": "D3100","PPC_A": "D8180", "No_parte_B": "D3112", "Contador_B": "D3101", "PPC_B": "D8182"},
    "MK25": {"codigo_parte_A": "D6500"}
    
}
PLC_PORT = 5002       # Puerto de comunicación
# PLC_PORT = 1025  

# # Configura los detalles de la base de datos
# DB_HOST = "192.168.120.13"
# DB_USER = "sa"
# DB_PASSWORD = "Alcala91"
# DB_NAME = "IOT_YKM"

partes = ['A', 'B', 'C', 'D']


def combinar_listas(cadena_limpia, contadores, tiempos):
    # Si las listas tienen la misma longitud, usar zip directamente
    if len(cadena_limpia) == len(contadores) == len(tiempos):
        datos = list(zip(cadena_limpia, contadores, tiempos))
    else:
        # Si las listas tienen longitudes diferentes, usar cycle para repetir la lista más corta
        datos = list(zip(cadena_limpia, cycle(contadores), tiempos))
    
    # Filtrar los datos: eliminar tuplas con elementos vacíos o None
    # datos_filtrados = [item for item in datos if all(item) and item[0] != '']
    
    # Elimina las tuplas solo cuando el primer elemento está vacío ('') o es None
    datos_filtrados = [item for item in datos if item and item[0] not in (None, '')]

    return datos_filtrados

def limpiar_cadena(cadena):
    # print(cadena)
     #eliminar "\x00" y espacios
    cadena_limpia = cadena.replace("\x00", "")#.replace(" ", "")
    if '/' in cadena_limpia:
        # Si la cadena contiene "/", dividir primero por "-" y luego por "/"
        partes = [parte.split('/') for parte in cadena_limpia.split(' ')]
        # Generar combinaciones usando product y concatenar en una sola línea
        return [''.join(combinacion) for combinacion in product(*partes)]
    else:
        return cadena_limpia.replace(" ", "")


def registro_cambio_turno(cursor, estacion, turno):
    # Consulta SQL para obtener el último registro según fecha_fin
    sql_query = '''
    SELECT TOP 1 modelo_A, no_parte_A, contador_A, modelo_B, no_parte_B, contador_B
    FROM Historico
    WHERE estacion = ?
    AND turno != ?
    ORDER BY fecha_fin DESC;
    '''

    # Ejecutar la consulta
    cursor.execute(sql_query, (estacion, turno))
    resultado = cursor.fetchone()

    if resultado:
        # Separar no_parte y contadores
        no_parte = [resultado[1], resultado[4]]  # no_parte_A, no_parte_B
        contadores = [resultado[2], resultado[5]]  # contador_A, contador_B
        modelos = [resultado[0], resultado[3]]
    else:
        # Si no hay resultados, devolver listas vacías
        no_parte = [None, None]
        contadores = [None, None]
        estacion = None

    return no_parte, contadores, estacion


def create_connection():
    # connection = pyodbc.connect(
    #     'DRIVER={ODBC Driver 17 for SQL Server};'
    #     'SERVER=192.168.130.87;' # cambiar ip de migue
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
                        
def actualizar_historico(cursor, contador, statuses, fecha_formateada, fecha_start, record_id):

    # Consulta SQL para actualizar
    sql_update = '''
                UPDATE production_records
                SET 
                    produced_quantity = ?,
                    status_id = ?,
                    production_end = ?,
                    production_start = ?
                WHERE id = ?  -- Usamos el ID obtenido
                '''

    cursor.execute(sql_update, (contador, statuses, fecha_formateada, fecha_start, record_id))


    # sql_update = '''
    # UPDATE production_records
    # SET 
    #     produced_quantity = ?,
    #     status_id = ?,          -- Cambiar el valor de status_id
    #     production_end = ?,
    #     production_start = ?    -- Registrar la fecha y hora actual
    # FROM production_records pr
    # JOIN part_numbers pn ON pr.part_number_id = pn.id
    # JOIN work_centers wc ON pn.work_center_id = wc.id
    # WHERE 
    #     wc.name = ? 
    #     AND pr.planned_date = ? 
    #     AND pr.shift_id = ? 
    #     AND pn.number = ?
    #     ORDER BY pr.id DESC
    # '''
    # cursor = cursor.fetchone()
    

def insertar_historico(cursor, numero_parte, contador, turno, fecha_formateada, statuses, planned_date):
    # Insertar nuevo registro en la tabla production_records
    sql_insert = '''
        INSERT INTO production_records (part_number_id, produced_quantity, shift_id, production_start, status_id, planned_date)
        VALUES (?, ?, ?, ?, ?, ?)
    '''
    cursor.execute(sql_insert, (numero_parte, contador, turno, fecha_formateada, statuses, planned_date))




async def plc_historico(prensa, ip,config):
    # connection = pyodbc.connect(
    #     'DRIVER={ODBC Driver 17 for SQL Server};'
    #     'SERVER=192.168.120.13;'
    #     'DATABASE=IOT_YKM;'
    #     'UID=sa;'
    #     'PWD=Alcala91'
    #     )
    # cursor = connection.cursor()
    # conn = create_connection()
    # cursor = conn.cursor()
    while True:
        try:
            conn = create_connection()
            # Usar un bloque with para manejar el cursor
            with conn.cursor() as cursor:

                start_time = time.time()

                plc = Type3E(plctype="Q")
                plc.soc_timeout = 10  # Tiempo de espera del socket en segundos

                # Intenta conectar al PLC   
                plc.connect(ip, PLC_PORT)
                estacion = prensa
                
                
                data = []
                cadena = []
                contadores = []
                tiempos = []

                # Determina cuántas partes vas a procesar dependiendo de la longitud de config
                partes_a_procesar = min(len(config) // 3, len(partes))  # Cada parte tiene 3 claves: codigo_parte, contador, no_parte


                for i, parte in enumerate(partes):
                    if i < partes_a_procesar:  # Procesa solo las partes que correspondan
                        contador_clave = f"Contador_{parte}"
                        no_parte_clave = f"No_parte_{parte}"
                        PPC_clave = f"PPC_{parte}"

                        # Lee los valores del PLC y los agrega a las listas
                        contadores.append(plc.batchread_wordunits(headdevice=config.get(contador_clave), readsize=1)[0])

        
                        # tiempos.append(plc.batchread_wordunits(headdevice=config.get(PPC_clave), readsize=1)[0])

                        try:
                            tiempos.append(abs(int(plc.batchread_wordunits(headdevice=config.get(PPC_clave), readsize=1)[0]) / 1000))
                        except (ValueError, TypeError) as e:
                            print(f"Error procesando valor: {e}")
                            tiempos.append(0.0)  # Valor por defecto en caso de error

                        # Lee la cadena si existe, sino agrega None
                        cadena.append(plc.batchread_wordunits(headdevice=config.get(no_parte_clave), readsize = 8)) if config.get(no_parte_clave) else cadena.append(None)
                    else:
                        # Asignar null (None en Python) a las partes que no se procesarán
                        tiempos.append(None)
                        contadores.append(None)
                        cadena.append(None)

                # print(cadena
                cadena_limpia = []
                for bloque in cadena:
                    if bloque is None:  # Verifica si el bloque es None
                        cadena_limpia.append(None)  # Agrega None directamente a la lista final
                        continue  # Salta el procesamiento de este bloque y pasa al siguiente
                    ascii_value = ""  # Variable para almacenar el resultado de cada lista
                    for valor in bloque:
                        parte_alta = (valor >> 8) & 0xFF  # Obtener los primeros 8 bits
                        parte_baja = valor & 0xFF         # Obtener los últimos 8 bits
                        caracter_alto = chr(parte_alta)
                        caracter_bajo = chr(parte_baja) 
                        ascii_char = caracter_bajo + caracter_alto
                        ascii_value += ascii_char
                    cadena_limpia.append(limpiar_cadena(ascii_value))  # Agregar el resultado de cada lista al total
                # print(cadena_limpia)
                
                # Obtener la fecha y hora actual
                fecha_actual = datetime.now()

                # Formatear la fecha y hora para SQL
                fecha_formateada = fecha_actual.strftime('%Y-%m-%d %H:%M:%S')
                # Obtener solo la hora actual
                hora_actual = fecha_actual.time()
                
                
                # # Obtener solo la fecha actual
                # solo_fecha = fecha_actual.date()
                # # Definir los intervalos de tiempo para los turnos
                # inicio_turno_dia = datetime_time(8, 0, 0)
                # fin_turno_dia = datetime_time(20, 0, 0)
                # inicio_dia = datetime_time(0, 0, 0)

                # # Dia: 1	D	Diurno
                # # noche: 2	N	Nocturno
                # # Determinar el turno
                # if inicio_turno_dia <= hora_actual < fin_turno_dia:
                #     turno = 1
                # else:
                #     turno = 2
                #     if fin_turno_dia <= hora_actual < inicio_dia:
                #         solo_fecha = solo_fecha
                #     else:
                #         # Restar un día a solo_fecha
                #         solo_fecha = solo_fecha - timedelta(days=1)
                
                TURNO_DIURNO_INICIO = datetime_time(8, 0, 0)
                TURNO_DIURNO_FIN = datetime_time(20, 0, 0)

                if TURNO_DIURNO_INICIO <= hora_actual < TURNO_DIURNO_FIN:
                    turno = 1
                    fecha_ajustada = fecha_actual.date()
                else:
                    turno = 2
                    fecha_ajustada = fecha_actual.date() - timedelta(days=1) if hora_actual < TURNO_DIURNO_INICIO else fecha_actual.date()

                
                
                data.append({"Estacion": estacion ,"Tiempo_Ciclo_A": tiempos[0] , "No_parte_A": cadena_limpia[0], "Contador_A": contadores[0],
                                                    "Tiempo_Ciclo_B": tiempos[1] ,"No_parte_B": cadena_limpia[1], "Contador_B": contadores[1], "Turno": turno,"Fecha": fecha_formateada})
                df = pd.DataFrame(data)

                print(df)


                # no_parte_cambio_turno, contadores_cambio_turno, modelos_cambio_turno= registro_cambio_turno(cursor, estacion, turno)

                turno1 = 'D'

                # Para cambio de turno 
                if turno1 == 'N':
                # if (inicio_turno_dia == hora_actual or fin_turno_dia == hora_actual) and cadena_limpia and codigo_parte_condicion and cadena_limpia == no_parte_cambio_turno:
                    print("entra cambio")

                    # realiza la operacion si las variables no son None
                    operacion0 = None if contadores[0] is None or contadores_cambio_turno[0] is None else contadores[0] - contadores_cambio_turno[0]
                    operacion1 = None if contadores[1] is None or contadores_cambio_turno[1] is None else contadores[1] - contadores_cambio_turno[1]
                    
                    insertar_historico(cursor, estacion, turno, operacion0, operacion1, fecha_formateada, codigo_partes, cadena_limpia)
                    
                elif turno1 == 'N':    
                # elif  no_parte_cambio_turno == cadena_limpia and modelos_cambio_turno == codigo_partes:
                    print(no_parte_cambio_turno)
                    print(cadena_limpia)
                    print("sigue la misma pieza")
                    print(contadores)
                    print(contadores_cambio_turno)


                    # realiza la operacion si las variables no son None
                    operacion0 = None if contadores[0] is None or contadores_cambio_turno[0] is None else contadores[0] - contadores_cambio_turno[0]
                    operacion1 = None if contadores[1] is None or contadores_cambio_turno[1] is None else contadores[1] - contadores_cambio_turno[1]

                    actualizar_historico(cursor, estacion, turno, operacion0, operacion1, fecha_formateada)

                else:

                    
                    datos_filtrados = combinar_listas(cadena_limpia, contadores, tiempos)
                    print(len(datos_filtrados))
                    
                    if not datos_filtrados:
                        print("No hay datos para procesar")
                        sql_update_status = '''
                                    UPDATE pr
                                    SET pr.status_id = 8
                                    FROM production_records pr
                                    JOIN part_numbers pn ON pr.part_number_id = pn.id
                                    JOIN work_centers wc ON pn.work_center_id = wc.id
                                    WHERE 
                                        wc.name = ?
                                        AND pr.planned_date = ?
                                        AND pr.shift_id = ?
                                        AND pr.status_id = 7
                                '''
                        cursor.execute(sql_update_status, estacion, fecha_ajustada, turno)
                    print(datos_filtrados)

                    for numero_parte, contador, tiempo in datos_filtrados:

                        sql_get_record = '''
                                        SELECT TOP 1
                                            pr.id,
                                            pr.production_start,
                                            pr.produced_quantity,
                                            pr.status_id,
                                            pr.planned_quantity,
                                            pn.number AS part_number,
                                            wc.name AS work_center
                                        FROM production_records pr
                                        JOIN part_numbers pn ON pr.part_number_id = pn.id
                                        JOIN work_centers wc ON pn.work_center_id = wc.id
                                        WHERE 
                                            wc.name = ? 
                                            AND pr.planned_date = ? 
                                            AND pr.shift_id = ?
                                            AND pn.number = ?
                                            AND pr.status_id != 8
                                        ORDER BY pr.id DESC
                                        '''

                        # Ejecutar la consulta con los parámetros adicionales
                        cursor.execute(sql_get_record, (estacion, fecha_ajustada, turno, numero_parte))
                        resultado = cursor.fetchone()
                        

 
                        # Verificar si el registro existe
                        if resultado:
                            fecha_start = resultado.production_start
                            record_id  = resultado.id
                            # Verificar si el contador ha aumentado
                            if contador > resultado.produced_quantity:
                                if resultado.status_id in [3, 4, 24]:
                                    if resultado.status_id == 4:
                                        statuses = 4
                                        actualizar_historico(cursor, contador, statuses, fecha_formateada, fecha_start, record_id)
                                                            
                                    else:
                                        print("actualiza registro ya existente")
                                        statuses = 7
                                        fecha_start = fecha_formateada
                                        actualizar_historico(cursor, contador, statuses, fecha_formateada, fecha_start, record_id)
                                
                                elif resultado.status_id == 7:
                                    if contador == resultado.planned_quantity:
                                        print("registro completado")
                                        statuses = 4
                                        actualizar_historico(cursor, contador, statuses, fecha_formateada, fecha_start, record_id)
                                    else:
                                        print("actualiza registro registro planeado no completado")
                                        statuses = 7
                                        actualizar_historico(cursor, contador, statuses, fecha_formateada, fecha_start, record_id)
                            elif contador < resultado.produced_quantity:
                                statuses = 24
                                print("entra en este")
                                planned_date = fecha_ajustada # fecha de prueba
                                insertar_historico(cursor, resultado.part_number_id, contador, turno, fecha_formateada, statuses, planned_date)

                        else:

                            # Consulta para obtener el part_number_id
                            query_id = '''
                                        SELECT pn.id
                                        FROM part_numbers pn
                                        JOIN work_centers wc ON pn.work_center_id = wc.id
                                        WHERE pn.number = ? AND wc.name = ?;
                                        '''
                            cursor.execute(query_id, (numero_parte, estacion))
                            part_number_id = cursor.fetchone()

                            if part_number_id:
                                part_number_id = part_number_id[0]  # Obtener el valor del id
                                statuses = 24
                                planned_date = fecha_ajustada # fecha de prueba
                                insertar_historico(cursor, part_number_id, contador, turno, fecha_formateada, statuses, planned_date)

                            else:
                                print(f"No se encontró el número de parte en el workcenter: {numero_parte}")
                                # Leer el archivo de logs para verificar si el error ya está registrado
                                with open("logs.txt", "r") as log_file:
                                    logs = log_file.readlines()
                                
                                error_message = f"{fecha_actual.date()} - Error: No se encontró el número de parte en el workcenter {estacion}: {numero_parte}\n"
                                
                                # Si el error no está en los logs, agregarlo
                                if error_message not in logs:
                                    with open("logs.txt", "a") as log_file:
                                        log_file.write(error_message)

                    #
                    if datos_filtrados:
                        for numero_parte, contador, tiempo in datos_filtrados:


                            # Consulta para obtener el part_number_id
                            query_id = '''
                                        SELECT pn.id
                                        FROM part_numbers pn
                                        JOIN work_centers wc ON pn.work_center_id = wc.id
                                        WHERE pn.number = ? AND wc.name = ?;
                                        '''
                            cursor.execute(query_id, (numero_parte, estacion))
                            part_number_id = cursor.fetchone()

                            if part_number_id:
                                part_number_id = part_number_id[0] 

                                # Obtener el último registro para ese part_number_id
                                sql_last_record = '''
                                    SELECT quantity 
                                    FROM histories 
                                    WHERE part_number_id = ? 
                                    ORDER BY created_at DESC;
                                '''
                                cursor.execute(sql_last_record, (part_number_id,))
                                last_record = cursor.fetchone()

                                # Verificar si se debe insertar un nuevo registro
                                if last_record is None or last_record[0] != contador:
                                    sql_insert = '''
                                        INSERT INTO histories (part_number_id, quantity, created_at, production_per_cycle)
                                        VALUES (?, ?, ?, ?);
                                    '''
                                    cursor.execute(sql_insert, (part_number_id, contador, fecha_formateada, tiempo))  





            
            # print(f"Tiempo total: {time.time() - start_time:.2f} segundos")
            # Confirma la transacción
            conn.commit()
            # time.sleep(1)
            await asyncio.sleep(.7)
            
    




            plc.close()
    
        except Exception as e:
            # cursor.close()
            # connection.close()
            print(f"Error al conectar al PLC: {e}")
            #Cierra la conexión
            cursor.close()
            # connection.close()
            conn.close()
            plc.close()
            break  # Rompe el bucle en caso de error
        finally:
            # Cierra la conexión
            cursor.close()
            # connection.close()
            conn.close()
            plc.close()

async def supervisor(PLC_IP, PLC_CONFIG):
    tasks = {}

    # Crear tareas para cada prensa en el diccionario PLC_IP
    for nombre_prensa, ip in PLC_IP.items():
        config = PLC_CONFIG.get(nombre_prensa, {})
        tasks[nombre_prensa] = asyncio.create_task(plc_historico(nombre_prensa, ip, config))
    
    while True:
        for nombre_prensa, task in tasks.items():
            if task.done():  # Verifica si la tarea ha terminado
                print(f"Tarea de {nombre_prensa} se ha detenido, reiniciando...")
                tasks[nombre_prensa] = asyncio.create_task(plc_historico(nombre_prensa, PLC_IP[nombre_prensa], config))
        await asyncio.sleep(3)  # Intervalo para chequear las tareas


async def main():
    await supervisor(PLC_IP, PLC_CONFIG)

asyncio.run(main())

# def main():
#     # plc_historico()
#     # Crear un flujo de trabajo
        
#     # Descomponer el diccionario en claves y valores
#     keys = list(PLC_IP.keys())
#     values = list(PLC_IP.values())
        
#     # Usar map para ejecutar la tarea en paralelo con las claves y valores
#     plc_historico.map(keys, values)



 
# if __name__ == "__main__":
#     main()