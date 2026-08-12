"""
Todo el SQL contra production_records, part_numbers e histories.

Estados de production_records:
    3 = planeado (el programa de producción; NUNCA se cierra desde aquí)
    7 = produciendo
    8 = cerrado
"""

import logging

logger = logging.getLogger("supervisor")


def _sin_multiplicador(cursor, numero_parte, estacion, log):
    """Por defecto un golpe es una pieza. Estampado inyecta su propio lector."""
    return 1


# ─────────────────────────── production_records ───────────────────────────

def actualizar_registro(cursor, delta_produccion, fecha_fmt, record_id, status, log,
                        start_fmt=None, necesita_start=False):
    """
    Suma el delta de producción al registro (modelo incremental).

    La BD es la dueña del acumulado: produced_quantity = produced_quantity + delta.
    Así, un reset de contador o una pérdida del state_cache nunca destruyen lo ya
    registrado, y dos lados produciendo la misma parte suman sin pisarse.
    """
    sql_parts = ["produced_quantity = produced_quantity + ?", "production_end=?", "status_id=?"]
    params = [delta_produccion, fecha_fmt, status]

    if start_fmt:
        sql_parts.insert(0, "production_start=?")
        params.insert(0, start_fmt)
    elif necesita_start:
        sql_parts.insert(0, "production_start=?")
        params.insert(0, fecha_fmt)

    sql = "UPDATE production_records SET " + ", ".join(sql_parts) + " WHERE id=?"
    params.append(record_id)

    cursor.execute(sql, tuple(params))


def obtener_id_registro_activo(cursor, estacion, fecha_ajustada, turno, numero_parte, log,
                               obtener_multiplicador=_sin_multiplicador):
    """
    Busca el registro con el que se debe trabajar para esta parte/turno/fecha.

    Incluye status 3 a propósito: así el recolector toma los registros PLANEADOS
    y produce contra ellos en vez de crear duplicados.

    `obtener_multiplicador` lo decide el ÁREA (ver areas/): solo Estampado
    consulta pieces_per_shot; las demás usan 1 sin tocar la base.
    """
    sql = '''SELECT TOP(1) pr.id, pr.planned_quantity, pr.produced_quantity, pr.status_id, pr.production_start
             FROM production_records pr
             JOIN part_numbers pn ON pr.part_number_id = pn.id
             JOIN work_centers wc ON pn.work_center_id = wc.id
             WHERE wc.name=? AND REPLACE(pn.number, ' ', '')=? AND pr.planned_date=? AND pr.shift_id=? AND pr.status_id IN (3, 7, 8) AND pr.synced_to_infor != 1
             ORDER BY pr.status_id DESC, pr.id DESC'''
    cursor.execute(sql, (estacion, numero_parte, fecha_ajustada, turno))
    res = cursor.fetchone()
    if res:
        mult = obtener_multiplicador(cursor, numero_parte, estacion, log)
        return res[0], res[1], res[2], res[3], res[4], mult
    return None, None, None, None, None, None


def crear_nuevo_registro(cursor, numero_parte, estacion, contador, turno, fecha_fmt,
                         fecha_ajustada, num_orig, log,
                         obtener_multiplicador=_sin_multiplicador):
    """
    Crea el registro de producción. Devuelve (id, q_plan, q_prod, mult, error).

    El error es un código para el CSV/tablero de partes rechazadas:
    PART_NUMBER_OBSOLETO, PART_NUMBER_NO_EXISTE_BD, SQL_NO_ENCONTRADO, DB_ERROR.
    """
    #  Verificación previa para diagnóstico
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
            cursor.execute(
                "SELECT pn.number FROM part_numbers pn JOIN work_centers wc ON pn.work_center_id = wc.id WHERE pn.number=? AND wc.name=?",
                (numero_parte, estacion)
            )
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
        log.info(f"🔍 Intentando crear registro para: numero_parte={numero_parte}, estacion={estacion}, turno={turno}")

        cursor.execute(sql, (contador, turno, fecha_fmt, fecha_ajustada, numero_parte, estacion))
        res = cursor.fetchone()
        if res:
            mult = obtener_multiplicador(cursor, numero_parte, estacion, log)
            log.info(f"✅ Registro creado exitosamente: ID={res[0]}, numero_parte={numero_parte}")
            return res[0], res[1], res[2], mult, None

        log.warning(f"⚠️ No se pudo crear registro para {numero_parte} - La consulta no retornó resultados")
        log.warning(f"   Esto significa que el número de parte no existe en part_numbers o no está asociado a {estacion}")
        return None, None, None, None, "SQL_NO_ENCONTRADO"
    except Exception as e:
        log.error(f"❌ Error crear registro para {numero_parte}: {e}")
        log.error(f"   Parámetros: contador={contador}, turno={turno}, estacion={estacion}")
        return None, None, None, None, "DB_ERROR"


def reactivar_registro(cursor, record_id):
    """Devuelve un registro cerrado a 'produciendo' (8 → 7)."""
    cursor.execute(
        "UPDATE production_records SET status_id = 7 WHERE id = ? AND status_id = 8",
        (record_id,)
    )


def cerrar_registro(cursor, record_id, fecha_fmt):
    """
    Cierra un registro que estaba produciendo (7 → 8).

    SOLO status 7: los registros en status 3 son el PLAN de producción y
    cerrarlos destruiría la programación.
    """
    cursor.execute(
        "UPDATE production_records SET status_id = 8, production_end=? WHERE id=? AND status_id=7",
        (fecha_fmt, record_id)
    )


def cerrar_registros_de_estacion(cursor, estacion, fecha_plan, turno, fecha_fmt):
    """Cierra todos los registros activos de una estación (dejó de reportar partes)."""
    cursor.execute("""
        UPDATE pr SET pr.status_id = 8, pr.production_end=?
        FROM production_records pr
        JOIN part_numbers pn ON pr.part_number_id = pn.id
        JOIN work_centers wc ON pn.work_center_id = wc.id
        WHERE wc.name=? AND pr.planned_date=? AND pr.shift_id=? AND pr.status_id=7
    """, (fecha_fmt, estacion, fecha_plan, turno))


# ─────────────────────────────── part_numbers ──────────────────────────────

def obtener_part_number_id(cursor, numero_parte, estacion):
    sql = ("SELECT pn.id FROM part_numbers pn "
           "JOIN work_centers wc ON pn.work_center_id = wc.id "
           "WHERE REPLACE(pn.number, ' ', '')=? AND wc.name=?")
    cursor.execute(sql, (numero_parte, estacion))
    res = cursor.fetchone()
    return res[0] if res else None


# ──────────────────────────────── histories ────────────────────────────────

def insertar_history(cursor, part_number_id, cantidad, fecha_fmt, tiempo, sequence=None):
    """
    Registra el detalle golpe a golpe.

    `cantidad` es el incremento CRUDO del contador del PLC: golpes en Estampado,
    piezas en las demás áreas. production_records siempre guarda piezas.
    `sequence` lleva el troquel en Estampado; en las demás áreas va nulo y la
    columna no se escribe.
    """
    if sequence is None:
        cursor.execute(
            "INSERT INTO histories (part_number_id, quantity, created_at, production_per_cycle) "
            "VALUES (?, ?, ?, ?)",
            (part_number_id, cantidad, fecha_fmt, tiempo)
        )
    else:
        cursor.execute(
            "INSERT INTO histories (part_number_id, quantity, created_at, production_per_cycle, sequence) "
            "VALUES (?, ?, ?, ?, ?)",
            (part_number_id, cantidad, fecha_fmt, tiempo, sequence)
        )
