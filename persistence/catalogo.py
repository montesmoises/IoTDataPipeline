"""
Catálogo de números de parte: MDI (troquel) y piezas por golpe.

Reemplaza las dos consultas que antes iban a AS400/Infor:

    LX834F01.IIU  IUFD05 (IUSEQN=2)  -> MDI          == attributes.[key]='mid'
    LX834F01.IIU  IUFD11 (IUSEQN=1)  -> multiplicador == attributes.[key]='pieces_per_shot'

Los atributos viven en la tabla `attributes` (patrón EAV de Laravel), colgados
del part_number. Una sola consulta trae los números del MDI, si están obsoletos,
a qué estación pertenecen y su multiplicador.
"""

import logging

logger = logging.getLogger("supervisor")

TIPO_ATRIBUTO = 'App\\Models\\PartNumber'

# Motivos de rechazo. Alimentan el CSV/JSONL y el tablero de partes rechazadas.
MDI_NO_EXISTE = "MDI_NO_EXISTE"
MDI_DE_OTRA_ESTACION = "MDI_DE_OTRA_ESTACION"
PART_NUMBER_OBSOLETO = "PART_NUMBER_OBSOLETO"


def resolver_mdi(cursor, mdi, estacion, log=None):
    """
    Traduce el MDI que manda el PLC a los números de parte que debe producir.

    Devuelve (numeros_activos, multiplicadores, error):
      - numeros_activos: lista de números no obsoletos de ESA estación
      - multiplicadores: {numero: piezas_por_golpe}
      - error: None si hubo resultados; si no, el motivo clasificado

    La consulta NO filtra por estación ni por obsoleto: se traen todas las filas
    del MDI para poder distinguir *por qué* falló, que es lo que necesita el
    tablero de partes rechazadas.
    """
    log = log or logger
    mdi_limpio = (mdi or "").replace(" ", "")
    if not mdi_limpio:
        return [], {}, MDI_NO_EXISTE

    cursor.execute("""
        SELECT pn.number, pn.is_obsolete, wc.name, a_pps.value
        FROM part_numbers pn
        JOIN work_centers wc ON pn.work_center_id = wc.id
        JOIN attributes a_mid
             ON a_mid.attributable_id = pn.id
            AND a_mid.attributable_type = ?
            AND a_mid.[key] = 'mid'
        LEFT JOIN attributes a_pps
             ON a_pps.attributable_id = pn.id
            AND a_pps.attributable_type = ?
            AND a_pps.[key] = 'pieces_per_shot'
        WHERE REPLACE(a_mid.value, ' ', '') = ?
    """, (TIPO_ATRIBUTO, TIPO_ATRIBUTO, mdi_limpio))

    filas = cursor.fetchall()

    if not filas:
        log.warning(f"⚠️ MDI {mdi} no existe en el catálogo")
        return [], {}, MDI_NO_EXISTE

    de_la_estacion = [f for f in filas if f[2] == estacion]
    if not de_la_estacion:
        otras = sorted({f[2] for f in filas})
        log.warning(f"⚠️ MDI {mdi} existe pero pertenece a {otras}, no a {estacion}")
        return [], {}, MDI_DE_OTRA_ESTACION

    activos = [f for f in de_la_estacion if not f[1]]
    if not activos:
        log.warning(f"⚠️ MDI {mdi}: los {len(de_la_estacion)} números de {estacion} están OBSOLETOS")
        return [], {}, PART_NUMBER_OBSOLETO

    obsoletos = [f[0] for f in de_la_estacion if f[1]]
    if obsoletos:
        log.warning(f"⚠️ Filtrados {len(obsoletos)} número(s) obsoleto(s) del MDI {mdi}: {obsoletos}")

    numeros = [f[0] for f in activos]
    multiplicadores = {f[0]: _a_entero(f[3]) for f in activos}

    log.info(f"✅ MDI {mdi} -> {numeros} en {estacion}")
    return numeros, multiplicadores, None


def obtener_multiplicador(cursor, numero_parte, estacion, log=None):
    """
    Piezas por golpe de una parte. 1 si no tiene el atributo configurado.

    Antes esto era una consulta a AS400 que además dependía de saber si la
    estación era de Estampado; ahora simplemente no hay atributo y devuelve 1.
    """
    log = log or logger
    try:
        cursor.execute("""
            SELECT TOP(1) a.value
            FROM attributes a
            JOIN part_numbers pn ON a.attributable_id = pn.id
            JOIN work_centers wc ON pn.work_center_id = wc.id
            WHERE a.attributable_type = ?
              AND a.[key] = 'pieces_per_shot'
              AND REPLACE(pn.number, ' ', '') = ?
              AND wc.name = ?
        """, (TIPO_ATRIBUTO, (numero_parte or "").replace(" ", ""), estacion))
        res = cursor.fetchone()
        return _a_entero(res[0]) if res else 1
    except Exception as e:
        log.error(f"Error consultando multiplicador de {numero_parte}: {e}")
        return 1


def _a_entero(valor):
    """El atributo se guarda como texto; un valor raro no debe tumbar el conteo."""
    try:
        n = int(str(valor).strip())
        return n if n > 0 else 1
    except (TypeError, ValueError):
        return 1
