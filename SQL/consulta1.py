
import pandasql as psql
from Helpers.regularize import save_csv
#ejercicio 1.
"""
    Para cada país informar cantidad de sedes, cantidad de secciones en
    promedio que poseen sus sedes y el flujo migratorio neto (inmigración
    emigración) entre el país y el resto del mundo en el año 2000. El orden del
    reporte debe respetar la cantidad de sedes (de manera descendente). En
    caso de empate, ordenar alfabéticamente por nombre de país. A modo de
    ejemplo, el resultado podría ser:
"""
def sedes_secciones(datos,total = True):
    """Calcula la cantidad de sedes y el promedio de secciones por país."""
    consulta_sedes_secciones_TOTAL = '''
        SELECT s.pais_iso_3, 
               COUNT(DISTINCT s.sede_id) AS sedes_count, 
               AVG(sec.cantidad_secciones) AS secciones_prom
        FROM sedes_completo AS s
        LEFT JOIN (
            SELECT sede_id, 
                   COUNT(*) AS cantidad_secciones
            FROM secciones
            GROUP BY sede_id
        ) AS sec
        ON s.sede_id = sec.sede_id
        GROUP BY s.pais_iso_3
    '''
    consulta_sedes_secciones =     consulta_sedes_secciones = '''
        SELECT s.pais_iso_3, COUNT(DISTINCT s.sede_id) AS sedes_count, 
            AVG(COALESCE(sec.cantidad_secciones, 0)) AS secciones_prom
        FROM sedes_completo AS s
        LEFT JOIN (
            SELECT sede_id, COUNT(sede_id) AS cantidad_secciones
            FROM secciones
            GROUP BY sede_id
        ) AS sec
        ON s.sede_id = sec.sede_id
        GROUP BY s.pais_iso_3
    '''
    if total: 
        sedes_secciones = psql.sqldf(consulta_sedes_secciones_TOTAL, env=datos)
    else:
        sedes_secciones = psql.sqldf(consulta_sedes_secciones, env=datos)
    return [consulta_sedes_secciones, sedes_secciones]

def sedes_migraciones(datos, total=True):
    """Creamos una tabla que unifica las sedes_secciones con los paises"""
    consulta_sedes_secciones = sedes_secciones(datos,total)[0]
    consulta_migraciones_sedes = '''
        SELECT m."Country Origin Name" AS country, m."Country Origin Code" AS code_country, 
            ss.sedes_count, ss.secciones_prom
        FROM (
            SELECT "Country Origin Name", "Country Origin Code"
            FROM migraciones
            GROUP BY "Country Origin Name", "Country Origin Code"
        ) AS m
        LEFT JOIN ({}) AS ss
        ON m."Country Origin Code" = ss.pais_iso_3
        ORDER BY ss.sedes_count DESC, m."Country Origin Name" ASC
    '''.format(consulta_sedes_secciones)
    resultado = psql.sqldf(consulta_migraciones_sedes, env=datos)
    return [consulta_migraciones_sedes,resultado]

def emigracion():
    consulta = '''
    SELECT "Country Origin Code" AS code_country, 
    SUM("2000 [2000]") AS emigracion
    FROM migraciones
    WHERE "Migration by Gender Code" = 'TOT'
    GROUP BY "Country Origin Code"
    '''
    return consulta

def inmigracion():
    consulta = '''
    SELECT "Country Dest Code" AS code_country, 
    SUM("2000 [2000]") AS inmigracion
    FROM migraciones
    WHERE "Migration by Gender Code" = 'TOT'
    GROUP BY "Country Dest Code"
    '''
    return consulta

def flujo_migratorio(datos):
    """Calcula y devuelve el flujo migratorio neto por país."""
    consulta_flujo_migratorio = f'''
        WITH flujo_emigracion AS (
            {emigracion()}
        ),
        flujo_inmigracion AS (
            {inmigracion()}
        )
        SELECT 
            COALESCE(f_inm.code_country, f_emi.code_country) AS code_country,
            MAX(COALESCE(m."Country Origin Name", m."Country Dest Name", '')) AS country,
            COALESCE(f_inm.inmigracion, 0) - COALESCE(f_emi.emigracion, 0) AS flujo_migratorio_neto
        FROM flujo_inmigracion f_inm
        FULL OUTER JOIN flujo_emigracion f_emi 
            ON f_inm.code_country = f_emi.code_country
        LEFT JOIN migraciones m 
            ON COALESCE(f_inm.code_country, f_emi.code_country) = m."Country Origin Code"
            OR COALESCE(f_inm.code_country, f_emi.code_country) = m."Country Dest Code"
        WHERE m."Migration by Gender Code" = 'TOT'
        GROUP BY COALESCE(f_inm.code_country, f_emi.code_country)
        ORDER BY flujo_migratorio_neto DESC
    '''
    resultado = psql.sqldf(consulta_flujo_migratorio, env=datos)
    return [consulta_flujo_migratorio, resultado]

def reporte_sedes_migracion(datos,save_df=False):
    """Genera el reporte consolidado con sedes, secciones y flujo migratorio."""
    consulta_sedes_secciones = sedes_secciones(datos)[0]
    consulta_flujo_migratorio = flujo_migratorio(datos)[0]

    consulta_final = f'''
        SELECT 
            COALESCE(ss.pais_iso_3, fmn.code_country) AS country,
            COALESCE(ss.sedes_count, 0) AS sedes_count,
            COALESCE(ss.secciones_prom, 0) AS secciones_prom,
            COALESCE(fmn.flujo_migratorio_neto, 0) AS flujo_migratorio_neto
        FROM ({consulta_sedes_secciones}) AS ss
        FULL OUTER JOIN (
            SELECT code_country, flujo_migratorio_neto
            FROM ({consulta_flujo_migratorio})
        ) AS fmn
        ON ss.pais_iso_3 = fmn.code_country
        ORDER BY ss.sedes_count DESC, country ASC
    '''
    resultado = psql.sqldf(consulta_final, env=datos)
    if save_df:
        save_csv(resultado,"reporte_sedes_migracion")
    return [consulta_final,resultado]