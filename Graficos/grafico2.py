import pandasql as psql
import pandas as pd
PATH_SEDES  = r"Datos\sedes.csv"
PATH_SEDES_MIN = r"Datos\sedes_min.csv"
PATH_SECCIONES = r"Datos\secciones.csv"
PATH_MIGRACIONES = r"Datos\migraciones.csv"
PATH_REGIONES = r"Datos\codigo_region.csv"
datos = {
    "migraciones" : pd.read_csv(PATH_MIGRACIONES),
    "sedes_basico" : pd.read_csv(PATH_SEDES_MIN),
    "sedes_completo" : pd.read_csv(PATH_SEDES),
    "secciones" : pd.read_csv(PATH_SECCIONES),
    "regiones" : pd.read_csv(PATH_REGIONES)
} 

def get_datos():
    """Retorna la lista de datos transformados en dataframes"""
    return datos

def get_keys():
    """Retorna las keys de los dataframe en el diccionario"""
    return datos.keys()





def sedes_secciones(datos):
    """Creamos una tabla que calcule el total de sedes por pais y el promedio de secciones"""
    consulta_sedes_secciones = '''
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
    sedes_secciones = psql.sqldf(consulta_sedes_secciones, env=datos)
    return [consulta_sedes_secciones,sedes_secciones]
#print(sedes_secciones(datos)[1])

def sedes_migraciones(datos):
    """Creamos una tabla que unifica las sedes_secciones con los paises"""
    consulta_sedes_secciones = sedes_secciones(datos)[0]
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
#print(sedes_migraciones(datos)[1])

""" 
    Notas: Ejercicio
    se puede usar al función: AVG(COALESCE(sec.cantidad_secciones, 0)) para calcular el pormedio?
    forma manual: ¿Como?
"""


# Ejer 2 

def obtener_migracion(tipo, anio):
    """Genera una consulta SQL para la emigración o inmigración por un año dado."""
    columna = f'"{anio} [{anio}]"'
    codigo = "Country Origin Code" if tipo == "emigracion" else "Country Dest Code"
    return f'''
        SELECT "{codigo}" AS code_country, 
               SUM({columna}) AS {tipo}
        FROM migraciones
        WHERE "Migration by Gender Code" = 'TOT'
        GROUP BY "{codigo}"
    '''

# Función general para calcular el flujo migratorio por año
def flujo_migratorio_por_anio(anio, datos):
    """Calcula el flujo migratorio neto por año específico."""
    consulta = f'''
        WITH flujo_emigracion AS (
            {obtener_migracion("emigracion", anio)}
        ),
        flujo_inmigracion AS (
            {obtener_migracion("inmigracion", anio)}
        )
        SELECT COALESCE(f_inm.code_country, f_emi.code_country) AS code_country,
               COALESCE(m."Country Origin Name", "") AS country,
               COALESCE(f_inm.inmigracion, 0) - COALESCE(f_emi.emigracion, 0) AS flujo_migratorio_{anio}
        FROM flujo_inmigracion f_inm
        FULL OUTER JOIN flujo_emigracion f_emi 
            ON f_inm.code_country = f_emi.code_country
        LEFT JOIN migraciones m
            ON COALESCE(f_inm.code_country, f_emi.code_country) = m."Country Origin Code"
        WHERE m."Migration by Gender Code" = 'TOT'
        GROUP BY COALESCE(f_inm.code_country, f_emi.code_country), m."Country Origin Name"
        ORDER BY flujo_migratorio_{anio} DESC
    '''
    return psql.sqldf(consulta, env=datos)

# Obtener flujos migratorios para múltiples décadas
def flujos_migratorios_por_decadas(datos, decadas):
    """Genera y unifica los flujos migratorios por varias décadas."""
    resultados = []
    for decada in decadas:
        flujo = flujo_migratorio_por_anio(decada, datos)
        resultados.append(flujo)

    # Unir los resultados de cada década por code_country
    flujo_total = resultados[0]
    for flujo in resultados[1:]:
        flujo_total = flujo_total.merge(flujo, on=["code_country", "country"], how="outer")

    return  flujo_total

#Unir 

def reporte_sedes_migracion(datos, decadas):
    """Genera reporte unificado de sedes y migraciones."""
    sedes_sec = sedes_secciones(datos)[1]
    flujos_migratorios = flujos_migratorios_por_decadas(datos, decadas)

    # Unir los resultados de sedes y flujos migratorios
    reporte = sedes_sec.merge(
        flujos_migratorios, left_on="pais_iso_3", right_on="code_country", how="left"
    ).fillna(0)

    return reporte

# Lista de décadas a considerar
decadas = [1960, 1970, 1980, 1990, 2000]

# Ejecutar reporte final
reporte_final = reporte_sedes_migracion(datos, decadas)
print(reporte_final)