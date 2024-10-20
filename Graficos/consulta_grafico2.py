import pandasql as psql
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt  # Para graficar series múltiples
import seaborn as sns            # Para graficar histogramas

# Rutas de los datos
PATH_SEDES  = r"Datos\sedes.csv"
PATH_SEDES_MIN = r"Datos\sedes_min.csv"
PATH_SECCIONES = r"Datos\secciones.csv"
PATH_MIGRACIONES = r"Datos\migraciones.csv"
PATH_REGIONES = r"Datos\codigo_region.csv"

# Cargar datos
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
    """Creamos una tabla que calcule el total de sedes por país y el promedio de secciones"""
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
    return [consulta_sedes_secciones, sedes_secciones]

def sedes_migraciones(datos):
    """Creamos una tabla que unifica las sedes_secciones con los países"""
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
    return [consulta_migraciones_sedes, resultado]

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

    return flujo_total

def reporte_sedes_migracion(datos, decadas):
    """Genera reporte unificado de sedes y migraciones."""
    sedes_sec = sedes_secciones(datos)[1]
    flujos_migratorios = flujos_migratorios_por_decadas(datos, decadas)

    # Unir los resultados de sedes y flujos migratorios
    reporte = sedes_sec.merge(
        flujos_migratorios, left_on="pais_iso_3", right_on="code_country", how="left"
    ).fillna(0)

    return reporte

def boxplot_flujo_migratorio_sin_outliers(datos, decadas):
    """Genera boxplots del flujo migratorio separados para América y otras regiones sin outliers extremos."""
    # Obtener el reporte de sedes y migraciones
    reporte = reporte_sedes_migracion(datos, decadas)

    # Preparar los datos para el boxplot
    flujo_columns = [f'flujo_migratorio_{anio}' for anio in decadas]
    flujo_melted = reporte.melt(id_vars=['pais_iso_3', 'sedes_count', 'secciones_prom', 'code_country'],
                                value_vars=flujo_columns,
                                var_name='anio',
                                value_name='flujo_migratorio')

    # Unir con la tabla de regiones para obtener la región geográfica
    flujo_melted = flujo_melted.merge(datos['regiones'], left_on='code_country', right_on='Codigo', how='left')

    # Dividir en América y otras regiones
    americas = flujo_melted[flujo_melted['Region'].isin(['América del Norte', 'América del Sur'])]
    other_regions = flujo_melted[~flujo_melted['Region'].isin(['América del Norte', 'América del Sur'])]

    # Eliminar más outliers (valores > 1.5 desviaciones estándar)
    def remove_outliers(df, threshold=1.5):
        std = df['flujo_migratorio'].std()
        mean = df['flujo_migratorio'].mean()
        df_clean = df[(df['flujo_migratorio'] >= mean - threshold * std) & (df['flujo_migratorio'] <= mean + threshold * std)]
        return df_clean

    americas_clean = remove_outliers(americas, threshold=1.0)
    other_regions_clean = remove_outliers(other_regions, threshold=2.0)  # Puedes mantener 2 std para otras regiones si prefieres

    # Crear boxplot para América con ancho reducido
    plt.figure(figsize=(12, 6))
    sns.boxplot(data=americas_clean, x='Region', y='flujo_migratorio', palette="Set2", width=0.3)  # Ancho reducido
    plt.title('Boxplot del Flujo Migratorio en América (1960, 1970, 1980, 1990, 2000)')
    plt.xlabel('Región Geográfica')
    plt.ylabel('Flujo Migratorio')
    plt.xticks(rotation=45)
    plt.show()

    # Crear boxplot para otras regiones
    plt.figure(figsize=(12, 6))
    sns.boxplot(data=other_regions_clean, x='Region', y='flujo_migratorio', palette="Set2", width=0.5)  # Ancho por defecto o ajustado
    plt.title('Boxplot del Flujo Migratorio en Otras Regiones (1960, 1970, 1980, 1990, 2000)')
    plt.xlabel('Región Geográfica')
    plt.ylabel('Flujo Migratorio')
    plt.xticks(rotation=45)
    plt.show()

# Definir las décadas a usar
decadas = [1960, 1970, 1980, 1990, 2000]

# Llamar a la función de boxplot
boxplot_flujo_migratorio_sin_outliers(datos, decadas)


"""1. Distribución del Flujo Migratorio en América (América del Sur y América del Norte):
América del Sur: El rango intercuartílico (IQR) es bastante compacto, lo que indica que la mayoría de los 
países en esta región tienen flujos migratorios que no son extremadamente dispersos, pero aún hay un buen 
número de outliers. Estos puntos atípicos sugieren que algunos países han experimentado movimientos migratorios
muy por encima o por debajo del promedio de la región, lo que podría ser el caso de países con situaciones 
políticas o económicas fluctuantes. El hecho de que haya puntos positivos y negativos también señala que 
algunos países han recibido grandes cantidades de inmigrantes mientras otros han visto una significativa emigración.

América del Norte: El rango es más ajustado que el de América del Sur, y hay menos outliers, lo que sugiere una 
distribución más uniforme del flujo migratorio en esta región. Sin embargo, el número de puntos atípicos sugiere 
que ciertos eventos (económicos o políticos) han causado fluctuaciones importantes en ciertos años, pero en 
general, parece menos volátil que América del Sur.

2. Distribución del Flujo Migratorio en Otras Regiones (África, Asia, Europa, Oceanía):
Oceanía es la región que muestra la mayor dispersión, con flujos migratorios que varían ampliamente y algunos 
valores extremadamente altos. Esto podría ser indicativo de que, aunque en general esta región no tiene un número 
elevado de migraciones, eventos específicos han causado grandes flujos de personas hacia o desde países en Oceanía 
en algunas décadas.
Europa/Asia parece tener una distribución más estable y estrecha, lo cual se esperaría debido a la estabilidad 
relativa en las políticas migratorias de muchos de sus países, aunque todavía muestra algunos outliers.
África y Asia también muestran rangos ajustados, lo que podría indicar que en su mayoría estos continentes han 
tenido flujos migratorios relativamente estables, con algunas excepciones que han llevado a grandes movimientos 
poblacionales en ciertos años.
3. Outliers:
A pesar de eliminar algunos outliers con el umbral definido, todavía hay muchos puntos extremos en los 
datos, particularmente en América del Sur y en Oceanía. Esto es esperable dado el impacto de eventos políticos, guerras 
o crisis económicas en la migración, especialmente en regiones como América del Sur y ciertas partes de Europa y Asia.

Conclusión y posibles mejoras:
En el caso de América, podrías considerar investigar más en profundidad qué países están impulsando esos outliers en 
América del Sur y qué eventos históricos o sociales podrían estar detrás.
Para la región de Oceanía, es evidente que los flujos migratorios han sido muy variables en ciertos momentos, lo que 
podría sugerir la importancia de eventos muy específicos.
Si aún deseas eliminar más outliers para una mejor visualización, podrías ajustar el umbral del filtro de 
outliers, reduciendo el número de desviaciones estándar permitido o aplicando una transformación a los datos para 
comprimir los valores extremos.
En resumen, los gráficos reflejan la realidad de que las migraciones no son uniformes, y factores políticos, económicos 
y sociales en cada región han causado fluctuaciones importantes en las décadas analizadas."""
