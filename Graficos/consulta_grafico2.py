from SQL.consulta1 import sedes_secciones
import pandasql as psql
import matplotlib.pyplot as plt
import seaborn as sns
"""
Boxplot, por cada región geográfica, del valor correspondiente al promedio
del flujo migratorio (para calcular el promedio tomar los valores anuales
correspondientes a los años 1960, 1970, 1980, 1990 y 2000) de los países
donde Argentina tiene una delegación. Mostrar todos los boxplots en una
misma figura, ordenados por la mediana de cada región.
"""

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

def flujos_migratorios_por_decadas(datos, decadas):
    """Genera y unifica los flujos migratorios por varias décadas."""
    resultados = []
    for decada in decadas:
        flujo = flujo_migratorio_por_anio(decada, datos)
        resultados.append(flujo)

    flujo_total = resultados[0]
    for flujo in resultados[1:]:
        flujo_total = flujo_total.merge(flujo, on=["code_country", "country"], how="outer")

    return flujo_total

def reporte_sedes_migracion(datos, decadas):
    """Genera reporte unificado de sedes y migraciones."""
    sedes_sec = sedes_secciones(datos,False)[1]
    flujos_migratorios = flujos_migratorios_por_decadas(datos, decadas)

    reporte = sedes_sec.merge(
        flujos_migratorios, left_on="pais_iso_3", right_on="code_country", how="left"
    ).fillna(0)

    return reporte

def remove_outliers(df, threshold=1.5):
        std = df['flujo_migratorio'].std()
        mean = df['flujo_migratorio'].mean()
        df_clean = df[(df['flujo_migratorio'] >= mean - threshold * std) & (df['flujo_migratorio'] <= mean + threshold * std)]
        return df_clean

def migratory_flow_graph(datos, decadas,isAmericas = False):
    """Genera boxplots del flujo migratorio separados para América y otras regiones sin outliers extremos."""
    reporte = reporte_sedes_migracion(datos, decadas)

    flujo_columns = [f'flujo_migratorio_{anio}' for anio in decadas]
    flujo_melted = reporte.melt(id_vars=['pais_iso_3', 'sedes_count', 'secciones_prom', 'code_country'],
                                value_vars=flujo_columns,
                                var_name='anio',
                                value_name='flujo_migratorio')

    flujo_melted = flujo_melted.merge(datos['regiones'], left_on='code_country', right_on='Codigo', how='left')

    americas = flujo_melted[flujo_melted['Region'].isin(['América del Norte', 'América del Sur'])]
    other_regions = flujo_melted[~flujo_melted['Region'].isin(['América del Norte', 'América del Sur'])]

    americas_clean = remove_outliers(americas, threshold=1.0)
    other_regions_clean = remove_outliers(other_regions, threshold=2.0)  
    
    if isAmericas:
        # Crear boxplot para América con ancho reducido
        plt.figure(figsize=(12, 6))
        sns.boxplot(data=americas_clean, x='Region', y='flujo_migratorio', palette="Set2", width=0.3)  # Ancho reducido
        plt.title('Boxplot del Flujo Migratorio en América (1960, 1970, 1980, 1990, 2000)')
        plt.xlabel('Región Geográfica')
        plt.ylabel('Flujo Migratorio')
        plt.xticks(rotation=45)
        plt.yticks(ticks=range(-2000000, 3000000, 1000000),
                   labels=[f'{tick/1e6:.1f}M' for tick in range(-2000000, 3000000, 1000000)])
        plt.show()
    else:
        # Crear boxplot para otras regiones
        plt.figure(figsize=(12, 6))
        sns.boxplot(data=other_regions_clean, x='Region', y='flujo_migratorio', palette="Set2", width=0.5)  # Ancho por defecto o ajustado
        plt.title('Boxplot del Flujo Migratorio en Otras Regiones (1960, 1970, 1980, 1990, 2000)')
        plt.xlabel('Región Geográfica')
        plt.ylabel('Flujo Migratorio')
        plt.xticks(rotation=45)
        plt.yticks(ticks=range(-3000000, 3000000, 1000000),
                   labels=[f'{tick/1e6:.1f}M' for tick in range(-3000000, 3000000, 1000000)])
        plt.show()

