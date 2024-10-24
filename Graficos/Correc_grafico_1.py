import pandas as pd
import pandasql as psql

carpeta = "Datos/"

# Cargar Datos Migraciones 
migraciones = pd.read_csv(carpeta + "migraciones.csv")
sedes_basico  = pd.read_csv(carpeta + "sedes_min.csv")
sedes_completo = pd.read_csv(carpeta + "sedes.csv")
secciones = pd.read_csv(carpeta + "secciones.csv")
#fue necesario agregar una base de datos externa para poder relacionar la info dada en un principio de los paises con su región 
regiones = pd.read_csv(carpeta + "codigo_region.csv")

import pandasql as psql
import pandas as pd
import matplotlib.pyplot as plt # Para graficar series multiples


consultaSQL= '''
    SELECT DISTINCT r.Region AS region_geografica , COUNT(s.pais_iso_3) AS cantidad_de_sedes
    FROM regiones AS r
    JOIN sedes_basico AS s ON r.Codigo = s.pais_iso_3 
    GROUP BY region_geografica
    ORDER BY cantidad_de_sedes ASC
'''
sedesXregion = psql.sqldf(consultaSQL, locals())
def plot_number_of_branches():
    """"""

    sedesXregion.to_csv(carpeta+'SedesPorRegion.csv', sep=';', index = False)

    #GRAFICO DE BARRAS 

    sedesPorRegion = pd.read_csv(carpeta + "SedesPorRegion.csv", sep=';')

    # Genera el grafico de barras de las ventas mensuales (mejorando la informacion mostrada)
    fig, ax = plt.subplots()

    plt.rcParams['font.family'] = 'sans-serif'           

    # Cambia `bar` por `barh` para barras horizontales
    ax.barh(y='region_geografica', width='cantidad_de_sedes', data=sedesPorRegion)

    ax.set_title('Cantidad de sedes por región geográfica')
    ax.set_xlabel('Cantidad de sedes', fontsize='medium')
    ax.set_ylabel('Región geográfica', fontsize='medium')
    ax.set_xticks([])

    # Agrega las etiquetas a las barras
    ax.bar_label(ax.containers[0], fontsize=8)