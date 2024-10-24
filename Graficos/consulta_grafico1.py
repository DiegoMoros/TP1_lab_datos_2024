import pandasql as psql
import pandas as pd
import matplotlib.pyplot as plt # Para graficar series multiples
from Helpers.regularize import save_csv

def cosulta_auxiliar_regiones(datos,save_df = False):
    """"""
    consultaSQL= '''
        SELECT DISTINCT r.Region AS region_geografica , COUNT(s.pais_iso_3) AS cantidad_de_sedes
        FROM regiones AS r
        JOIN sedes_basico AS s ON r.Codigo = s.pais_iso_3 
        GROUP BY region_geografica
        ORDER BY cantidad_de_sedes ASC
    '''
    sedesXregion = psql.sqldf(consultaSQL, env=datos)
    if save_df:
        save_csv(sedesXregion,"SedesPorRegion")
    return [consultaSQL,sedesXregion]

#GRAFICO DE BARRAS 
def plot_number_of_branches(datos):
    """"""
    sedesPorRegion = cosulta_auxiliar_regiones(datos,True)
    fig, ax = plt.subplots()

    plt.rcParams['font.family'] = 'sans-serif'           

    ax.barh(y='region_geografica', width='cantidad_de_sedes', data=sedesPorRegion)

    ax.set_title('Cantidad de sedes por región geográfica')
    ax.set_xlabel('Cantidad de sedes', fontsize='medium')
    ax.set_ylabel('Región geográfica', fontsize='medium')
    ax.set_xticks([])

    ax.bar_label(ax.containers[0], fontsize=8)

    plt.show()
