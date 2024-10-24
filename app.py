"""
Integrantes: Diego Moros, Nicole Britos, María Obregón
Descripción:
    El codigo se encuentra separado en 3 carpetas principales
        ->SQL: Donde realziamos las cosnultas SQL
        ->Helpers: Donde creamos algunas funciones auxiliares
        ->Graficos: Donde generamos los graficos que respalndan nuestroa analsiis 
    Ademas guardamos nuestros resultado en csv de las tablas en la carpeta de Resultados.
    Los datos que usamos como referencia se ecneuntran en la carpeta de Dato
    Manejamos el control de las consultas y procesos dentro de esta rchivo llamado app.py
"""
from Helpers.datos import get_datos
from Helpers.regularize import split_in_df
from Helpers.consultas_aux import *
from SQL.consulta1 import reporte_sedes_migracion
from SQL.consulta2 import reporte_region_geografica
from SQL.consulta3 import reporte_variacion_redes
from SQL.consulta4 import reporte_redes_sociales
from Graficos.consulta_grafico2 import migratory_flow_graph
from Graficos.consulta_grafico1 import plot_number_of_branches
from Graficos.consulta_grafico3 import migration_flow_vs_number_of_branches

##Auxiliares
datos = get_datos()
decadas = [1960, 1970, 1980, 1990, 2000]
redes_sociales_df = split_in_df(datos["sedes_completo"], 'redes_sociales', '// ', drop_col=True, save_csv=True)
sedes_diplomaticas = consulta_sede_diplomatica(datos,save_df=True)
proceso_migratorio = consulta_proceso_migratorio(datos,save_df=True)
redes_y_sedes = consulta_redes(save_df=True)
##Consultas
ejercicio1 = reporte_sedes_migracion(datos,save_df = True)
ejercicio2 = reporte_region_geografica(save_df = True)
ejercicio4 = reporte_redes_sociales(redes_sociales_df,datos,save_df = True)
ejercicio3 = reporte_variacion_redes(ejercicio4[1],save_df = True)
##Graficos
grafico2_americas = migratory_flow_graph(datos,decadas,isAmericas=True)
grafico2_others = migratory_flow_graph(datos,decadas)
grafico1 = plot_number_of_branches()
grafico3 = migration_flow_vs_number_of_branches()