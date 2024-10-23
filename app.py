from Helpers.datos import get_datos
from Helpers.regularize import split_in_df
from SQL.consulta1 import reporte_sedes_migracion
from SQL.consulta2 import reporte_region_geografica
from SQL.consulta3 import reporte_variacion_redes
from SQL.consulta4 import reporte_redes_sociales
#from Graficos.consulta_grafico2 import migratory_flow_graph

##Auxiliares
datos = get_datos()
decadas = [1960, 1970, 1980, 1990, 2000]
redes_sociales_df = split_in_df(datos["sedes_completo"], 'redes_sociales', '// ', drop_col=True, save_csv=True)

##Consultas
ejercicio1 = reporte_sedes_migracion(datos,save_df = True)
ejercicio2 = reporte_region_geografica(datos,save_df = True)
ejercicio4 = reporte_redes_sociales(redes_sociales_df,datos,save_df = True)
ejercicio3 = reporte_variacion_redes(ejercicio4[1],save_df = True)


##Graficos
# grafico2_americas = migratory_flow_graph(datos,decadas,isAmericas=True)
# grafico2_others = migratory_flow_graph(datos,decadas)
