from Helpers.datos import get_datos
from Helpers.regularize import split_in_df
from Helpers.consultas_aux import *
from SQL.consulta1 import reporte_sedes_migracion
from SQL.consulta2 import reporte_region_geografica
from SQL.corre_ejer2 import reporte_unificado_flujo_sedes
from SQL.consulta3 import reporte_variacion_redes
from SQL.consulta4 import reporte_redes_sociales, consultaFinal, consultaNombre
from Graficos.consulta_grafico2 import migratory_flow_graph
from Graficos.Correc_grafico_1 import plot_number_of_branches
from Graficos.consilta_grafico3 import migration_flow_vs_number_of_branches

##Auxiliares
datos = get_datos()
decadas = [1960, 1970, 1980, 1990, 2000]
redes_sociales_df = split_in_df(datos["sedes_completo"], 'redes_sociales', '// ', drop_col=True, save_csv=True)
sedes_diplomaticas = consulta_sede_diplomatica(datos,save_df=True)
proceso_migratorio = consulta_proceso_migratorio(datos,save_df=True)
##Consultas
# ejercicio1 = reporte_sedes_migracion(datos,save_df = True)
# ejercicio2 = reporte_region_geografica(datos,save_df = True)
# ejercicio4 = reporte_redes_sociales(redes_sociales_df,save_df = True)
# ejercicio3 = reporte_variacion_redes(ejercicio4[1],save_df = True)
# ejercicio4_1 = consultaFinal(datos, redes_sociales_df)
# ejercicio4_2 = consultaNombre(datos)
ejercicio2 = reporte_unificado_flujo_sedes(save_df=True)
##Graficos
# grafico2_americas = migratory_flow_graph(datos,decadas,isAmericas=True)
# grafico2_others = migratory_flow_graph(datos,decadas)
grafico1 = plot_number_of_branches()
grafico3 = migration_flow_vs_number_of_branches()