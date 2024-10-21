from Helpers.datos import get_datos
from Helpers.regularize import split_in_df
from SQL.consulta1 import reporte_sedes_migracion
from SQL.consulta2 import cant_country_sedes
from SQL.consulta3 import reporte_variacion_redes
from SQL.consulta4 import reporte_redes_sociales

datos = get_datos()
ejercicio1 = reporte_sedes_migracion(datos)
redes_sociales_df = split_in_df(datos["sedes_completo"], 'redes_sociales', '// ', drop_col=True, save_csv=True)
ejercicio2a = cant_country_sedes(datos)
print(redes_sociales_df)

# Llamamos la función de reporte de redes sociales
ejercicio4 = reporte_redes_sociales(redes_sociales_df)

# Llamamos la función de reporte de variación de redes
ejercicio3 = reporte_variacion_redes(ejercicio4[1])

"Cambio2"