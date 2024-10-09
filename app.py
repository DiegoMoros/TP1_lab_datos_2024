from Helpers.datos import get_datos
from Helpers.regularize import split_in_df
from SQL.consulta1 import reporte_sedes_migracion
from SQL.consulta2 import cant_country_sedes
datos = get_datos()
ejercicio1 = reporte_sedes_migracion(datos)
# print(ejercicio1[1])

# # Ejemplo de uso split_in_df
# # Llamar a la función para separar la columna 'redes_sociales' por '//'
# result = split_in_df(datos["sedes_completo"], 'redes_sociales', '//',drop_col=True)
# print(result)
# print(datos["regiones"])
ejercicio2a = cant_country_sedes(datos)
print(ejercicio2a[1])