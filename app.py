from Helpers.datos import get_datos
from SQL.consulta1 import reporte_sedes_migracion
# from SQL.consulta2 import reporte_flujo
datos = get_datos()
ejercicio1 = reporte_sedes_migracion(datos)
print(ejercicio1[1])

