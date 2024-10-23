import pandas as pd
import pandasql as psql

carpeta = "C:/Users/mgo20/OneDrive/Desktop/Data/plab/TP1_lab_datos_2024/Datos/"

# Cargar Datos Migraciones 
migraciones = pd.read_csv(carpeta + "migraciones.csv")
sedes_basico  = pd.read_csv(carpeta + "sedes_min.csv")
sedes_completo = pd.read_csv(carpeta + "sedes.csv")
secciones = pd.read_csv(carpeta + "secciones.csv")
#fue necesario agregar una base de datos externa para poder relacionar la info dada en un principio de los paises con su región 
regiones = pd.read_csv(carpeta + "codigo_region.csv")

consultaUnion = '''
    SELECT s.sede_id, s.sede_desc_castellano, s.pais_iso_3 AS ISO3, COALESCE(sec.repeticiones, 0) AS cantidad_de_secciones
    FROM sedes_completo AS s
    LEFT JOIN (
        SELECT DISTINCT m.sede_id, COUNT(*) AS repeticiones
        FROM secciones AS m
        GROUP BY m.sede_id
    ) AS sec
    ON s.sede_id = sec.sede_id;
'''

union_result = psql.sqldf(consultaUnion)
print(union_result)

