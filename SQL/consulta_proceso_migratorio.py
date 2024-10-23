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
    SELECT 
    `Country Origin Code` AS ISO_pais_origen, 
    `Migration by Gender Code` AS Gender_Code, 
    `Country Dest Code` AS ISO_pais_destino, 
    `1960 [1960]`, 
    `1970 [1970]`, 
    `1980 [1980]`, 
    `1990 [1990]`, 
    `2000 [2000]`
    FROM migraciones;
'''

union_resultado = psql.sqldf(consultaUnion)
print(union_resultado)