# -*- coding: utf-8 -*-
"""
Created on Sun Sep 22 15:29:51 2024

@author: mgo20
"""

import pandas as pd
import pandasql as psql

# Importamos los datasets que vamos a utilizar en este programa
carpeta = "C:/Users/mgo20/OneDrive/Desktop/Data/plab/"

# Cargar Datos Migraciones 
datos_migraciones = pd.read_csv(carpeta + "datos_migraciones.csv")

# Cargar otros datasets si los necesitas
basicos = pd.read_csv(carpeta + "lista-sedes.csv")
completo = pd.read_csv(carpeta + "lista-sedes-datos(1).csv")
secciones = pd.read_csv(carpeta + "lista-secciones.csv")
#fue necesario agregar una base de datos externa para poder relacionar la info dada en un principio de los paises con su región 
region = pd.read_csv(carpeta + "codigoRegion.csv")
#%% 
# Consulta SQL
consultaSQL = '''
               SELECT *
               FROM datos_migraciones
               WHERE "Country Origin Name" = 'Argentina';
              '''

# Ejecutar la consulta
datos_migraciones_Argentina = psql.sqldf(consultaSQL)

# Mostrar los resultados
print(datos_migraciones_Argentina)


#%%
# Este no es necesaro, solo lo usé para que se me hiciera más facil buscar y completar los paises y sus regiones 
# Consulta SQL
consultaSQL = '''
               SELECT "Country Dest Name","Country Dest Code"
               FROM datos_migraciones
               WHERE "Country Origin Name" = 'Argentina'
               Group by  "Country Dest Name";
              '''

# Ejecutar la consulta
datos_migraciones_Paises = psql.sqldf(consultaSQL)

# Mostrar los resultados
print(datos_migraciones_Paises)

#%%
# Es un left Join para que machee con los datos de la tabla que me interesa completar 
# Consulta SQL
consultaSQL = '''
               SELECT "Country Dest Name","Country Dest Code", Región
               FROM datos_migraciones_Argentina
               LEFT JOIN region
               ON "Country Dest Code" = Código 
               Group by  "Country Dest Code";
              '''

# Ejecutar la consulta
datos_migraciones_PaisesR = psql.sqldf(consultaSQL)

# Mostrar los resultados
print(datos_migraciones_PaisesR)

