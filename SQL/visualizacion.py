# -*- coding: utf-8 -*-
"""
Created on Thu Oct 17 11:37:50 2024

@author: niqui
"""

# Importamos bibliotecas
import pandasql as psql
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt # Para graficar series multiples
from   matplotlib import ticker   # Para agregar separador de miles
import seaborn as sns           # Para graficar histograma

# Carpeta donde se encuentran los archivos a utilizar
#carpeta = "~/Downloads/clase-12/scriptClase/"
carpeta = "C:\\Users\\niqui\\Documents\\GitHub\\TP1_lab_datos_2024\\Datos\\"
sedeCompleta =  pd.read_csv(carpeta+"sedes.csv")
migracion = pd.read_csv(carpeta + "migraciones.csv")


#%%##########################################################################

#i)
consultaSQL= '''
    SELECT DISTINCT region_geografica , COUNT(DISTINCT sede_id ) AS cantidad_de_sedes
    FROM sedeCompleta
    GROUP BY region_geografica
    ORDER BY cantidad_de_sedes ASC
'''
sedesXregion = psql.sqldf(consultaSQL, locals())
sedesXregion.to_csv(carpeta+'SedesPorRegion.csv', sep=';', index = False)

#GRAFICO DE BARRAS (BAR PLOT)

sedesPorRegion = pd.read_csv(carpeta + "SedesPorRegion.csv", sep=';')

# Genera el grafico de barras de las ventas mensuales (mejorando la informacion mostrada)
fig, ax = plt.subplots()

plt.rcParams['font.family'] = 'sans-serif'           

# Cambia `bar` por `barh` para barras horizontales
ax.barh(y='region_geografica', width='cantidad_de_sedes', data=sedesPorRegion)

ax.set_title('Cantidad de sedes por región geográfica')
ax.set_xlabel('Cantidad de sedes', fontsize='medium')
ax.set_ylabel('Región geográfica', fontsize='medium')
ax.set_xticks([])

# Agrega las etiquetas a las barras
ax.bar_label(ax.containers[0], fontsize=8)

plt.show()

#%%##########################################################################

#ii)

#%%##########################################################################

#iii)
'''Primero creo una tabla que contenga la cantidad de sedes por pais, despues hago una tabla 
que me muestre la cantidad de emigrantes de todos los paises hacia Argentina y otra con la 
cantidad de inmigrantes de argentina hacia el resto de todos los paises'''

consultaSQL= '''
        SELECT DISTINCT pais_iso_3 AS Pais, COUNT(DISTINCT sede_id) AS cantidad_sedes

        FROM sedeCompleta
        GROUP BY Pais
        ORDER BY Pais
'''
cantidad_sedes_pais = psql.sqldf(consultaSQL, locals())

consulta_emigraciones = '''
        SELECT DISTINCT "Country Origin Code" AS pais_origen, "2000 [2000]" AS cantidad_emigrantes
        FROM migracion
        WHERE "Country Dest Code" = 'ARG' AND "Migration by Gender Code" = 'TOT'
        ORDER BY "Country Origin Code" ASC
'''
    
emigraciones = psql.sqldf(consulta_emigraciones, locals())

consulta_inmigraciones = '''
       SELECT DISTINCT "Country Dest Code" AS pais_destino, "2000 [2000]" AS cantidad_inmigrantes
       FROM migracion
       WHERE "Country Origin Code" = 'ARG' AND "Migration by Gender Code" = 'TOT'
       ORDER BY "Country Dest Code" ASC

'''

inmigraciones = psql.sqldf(consulta_inmigraciones, locals())

'''Ahora lo que quiero es calcular el flujo migratorio, para esto anteriormente calcule la cantidad de emigrantes
hacia argentina y la acanyidad de inmigrantes de argentina. 
Entonces, hacemos un INNER JOIN para que solo se muestre el flujo migratorio de los países que aparecen en ambas tablas.
'''

# Unimos las tablas por el código de país

consulta_flujo_migratorio = '''
        SELECT DISTINCT pais_origen AS Pais, 
        (inmigraciones.cantidad_inmigrantes - emigraciones.cantidad_emigrantes) AS flujo_migratorio
        FROM emigraciones 
        INNER JOIN inmigraciones
        ON pais_origen = pais_destino
'''

flujo_mig = psql.sqldf(consulta_flujo_migratorio, locals())


consulta_flujo_y_sedes = '''
        SELECT DISTINCT f.Pais, f.flujo_migratorio, c.cantidad_sedes
        FROM flujo_mig AS f
        INNER JOIN cantidad_sedes_pais AS c
        ON c.pais = f.pais
        ORDER BY Pais ASC
'''
flujo_y_sedes = psql.sqldf(consulta_flujo_y_sedes, locals())
















