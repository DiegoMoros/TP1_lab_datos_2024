# -*- coding: utf-8 -*-
"""
Created on Tue Oct 22 18:04:19 2024

@author: niqui
"""
# Importamos bibliotecas
import pandasql as psql
import pandas as pd
import matplotlib.pyplot as plt # Para graficar series multiples

# Carpeta donde se encuentran los archivos a utilizar

carpeta = "C:\\Users\\niqui\\Documents\\GitHub\\TP1_lab_datos_2024\\Datos\\"
sedeCompleta =  pd.read_csv(carpeta+"sedes.csv")


consultaSQL= '''
    SELECT DISTINCT region_geografica , COUNT(DISTINCT sede_id ) AS cantidad_de_sedes
    FROM sedeCompleta
    GROUP BY region_geografica
    ORDER BY cantidad_de_sedes ASC
'''
sedesXregion = psql.sqldf(consultaSQL, locals())
sedesXregion.to_csv(carpeta+'SedesPorRegion.csv', sep=';', index = False)

#GRAFICO DE BARRAS 

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
