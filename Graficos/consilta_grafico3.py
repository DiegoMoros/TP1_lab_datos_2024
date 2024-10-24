# -*- coding: utf-8 -*-
"""
Created on Tue Oct 22 18:04:16 2024

@author: niqui
"""
import pandasql as psql
import pandas as pd
import matplotlib.pyplot as plt # Para graficar series multiples

carpeta = "Datos/"
sedeCompleta =  pd.read_csv(carpeta+"sedes.csv")
migracion = pd.read_csv(carpeta + "migraciones.csv")


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
        ORDER BY f.Pais ASC
'''
flujo_y_sedes = psql.sqldf(consulta_flujo_y_sedes, locals())


flujo_y_sedes.to_csv(carpeta+"flujo_y_sedes.csv" , sep=';', index = False)

def migration_flow_vs_number_of_branches():
        # Cargo el dataframe desde el archivo CSV
        df = pd.read_csv(carpeta+"flujo_y_sedes.csv", sep=';')

        # Filtramos los datos para que solo incluyan puntos dentro de los límites deseados en el eje X
        df_filtered = df[(df['flujo_migratorio'] > -1000) & (df['flujo_migratorio'] < 1010)]

        # Añadimos jitter para separar puntos superpuestos
        #jitter = np.random.normal(0, 0.2, size=len(df_filtered))

        plt.figure(figsize=(10,6))

        # Creamos el scatter plot
        plt.scatter(df_filtered['flujo_migratorio'], df_filtered['cantidad_sedes'], color='b', s=50, alpha=0.7)

        # Etiquetas del gráfico
        plt.title('Relación entre Flujo Migratorio y Cantidad de Sedes por País')
        plt.xlabel('Flujo Migratorio (Inmigrantes - Emigrantes)')
        plt.ylabel('Cantidad de Sedes')

        # Añadir nombres de los países como etiquetas, pero solo para países filtrados
        for i, pais in enumerate(df_filtered['Pais']):
                if (df_filtered['flujo_migratorio'].iloc[i]) > 700 or (df_filtered['flujo_migratorio'].iloc[i]) < -200 or df_filtered['cantidad_sedes'].iloc[i] > 1:  # Etiquetar solo países relevantes
                        plt.text(df_filtered['flujo_migratorio'].iloc[i], df_filtered['cantidad_sedes'].iloc[i], pais, fontsize=9, ha='right')

        # Acotar el rango del eje X
        plt.xlim(-750, 1010)
        plt.ylim(0.5,2.5)

        plt.grid(True)
        plt.show()


        # Crear un gráfico de burbujas
        plt.figure(figsize=(10,6))

        # El tamaño de las burbujas estará relacionado con la cantidad de sedes
        bubble_size = df['cantidad_sedes'] * 100  # Multiplicamos para que las burbujas sean visibles

        # Crear el scatter plot con tamaño variable de las burbujas
        plt.scatter(df['cantidad_sedes'], df['flujo_migratorio'], s=bubble_size, alpha=0.5, c='b', edgecolors='w', linewidth=1)

        # Añadir etiquetas y título
        plt.title('Relación entre Flujo Migratorio y Cantidad de Sedes por País (Tamaño = Cantidad de Sedes)', fontsize=14)
        plt.xlabel('Cantidad de Sedes', fontsize=12)
        plt.ylabel('Flujo Migratorio', fontsize=12)

        # Añadir etiquetas para cada país en los puntos
        for i, pais in enumerate(df['Pais']):
                if (df['cantidad_sedes'].iloc[i]) > 2:
                        plt.text(df['cantidad_sedes'][i], df['flujo_migratorio'][i], pais, fontsize=9)

        plt.grid(True)
        plt.show()
        