import pandas as pd
import pandasql as psql

# Importamos los datasets que vamos a utilizar en este programa
carpeta = "C:/Users/mgo20/OneDrive/Desktop/Data/plab/TP1_lab_datos_2024/Datos/"

# Cargar Datos Migraciones 
migraciones = pd.read_csv(carpeta + "migraciones.csv")
sedes_basico  = pd.read_csv(carpeta + "sedes_min.csv")
sedes_completo = pd.read_csv(carpeta + "sedes.csv")
secciones = pd.read_csv(carpeta + "secciones.csv")
#fue necesario agregar una base de datos externa para poder relacionar la info dada en un principio de los paises con su región 
regiones = pd.read_csv(carpeta + "codigo_region.csv")

#%%
#esta consulta me da los paises donde el origen y destino es argentina, Así puedo calcular el flujo migratorio 
# Consulta SQL
consulta_cantidad_de_sedes = '''
        SELECT r.Region AS region_geografica, COUNT(DISTINCT s.pais_iso_3) AS Paises_con_sedes
        FROM regiones AS r
        JOIN sedes_basico AS s ON r.Codigo = s.pais_iso_3  -- Relacionamos las tablas correctamente
        GROUP BY r.Region
        ORDER BY r.Region
    '''
cantidad_sedes = psql.sqldf(consulta_cantidad_de_sedes)
# Mostrar los resultados
print(cantidad_sedes)

#ejercicio 2
""" 
    Reportar agrupando por región geográfica: a) la cantidad de países en que
    Argentina tiene al menos una sede y b) el promedio del flujo migratorio de
    Argentina hacia esos países en el año 2000 (promedio sobre países donde
    Argentina tiene sedes). Ordenar de manera descendente por este último
    campo.
"""   
'''Primero creo una tabla que contenga la cantidad de sedes por pais, despues hago una tabla 
que me muestre la cantidad de emigrantes de todos los paises hacia Argentina y otra con la 
cantidad de inmigrantes de argentina hacia el resto de todos los paises'''

consultaSQL = '''
        SELECT DISTINCT pais_iso_3 AS Pais, COUNT(DISTINCT sede_id) AS cantidad_sedes
        FROM sedes_completo
        GROUP BY Pais
        ORDER BY Pais
    '''
cantidad_sedes_pais = psql.sqldf(consultaSQL)
print(cantidad_sedes_pais)
#%%
consulta_emigraciones = '''
        SELECT DISTINCT "Country Origin Code" AS pais_origen, "2000 [2000]" AS cantidad_emigrantes
        FROM migraciones
        WHERE "Country Dest Code" = 'ARG' AND "Migration by Gender Code" = 'TOT'
        ORDER BY "Country Origin Code" ASC
'''  
emigraciones = psql.sqldf(consulta_emigraciones)

consulta_inmigraciones = '''
       SELECT DISTINCT "Country Dest Code" AS pais_destino, "2000 [2000]" AS cantidad_inmigrantes
       FROM migraciones
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
flujo_mig = psql.sqldf(consulta_flujo_migratorio)


consulta_flujo_con_sedes = '''
        SELECT DISTINCT f.Pais, f.flujo_migratorio
        FROM flujo_mig AS f
        INNER JOIN cantidad_sedes_pais AS c
        ON c.pais = f.pais
        ORDER BY f.Pais ASC
'''
flujo_con_sedes = psql.sqldf(consulta_flujo_con_sedes)
print(flujo_con_sedes)


"Ahora agrupo el flujo migratorio por región"
consulta_flujo_migratorio2= f'''
        SELECT c.Region AS region_geografica,
            AVG(fmn.flujo_migratorio) AS promedio_flujo_neto_arg_2000
        FROM flujo_con_sedes AS fmn
        LEFT JOIN regiones c 
            ON fmn.Pais = c.Codigo
        WHERE fmn.Pais IS NOT NULL
        GROUP BY c.Region
        ORDER BY promedio_flujo_neto_arg_2000 DESC
        '''
resultado = psql.sqldf(consulta_flujo_migratorio2)
print(resultado)

consulta_reporte_final = '''
        SELECT DISTINCT tt.region_geografica AS Region_geografica, 
        tt.Paises_con_sedes AS Paises_con_sedes,
        ss.promedio_flujo_neto_arg_2000 AS promedio_flujo_neto_arg_2000
        FROM cantidad_sedes AS tt 
        JOIN resultado AS ss 
        ON tt.region_geografica = ss.region_geografica
        ORDER BY promedio_flujo_neto_arg_2000 ASC
'''
reporte_final = psql.sqldf(consulta_reporte_final)
print(reporte_final)