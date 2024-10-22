import pandasql as psql
from Helpers.datos import get_datos
from SQL.consulta1 import reporte_sedes_migracion

datos = get_datos() 
#ejercicio 2
""" 
    Reportar agrupando por región geográfica: a) la cantidad de países en que
    Argentina tiene al menos una sede y b) el promedio del flujo migratorio de
    Argentina hacia esos países en el año 2000 (promedio sobre países donde
    Argentina tiene sedes). Ordenar de manera descendente por este último
    campo.
    
    datos: 
        tabla regiones: columnas que tiene la tabla
            Pais,Codigo,Region
            example:
            Afganistán,AFG,Asia
        tabla sedes_basico: columnas que tiene la tabla
            sede_id,sede_desc_castellano,sede_desc_ingles,pais_iso_2,pais_iso_3,pais_castellano,pais_ingles,ciudad_castellano,ciudad_ingles,estado,sede_tipo
            example:
            CANTO,"Consulado  en  Antofagasta","Consulate  in  ANTOFAGASTA",CL,CHL,"REPÚBLICA  DE  CHILE","REPUBLIC  OF  CHILE","Antofagasta","Antofagasta",Activo,"Consulado"

    Probelmatica: 
        la cantidad de países en que Argentina tiene al menos una sede y el promedio del flujo migratorio de Argentina hacia esos países en el año 2000 (promedio sobre países donde Argentina tiene sedes).
        
"""
def cant_country_sedes(datos):
    """"""
    consulta_cantidad_de_sedes = '''
        SELECT r.Region AS Región_geográfica, COUNT(DISTINCT s.pais_iso_3) AS PaísesConSedes
        FROM regiones AS r
        JOIN sedes_basico AS s ON r.Codigo = s.pais_iso_3  -- Relacionamos las tablas correctamente
        GROUP BY r.Region
        ORDER BY r.Region
    '''
    cantidad_sedes = psql.sqldf(consulta_cantidad_de_sedes, env=datos)
    return [consulta_cantidad_de_sedes,cantidad_sedes]

#print(cant_country_sedes(datos)[1])


#%%    
def flujo_migratorio_por_region(datos):
    consulta_reporte_flujo_migratorio = reporte_sedes_migracion(datos)[0]

    consulta_flujo_migratorio2= f'''
    
    SELECT c.Region AS region_geografica,
           AVG(fmn.flujo_migratorio_neto) AS promedio_flujo_neto_arg_2000
    FROM ({consulta_reporte_flujo_migratorio}) AS fmn
    LEFT JOIN regiones c 
        ON fmn.country = c.Codigo
    WHERE fmn.country IS NOT NULL
    GROUP BY c.Region
    ORDER BY promedio_flujo_neto_arg_2000 DESC
     '''

    resultado = psql.sqldf(consulta_flujo_migratorio2, env=datos)
    return [consulta_flujo_migratorio2,resultado]
#print(flujo_migratorio_por_region(datos)[1])




#%%

def  reporte_region_geografica(datos):
    
    consulta_flujo_migratorio_por_region = flujo_migratorio_por_region(datos)[0]
    consulta_cant_country_sedes = cant_country_sedes(datos)[0]
      

    consulta_final = f'''
        SELECT ss.Región_geográfica AS Región_geográfica, 
               ss.PaísesConSedes AS Países_Con_Sedes_Argentinas, 
               fmn.promedio_flujo_neto_arg_2000 AS Promedio_flujo_con_Argentina
        FROM ({consulta_cant_country_sedes}) AS ss
        LEFT JOIN (
            SELECT region_geografica, promedio_flujo_neto_arg_2000
            FROM ({consulta_flujo_migratorio_por_region})
        ) AS fmn
        ON ss.Región_geográfica = fmn.region_geografica
        ORDER BY promedio_flujo_neto_arg_2000 DESC
    '''
    resultado = psql.sqldf(consulta_final, env=datos)
    return [consulta_final,resultado]
#print(reporte_region_geografica(datos)[1])
