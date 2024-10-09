import pandasql as psql
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
        ORDER BY r.Region;
    '''
    cantidad_sedes = psql.sqldf(consulta_cantidad_de_sedes, env=datos)
    return [consulta_cantidad_de_sedes,cantidad_sedes]


def prom_flujo_migratorio_arg(datos):
    """"""
    consulta_flujo_migratorio_arg = '''
    
    '''
    prom_flujo_migratorio_arg = psql.sqldf(consulta_flujo_migratorio_arg, env=datos)
    return [consulta_flujo_migratorio_arg,prom_flujo_migratorio_arg] 


def reporte_region_geografica():
    """"""
    return None