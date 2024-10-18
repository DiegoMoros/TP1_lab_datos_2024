import pandasql as psql

"""
    Confeccionar un reporte con la información de redes sociales, donde se
    indique para cada caso: el país, la sede, el tipo de red social y url utilizada.
    Ordenar de manera ascendente por nombre de país, sede, tipo de red y
    finalmente por url.
    
    datos: 
        tabla sedes(modificada con la funcion split_in_df): columnas que tiene la tabla
            Se seleccionan las siguientes columnas:
            pais_castellano se renombra como País.
            sede_id se renombra como Sede.
            La red social se determina utilizando un bloque CASE que identifica la red social en función del contenido de las columnas de redes sociales (como %facebook.com% o %twitter.com%).
            Se extrae la URL completa de la columna correspondiente a la red social (como redes_sociales_1, redes_sociales_2, etc.).
            example:
            Afganistán,AFG,Asia
  
        
    Problemática
        Datos incompletos: Se puede observar, muchas sedes tienen redes sociales vacías o 
        nulas en algunas columnas. Esto podría generar un reporte con algunos huecos en la 
        información o sesgar la interpretación sobre la cobertura real de cada sede en las redes.
        
        Normalización de nombres de redes sociales: Existen variaciones en las URLs de redes sociales, 
        lo que puede generar problemas en la clasificación automática. Por ejemplo, la red "Twitter" 
        tiene diferentes variantes de URLs (twitter.com y x.com), lo que necesita un tratamiento especial
        en la consulta para agrupar estos casos bajo el mismo nombre.
        
        Redundancia en las redes: Algunas sedes tienen varias redes sociales para la misma plataforma 
        (ej. múltiples cuentas de Facebook o Twitter), lo cual puede generar duplicados si no se filtran
        adecuadamente los datos.
"""

def reporte_redes_sociales(datos_redes):
    """"""
    redes_sociales_df = datos_redes
    consulta_reporte ='''
        SELECT DISTINCT pais_castellano AS País, sede_id AS Sede, 
            CASE
               WHEN redes_sociales_1 LIKE '%facebook.com%' THEN 'Facebook'
               WHEN redes_sociales_1 LIKE '%instagram.com%' THEN 'Instagram'
               WHEN redes_sociales_1 LIKE '%twitter.com%' THEN 'Twitter'
               WHEN redes_sociales_1 LIKE '%linkedin.com%' THEN 'Linkedin'
               WHEN redes_sociales_1 LIKE '%flickr.com%' THEN 'Flickr'
               WHEN redes_sociales_1 LIKE '%youtube.com%' THEN 'Youtube'
               WHEN redes_sociales_1 LIKE '%x.com%' THEN 'Twitter'
            END AS 'Red Social', 
            redes_sociales_1 AS URL
            
        FROM  redes_sociales_df
        WHERE redes_sociales_1 IS NOT NULL
        UNION
        
        SELECT DISTINCT pais_castellano AS País, sede_id AS Sede,
            CASE
               WHEN redes_sociales_2 LIKE '%facebook.com%' THEN 'Facebook'
               WHEN redes_sociales_2 LIKE '%instagram.com%' THEN 'Instagram'
               WHEN redes_sociales_2 LIKE '%twitter.com%' THEN 'Twitter'
               WHEN redes_sociales_2 LIKE '%linkedin.com%' THEN 'Linkedin'
               WHEN redes_sociales_2 LIKE '%flickr.com%' THEN 'Flickr'
               WHEN redes_sociales_2 LIKE '%youtube.com%' THEN 'Youtube'
               WHEN redes_sociales_2 LIKE '%x.com%' THEN 'Twitter'
            END AS 'Red Social', 
            redes_sociales_2 AS URL
            
        FROM  redes_sociales_df
        WHERE redes_sociales_2 IS NOT NULL
        UNION
        
        SELECT DISTINCT pais_castellano AS País, sede_id AS Sede,
            CASE
               WHEN redes_sociales_3 LIKE '%facebook.com%' THEN 'Facebook'
               WHEN redes_sociales_3 LIKE '%instagram.com%' THEN 'Instagram'
               WHEN redes_sociales_3 LIKE '%twitter.com%' THEN 'Twitter'
               WHEN redes_sociales_3 LIKE '%linkedin.com%' THEN 'Linkedin'
               WHEN redes_sociales_3 LIKE '%flickr.com%' THEN 'Flickr'
               WHEN redes_sociales_3 LIKE '%youtube.com%' THEN 'Youtube'
               WHEN redes_sociales_3 LIKE '%x.com%' THEN 'Twitter'
            END AS 'Red Social', 
            redes_sociales_3 AS URL
            
        FROM  redes_sociales_df
        WHERE redes_sociales_3 IS NOT NULL
        UNION
        
        SELECT DISTINCT pais_castellano AS País, sede_id AS Sede,
            CASE
               WHEN redes_sociales_4 LIKE '%facebook.com%' THEN 'Facebook'
               WHEN redes_sociales_4 LIKE '%instagram.com%' THEN 'Instagram'
               WHEN redes_sociales_4 LIKE '%twitter.com%' THEN 'Twitter'
               WHEN redes_sociales_4 LIKE '%linkedin.com%' THEN 'Linkedin'
               WHEN redes_sociales_4 LIKE '%flickr.com%' THEN 'Flickr'
               WHEN redes_sociales_4 LIKE '%youtube.com%' THEN 'Youtube'
               WHEN redes_sociales_4 LIKE '%x.com%' THEN 'Twitter'
            END AS 'Red Social',  
            redes_sociales_4  AS URL
            
        FROM  redes_sociales_df
        WHERE redes_sociales_4 IS NOT NULL
        UNION
        
        SELECT DISTINCT pais_castellano AS País, sede_id AS Sede,  
            CASE
               WHEN redes_sociales_5 LIKE '%facebook.com%' THEN 'Facebook'
               WHEN redes_sociales_5 LIKE '%instagram.com%' THEN 'Instagram'
               WHEN redes_sociales_5 LIKE '%twitter.com%' THEN 'Twitter'
               WHEN redes_sociales_5 LIKE '%linkedin.com%' THEN 'Linkedin'
               WHEN redes_sociales_5 LIKE '%flickr.com%' THEN 'Flickr'
               WHEN redes_sociales_5 LIKE '%youtube.com%' THEN 'Youtube'
               WHEN redes_sociales_5 LIKE '%x.com%' THEN 'Twitter'
            END AS 'Red Social',  
            redes_sociales_5  AS URL
             
        FROM  redes_sociales_df
        WHERE redes_sociales_5 IS NOT NULL
        UNION
        
        SELECT DISTINCT pais_castellano AS País, sede_id AS Sede,  
            CASE
               WHEN redes_sociales_6 LIKE '%facebook.com%' THEN 'Facebook'
               WHEN redes_sociales_6 LIKE '%instagram.com%' THEN 'Instagram'
               WHEN redes_sociales_6 LIKE '%twitter.com%' THEN 'Twitter'
               WHEN redes_sociales_6 LIKE '%linkedin.com%' THEN 'Linkedin'
               WHEN redes_sociales_6 LIKE '%flickr.com%' THEN 'Flickr'
               WHEN redes_sociales_6 LIKE '%youtube.com%' THEN 'Youtube'
               WHEN redes_sociales_6 LIKE '%x.com%' THEN 'Twitter'
            END AS 'Red Social',  
            redes_sociales_6  AS URL
             
        FROM  redes_sociales_df
        WHERE redes_sociales_6 IS NOT NULL
        UNION
        
        SELECT DISTINCT pais_castellano AS País, sede_id AS Sede,  
            CASE
               WHEN redes_sociales_7 LIKE '%facebook.com%' THEN 'Facebook'
               WHEN redes_sociales_7 LIKE '%instagram.com%' THEN 'Instagram'
               WHEN redes_sociales_7 LIKE '%twitter.com%' THEN 'Twitter'
               WHEN redes_sociales_7 LIKE '%linkedin.com%' THEN 'Linkedin'
               WHEN redes_sociales_7 LIKE '%flickr.com%' THEN 'Flickr'
               WHEN redes_sociales_7 LIKE '%youtube.com%' THEN 'Youtube'
               WHEN redes_sociales_7 LIKE '%x.com%' THEN 'Twitter'
            END AS 'Red Social',  
            redes_sociales_7  AS URL
            
        FROM  redes_sociales_df
        WHERE redes_sociales_7 IS NOT NULL
        
        ORDER BY País ASC, Sede ASC, "Red Social" ASC, URL ASC;
    '''
       
    redes_sociales = psql.sqldf(consulta_reporte, locals())

    return [consulta_reporte ,redes_sociales]
