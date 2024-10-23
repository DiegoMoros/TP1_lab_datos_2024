import pandasql as psql
from Helpers.regularize import save_csv
from Helpers.datos import get_datos
datos = get_datos()

#ejercicio 4
"""
    Confeccionar un reporte con la información de redes sociales, donde se
    indique para cada caso: el país, la sede, el tipo de red social y url utilizada.
    Ordenar de manera ascendente por nombre de país, sede, tipo de red y
    finalmente por url. 
"""
def reporte_redes_sociales(datos_redes,save_df=False):
    """Reporte general sobre las influencia o participación de las redes sociales en la sedes"""
    redes_sociales_df = datos_redes
    consulta_reporte ='''
        SELECT DISTINCT pais_iso_3 AS País, sede_id AS Sede, 
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
        
        SELECT DISTINCT pais_iso_3 AS País, sede_id AS Sede,
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
        
        SELECT DISTINCT pais_iso_3 AS País, sede_id AS Sede,
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
        
        SELECT DISTINCT pais_iso_3 AS País, sede_id AS Sede,
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
        
        SELECT DISTINCT pais_iso_3 AS País, sede_id AS Sede,  
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
        
        SELECT DISTINCT pais_iso_3 AS País, sede_id AS Sede,  
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
        
        SELECT DISTINCT pais_iso_3 AS País, sede_id AS Sede,  
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
    
    
    if save_df:
        save_csv(redes_sociales,"reporte_redes_sociales")
    return [consulta_reporte ,redes_sociales]

def cansultaNombre(datos):
    consulta_nombre = '''
            SELECT DISTINCT Pais, Codigo
            FROM regiones
    '''
    resultado = psql.sqldf(consulta_nombre, env=datos)
    return [consulta_nombre,resultado]

def consultaFinal(datos):
    nombre = cansultaNombre(datos)[0]
    consulta_final = '''
            SELECT DISTINCT n.Pais, r.Sede
            FROM ({nombre}) AS n
            INNER JOIN redes_sociales AS r
            ON n.Codigo = r.País
            ORDER BY n.Pais ASC
    '''
    resultado = psql.sqldf(consulta_final, env=datos)
    return [consulta_final,resultado]

    
print(consultaFinal(datos)[1])
  

