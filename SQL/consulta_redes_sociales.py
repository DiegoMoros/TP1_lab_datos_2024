import pandasql as psql
import pandas as pd


carpeta = "C:/Users/niqui/Documents/GitHub/TP1_lab_datos_2024/Resultados/"
redes =  pd.read_csv(carpeta+"reporte_redes_sociales.csv")

consultaSQL= '''
        SELECT DISTINCT Sede AS Sede_id,"Red Social" AS plataforma,URL AS Url
        FROM redes
'''

redesSociales = psql.sqldf(consultaSQL, locals())

