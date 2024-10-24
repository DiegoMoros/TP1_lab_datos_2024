import pandas as pd
import pandasql as psql
from Helpers.regularize import save_csv

def consulta_proceso_migratorio(datos,save_df = False):
    consultaUnion = '''
        SELECT 
        `Country Origin Code` AS ISO_pais_origen, 
        `Migration by Gender Code` AS Gender_Code, 
        `Country Dest Code` AS ISO_pais_destino, 
        `1960 [1960]`, 
        `1970 [1970]`, 
        `1980 [1980]`, 
        `1990 [1990]`, 
        `2000 [2000]`
        FROM migraciones;
    '''
    union_resultado = psql.sqldf(consultaUnion,env=datos)
    if save_df:
        save_csv(union_resultado,"proceso_migratorio")
    return [consultaUnion,union_resultado]

def consulta_sede_diplomatica(datos,save_df=False):
    consultaUnion = '''
        SELECT s.sede_id, s.sede_desc_castellano, s.pais_iso_3 AS ISO3, COALESCE(sec.repeticiones, 0) AS cantidad_de_secciones
        FROM sedes_completo AS s
        LEFT JOIN (
            SELECT DISTINCT m.sede_id, COUNT(*) AS repeticiones
            FROM secciones AS m
            GROUP BY m.sede_id
        ) AS sec
        ON s.sede_id = sec.sede_id;
    '''

    union_result = psql.sqldf(consultaUnion)
    if save_df:
        save_csv(union_result,"sede_diplomatica")
    return [consultaUnion, union_result]

