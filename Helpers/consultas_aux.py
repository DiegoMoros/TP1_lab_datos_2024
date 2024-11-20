import pandas as pd
import pandasql as psql
from Helpers.regularize import save_csv

def consulta_proceso_migratorio(datos,save_df = False):
    consultaUnion = '''
        SELECT 
        `Country Origin Code` AS ISO_pais_origen, 
        `Country Dest Code` AS ISO_pais_destino, 
        `1960 [1960]`, 
        `1970 [1970]`, 
        `1980 [1980]`, 
        `1990 [1990]`, 
        `2000 [2000]`
        FROM migraciones
        WHERE `Migration by Gender Code` = "TOT";
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

    union_result = psql.sqldf(consultaUnion,env=datos)
    if save_df:
        save_csv(union_result,"sede_diplomatica")
    return [consultaUnion, union_result]

def consulta_redes(save_df = False):
    redes = pd.read_csv("Resultados/reporte_redes_sociales.csv")

    consultaSQL= '''
            SELECT DISTINCT Sede,"Red Social",URL
            FROM redes
    '''
    redesSociales = psql.sqldf(consultaSQL, locals())
    if save_df:
        save_csv(redesSociales,"redes_y_sedes")
    return [consultaSQL,redesSociales]
