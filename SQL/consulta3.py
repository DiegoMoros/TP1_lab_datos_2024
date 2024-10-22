import pandasql as psql
from Helpers.regularize import save_csv
#Ejercicio 3
"""
    Para saber cuál es la vía de comunicación de las sedes en cada país, nos
    hacemos la siguiente pregunta: ¿Cuán variado es, en cada el país, el tipo de
    redes sociales que utilizan las sedes? Se espera como respuesta que para
    cada país se informe la cantidad de tipos de redes distintas utilizadas. Por
    ejemplo, si en Chile utilizan 4 redes de facebook, 5 de instagram y 4 de
    twitter, el valor para Chile debería ser 3 (facebook, instagram y twitter).
"""
def reporte_variacion_redes(datos,save_df = False):
    """Reporte sobre la variedad de medios de comunicación usados"""
    ejercicio4 = datos
    consulta_variacion_redes = """
        SELECT DISTINCT País, COUNT(DISTINCT "Red Social") AS 'Cantidad de Redes'
        FROM ejercicio4
        GROUP BY País
        ORDER BY País
    """
    variacion_redes_sociales = psql.sqldf(consulta_variacion_redes, locals())
    if save_df:
        save_csv(variacion_redes_sociales,"reporte_variacion_redes")
    return [reporte_variacion_redes ,variacion_redes_sociales]