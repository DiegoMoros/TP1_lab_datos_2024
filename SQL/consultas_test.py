import pandasql as psql
from Helpers.datos import get_datos

datos = get_datos()
consulta_test = '''
        SELECT *
        FROM migraciones
        WHERE "Country Origin Name" = 'Argentina';
    '''
datos_test = psql.sqldf(consulta_test,env=datos)