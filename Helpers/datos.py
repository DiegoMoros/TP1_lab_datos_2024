import pandas as pd
PATH_SEDES  = r"Datos\sedes.csv"
PATH_SEDES_MIN = r"Datos\sedes_min.csv"
PATH_SECCIONES = r"Datos\secciones.csv"
PATH_MIGRACIONES = r"Datos\migraciones.csv"
PATH_REGIONES = r"Datos\codigo_region.csv"
datos = {
    "migraciones" : pd.read_csv(PATH_MIGRACIONES),
    "sedes_basico" : pd.read_csv(PATH_SEDES_MIN),
    "sedes_completo" : pd.read_csv(PATH_SEDES),
    "secciones" : pd.read_csv(PATH_SECCIONES),
    "regiones" : pd.read_csv(PATH_REGIONES)
}

def get_datos():
    """Retorna la lista de datos"""
    return datos