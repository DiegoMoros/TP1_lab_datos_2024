# -*- coding: utf-8 -*-
"""
Created on Tue Oct 22 19:50:19 2024

@author: niqui
"""

carpeta = "C:\\Users\\niqui\\Documents\\GitHub\\TP1_lab_datos_2024\\Datos\\"
sedeCompleta =  pd.read_csv(carpeta+"sedes.csv")

consultaSede = '''
        SELECT DISTINCT sede_id AS Sede_id, 
        pais_castellano AS "Nombre sede en español", 
        pais_iso_3AS ISO3
        FROM sedeCompleta
'''