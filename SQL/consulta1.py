
import pandasql as psql

#ejercicio 1.
"""
    Para cada país informar cantidad de sedes, cantidad de secciones en
    promedio que poseen sus sedes y el flujo migratorio neto (inmigración
    emigración) entre el país y el resto del mundo en el año 2000. El orden del
    reporte debe respetar la cantidad de sedes (de manera descendente). En
    caso de empate, ordenar alfabéticamente por nombre de país. A modo de
    ejemplo, el resultado podría ser:

    datos: 
        tabla migraciones: columnas que tiene la tabla
            Country Origin Name,Country Origin Code,Migration by Gender Name,Migration by Gender Code,Country Dest Name,Country Dest Code,1960 [1960],1970 [1970],1980 [1980],1990 [1990],2000 [2000]
            example:
            Afghanistan,AFG,Female,FEM,Afghanistan,AFG,0,0,0,0,0
            comentarios adiconales: El genero cuanta con 2 tipos total, masculino, femenino donde el total hace la sumatoria entre masculinos y femeninos
        tabla sedes: columnas que tiene la tabla
            sede_id,sede_desc_castellano,sede_desc_ingles,pais_castellano,pais_ingles,region_geografica,pais_iso_2,pais_iso_3,pais_codigo_telefonico,ciudad_castellano,ciudad_ingles,ciudad_zona_horaria_gmt,ciudad_codigo_telefonico,estado,titular_nombre,titular_apellido,titular_cargo,direccion,codigo_postal,telefono_principal,telefonos_adicionales,celular_guardia,celulares_adicionales,fax_principal,faxes_adicionales,correo_electronico,correos_electronicos_adicionales,sitio_web,sitios_web_adicionales,redes_sociales,atencion_dia_desde,atencion_dia_hasta,atencion_hora_desde,atencion_hora_hasta,atencion_comentario,concurrencias,circunscripcion
            example:
            CALEG,Consulado  General  en  Porto  Alegre,Consulate  General  in  PORTO  ALEGRE,REPÚBLICA  FEDERATIVA  DEL  BRASIL,FEDERATIVE  REPUBLIC  OF  BRAZIL,AMÉRICA  DEL  SUR,BR,BRA,55,Porto  Alegre,Porto  Alegre,-3,51,Activo,  Jorge  Enrique  ,Perren,Cónsul  General,"Rua  Cel.  Bordini  1033, Bairro  Moinhos  de  Vento",90440-001,00  55  (51)  3321  1360,,00  55  (51)  99959  0061  ,,00  55  (51)  3321  1360,,caleg@cancilleria.gov.ar,,caleg.cancilleria.gob.ar,,https://twitter.com/ArgPortoAlegre  //  https://www.facebook.com/ArgentinaEnPortoAlegre/  //  https://www.instagram.com/argenportoalegre/  //  ,,,00:00:00,00:00:00,9  a  18  hs;  Atención  al  público  9  a  15  hs,,Aceguá;  Água  Santa;  Agudo;  Alto  Alegre;  Alto  Feliz;  Alvorada;  Amaral  Ferrador;  André  da  Rocha;  Anta  Gorda;  Antônio  Prado;  Arambaré;  Araricá;  Aratiba;  Arroio  do  Meio;  Arroio  do  Padre;  Arroio  do  Sal;  Arroio  do  Tigre;  Arroio  dos  Ratos;  Arroio  Grande;    Arvorezinha;  Áurea;  Balneário  Pinhal;  Barão;  Barão  de  Cotegipe;  Barão  do  Triunfo;  Barra  do  Ribeiro;  Barra  do  Rio  Azul;  Barracão;  Barros  Cassal;  Benjamin  Constant  do  Sul;  Bento  Gonçalves;  Boa  Vista  do  Sul;  Bom  Jesus;  Bom  Princípio;  Bom  Retiro  do  Sul;  Boqueirão  do  Leão;  Brochier;  Butiá;  Caçapava  do  Sul;  Cachoeira  do  Sul;  Cachoeirinha;  Cacique  Doble;    Camaquã;  Camargo;  Cambará  do  Sul;  Campestre  da  Serra;  Campinas  do  Sul;  Campo  Bom;  Candiota;  Canela;  Canguçu;  Canoas;  Canudos  do  Vale;  Capão  Bonito  do  Sul;  Capão  da  Canoa;  Capão  do  Leão;  Capela  de  Santana;  Capitão;  Capivari  do  Sul;  Caraá;  Carlos  Barbosa;  Carlos  Gomes;  Casca;  Caseiros;  Caxias  do  Sul;  Centenário;  Cerrito;  Cerro  Branco;  Cerro  Grande  do  Sul;  Charqueadas;  Charrua;  Chuí;  Chuvisca;  Cidreira;  Ciríaco;  Colinas;  Colorado;  Coqueiro  Baixo;  Coronel  Pilar;  Cotiporã;  Coxilha;  Cristal;  Cruzaltense;    Cruzeiro  do  Sul;  David  Canabarro;  Dois  Irmãos;  Dois  Lajeados;  Dom  Feliciano;  Dom  Pedro  de  Alcântara;  Dona  Francisca;Doutor  Ricardo;  Eldorado  do  Sul;  Encantado;  Encruzilhada  do  Sul;  Entre  Rios  do  Sul;  Erebango;  Erechim;  Ernestina;  Erval  Grande;  Esmeralda;    Estação;  Estância  Velha;  Esteio;  Estrela;  Estrela  Velha;  Fagundes  Varela;  Farroupilha;  Faxinal  do  Soturno;  Faxinalzinho;  Fazenda  Vilanova;  Feliz;  Flores  da  Cunha;  Floriano  Peixoto;  Fontoura  Xavier;  Formigueiro;  Forquetinha;  Garibaldi;  Gaurama;  General  Câmara;  Gentil;  Getúlio  Vargas;  Glorinha;  Gramado;  Gramado  Xavier;  Gravataí;  Guabiju;  Guaíba;  Guaporé;  Harmonia;  Herval;  Herveiras;  Ibarama;  Ibiaçá;  Ibiraiaras;  Ibirapuitã;  Igrejinha;  Ilópolis;  Imbé;  Imigrante;  Inhacorá;  Ipê;  Ipiranga  do  Sul;  Itaara;  Itapuca;  Itati;  Itatiba  do  Sul;  Ivorá;  Ivoti;  Jacutinga;  Jaguarão;    Jaquirana;  Lagoa  Bonita  do  Sul;  Lagoa  dos  Três  Cantos;  Lagoa  Vermelha;  Lagoão;    Lajeado;  Lavras  do  Sul;  Lindolfo  Collor;  Linha  Nova;  Machadinho;  Mampituba;  Maquiné;  Maratá;    Marau;  Marcelino  Ramos;  Mariana  Pimentel;  Mariano  Moro;  Marques  de  Souza;  Mato  Castelhano;  Mato  Leitão;  Maximiliano  de  Almeida;  Minas  do  Leão;  Montauri;  Monte  Alegre  dos  Campos;  Monte  Belo  do  Sul;  Montenegro;  Mormaço;  Morrinhos  do  Sul;  Morro  Redondo;  Morro  Reuter;  Mostardas;  Muçum;  Muitos  Capões;  Muliterno;  Não-Me-Toque;  Nicolau  Vergueiro;  Nova  Alvorada;  Nova  Araçá;  Nova  Bassano;  Nova  Bréscia;  Nova  Hartz;  Nova  Pádua;  Nova  Palma;  Nova  Petrópolis;  Nova  Prata;  Nova  Roma  do  Sul;  Nova  Santa  Rita;  Novo  Cabrais;  Novo  Hamburgo;  Osório;  Paim  Filho;  Palmares  do  Sul;  Pantano  Grande;  Paraí;  Paraíso  do  Sul;  Pareci  Novo;  Parobé;  Passa  Sete;  Passo  do  Sobrado;  Passo  Fundo;  Paulo  Bento;  Paverama;  Pedras  Altas;  Pedro  Osório;  Pelotas;  Picada  Café;  Pinhal  da  Serra;  Piratini;  Poço  das  Antas;  Pontão;  Ponte  Preta;  Portão;  Porto  Alegre  (capital  estatal);  Pouso  Novo;  Presidente  Lucena;  Protásio  Alves;  Putinga;  Quatro  Irmãos;  Relvado;  Restinga  Seca;  Rio  Grande;  Rio  Pardo;  Riozinho;  Roca  Sales;  Rolante;  Ronda  Alta;  Salvador  do  Sul;  Sananduva;  Santa  Cecília  do  Sul;  Santa  Clara  do  Sul;  Santa  Cruz  do  Sul;  Santa  Margarida  do  Sul;  Santa  Maria;  Santa  Maria  do  Herval;  Santa  Tereza;  Santa  Vitória  do  Palmar;  Santana  da  Boa  Vista;  Santo  Antônio  da  Patrulha;  Santo  Antônio  do  Palma;  Santo  Expedito  do  Sul;  São  Domingos  do  Sul;  São  Francisco  de  Paula;  São  Gabriel;  São  Jerônimo;  São  João  da  Urtiga;  São  João  do  Polêsine;  São  Jorge;  São  José  do  Herval;  São  José  do  Hortêncio;  São  José  do  Norte;  São  José  do  Ouro;  São  José  do  Sul;  São  José  dos  Ausentes;  São  Leopoldo;  São  Lourenço  do  Sul;  São  Marcos;  São  Pedro  da  Serra;  São  Sepé;  São  Valentim;  São  Valentim  do  Sul;  São  Vendelino;    Sapiranga;  Sapucaia  do  Sul;  Segredo;  Selbach;  Sentinela  do  Sul;  Serafina  Corrêa;  Sério;    Sertão;  Sertão  Santana;  Severiano  de  Almeida;  Silveira  Martins;  Sinimbu;  Sobradinho;    Soledade;  Tabaí;  Tapejara;  Tapera;  Tapes;  Taquari;  Tavares;  Terra  de  Areia;  Teutônia;  Tio  Hugo;  Torres;  Tramandaí;  Travesseiro;  Três  Arroios;  Três  Cachoeiras;  Três  Coroas;  Três  Forquilhas;  Tunas;  Tupanci  do  Sul;  Turuçu;  União  da  Serra;  Vacaria;  Vale  do  Sol;  Vale  Real;  Vale  Verde;  Vanini;  Venâncio  Aires;  Vera  Cruz;  Veranópolis;  Vespasiano  Correa;  Viadutos;    Viamão;  Victor  Graeff;  Vila  Flores;  Vila  Lângaro;  Vila  Maria;  Vila  Nova  do  Sul;  Vista  Alegre  do  Prata;  Westfália;  Xangri-lá. 
            comentarios adiconales: 
        tabla secciones: columnas que tiene la tabla
            sede_id,sede_desc_castellano,sede_desc_ingles,tipo_seccion,nombre_titular,apellido_titular,cargo_titular,telefono_principal,telefonos_adicionales,celular_de_guardia,celulares_adicionales,fax_principal,faxes_adicionales,correo_electronico,correos_adicionales,sitio_web,sitios_web_adicionales,atencion_dia_desde,atencion_dia_hasta,atencion_hora_desde,atencion_hora_hasta,comentario_del_horario,temas
            example:
            ECHES,"Administración","Administrative  Section","Seccion","María  Verónica  ","Skerianz","Jefe  de  Sección  Administrativa",,"",,"",,"","eches@mrecic.gov.ar","",,"",Lunes,Viernes,09:00:00,17:00:00,"", 
    solución:
        de modo que contamos con una gran cantidad de datos que no nos aportan información relevante decimos quedarnos con los siguientes datos de los csv
        tabla migraciones: Columnas objetivo
            Country Origin Name,Country Origin Code,Migration by Gender Code, 2000 [2000]
        tabla sedes:
            sede_id,pais_iso_3
        tabla secciones:
            sede_id,tipo_seccion
    
    ahora quiero hacer una consulta en sql que basicamente me una unificación entre la tabla de sedes y secciones (uniendolo poir sede_id)
    luego quiero hacer una consulta en sql que unifique la tabla nueva de sedes_secciones con la de migraciones de modo que pueda asociar por (pais_iso_3 y Country Origin Code)
    luego quiero hacer una sumatoria de la cantidad total de sedes que hay por pais, y calcular el promedio de secciones por pais
    De forma que me quede: 
        COUNTRY, CODE_CUNTRY SEDE_ID SEDES_COUNT SECCIONES_PROM
    ahora quiero hacer una consultas en sql quye basicmaente haga una reduccion de los datos que tomo de la tabla de migraciones de modo quye me quede la tabla simplificada:
             Country Origin Name,Country Origin Code,Migration by Gender Code, 2000 [2000]
    luego quiero hacer una consuta sql que me calcule el flujo migratoprio de un pais con el resto del mundo y crear una nueva tabla con este formato:
        COUNTRY, CODE_CUNTRY,  FLUJO_MIGRATORIO_NETO
    ahora basta con unificar ambas tablas en una sola:    
        COUNTRY, SEDES_COUNT, SECCIONES_PROM,  FLUJO_MIGRATORIO_NETO
        
"""
#Parte 1: Secciones y sedes

def sedes_secciones(datos):
    """Calcula la cantidad de sedes y el promedio de secciones por país."""
    consulta_sedes_secciones = '''
        SELECT s.pais_iso_3, 
               COUNT(DISTINCT s.sede_id) AS sedes_count, 
               AVG(sec.cantidad_secciones) AS secciones_prom
        FROM sedes_completo AS s
        LEFT JOIN (
            SELECT sede_id, 
                   COUNT(*) AS cantidad_secciones
            FROM secciones
            GROUP BY sede_id
        ) AS sec
        ON s.sede_id = sec.sede_id
        GROUP BY s.pais_iso_3
    '''
    sedes_secciones = psql.sqldf(consulta_sedes_secciones, env=datos)
    return [consulta_sedes_secciones, sedes_secciones]
# print(sedes_secciones(datos)[1])

def sedes_migraciones(datos):
    """Creamos una tabla que unifica las sedes_secciones con los paises"""
    consulta_sedes_secciones = sedes_secciones(datos)[0]
    consulta_migraciones_sedes = '''
        SELECT m."Country Origin Name" AS country, m."Country Origin Code" AS code_country, 
            ss.sedes_count, ss.secciones_prom
        FROM (
            SELECT "Country Origin Name", "Country Origin Code"
            FROM migraciones
            GROUP BY "Country Origin Name", "Country Origin Code"
        ) AS m
        LEFT JOIN ({}) AS ss
        ON m."Country Origin Code" = ss.pais_iso_3
        ORDER BY ss.sedes_count DESC, m."Country Origin Name" ASC
    '''.format(consulta_sedes_secciones)
    resultado = psql.sqldf(consulta_migraciones_sedes, env=datos)
    return [consulta_migraciones_sedes,resultado]
# print(sedes_migraciones(datos)[1])

""" 
    Notas: Ejercicio
    se puede usar al función: AVG(COALESCE(sec.cantidad_secciones, 0)) para calcular el pormedio?
    forma manual: ¿Como?
"""

#Parte 2: FLujo Migratorio

def emigracion():
    consulta = '''
    SELECT "Country Origin Code" AS code_country, 
    SUM("2000 [2000]") AS emigracion
    FROM migraciones
    WHERE "Migration by Gender Code" = 'TOT'
    GROUP BY "Country Origin Code"
    '''
    return consulta
def inmigracion():
    consulta = '''
    SELECT "Country Dest Code" AS code_country, 
    SUM("2000 [2000]") AS inmigracion
    FROM migraciones
    WHERE "Migration by Gender Code" = 'TOT'
    GROUP BY "Country Dest Code"
    '''
    return consulta

def flujo_migratorio(datos):
    """Calcula y devuelve el flujo migratorio neto por país."""
    consulta_flujo_migratorio = f'''
        WITH flujo_emigracion AS (
            {emigracion()}
        ),
        flujo_inmigracion AS (
            {inmigracion()}
        )
        SELECT 
            COALESCE(f_inm.code_country, f_emi.code_country) AS code_country,
            MAX(COALESCE(m."Country Origin Name", m."Country Dest Name", '')) AS country,
            COALESCE(f_inm.inmigracion, 0) - COALESCE(f_emi.emigracion, 0) AS flujo_migratorio_neto
        FROM flujo_inmigracion f_inm
        FULL OUTER JOIN flujo_emigracion f_emi 
            ON f_inm.code_country = f_emi.code_country
        LEFT JOIN migraciones m 
            ON COALESCE(f_inm.code_country, f_emi.code_country) = m."Country Origin Code"
            OR COALESCE(f_inm.code_country, f_emi.code_country) = m."Country Dest Code"
        WHERE m."Migration by Gender Code" = 'TOT'
        GROUP BY COALESCE(f_inm.code_country, f_emi.code_country)
        ORDER BY flujo_migratorio_neto DESC
    '''
    resultado = psql.sqldf(consulta_flujo_migratorio, env=datos)
    return [consulta_flujo_migratorio, resultado]

# print(flujo_migratorio(datos)[1])


# Paso 3: Unificación
def reporte_sedes_migracion(datos):
    """Genera el reporte consolidado con sedes, secciones y flujo migratorio."""
    consulta_sedes_secciones = sedes_secciones(datos)[0]
    consulta_flujo_migratorio = flujo_migratorio(datos)[0]

    consulta_final = f'''
        SELECT 
            COALESCE(ss.pais_iso_3, fmn.code_country) AS country,
            COALESCE(ss.sedes_count, 0) AS sedes_count,
            COALESCE(ss.secciones_prom, 0) AS secciones_prom,
            COALESCE(fmn.flujo_migratorio_neto, 0) AS flujo_migratorio_neto
        FROM ({consulta_sedes_secciones}) AS ss
        FULL OUTER JOIN (
            SELECT code_country, flujo_migratorio_neto
            FROM ({consulta_flujo_migratorio})
        ) AS fmn
        ON ss.pais_iso_3 = fmn.code_country
        ORDER BY ss.sedes_count DESC, country ASC
    '''
    resultado = psql.sqldf(consulta_final, env=datos)
    return [consulta_final, resultado]