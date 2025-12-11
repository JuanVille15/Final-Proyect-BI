import pandas as pd
import numpy as np
import os
pd.set_option('display.max_columns',None)

# Leemos las bases

gest_table = pd.read_parquet(r"C:\Users\jjvt9593\OneDrive - Grupo Coomeva\Escritorio\Python\Power BI\ProyectoFinal\Data\gestiones_202512.parquet", engine='pyarrow')

gest_table.head()

rec_table = pd.read_parquet(r"C:\Users\jjvt9593\OneDrive - Grupo Coomeva\Escritorio\Python\Power BI\ProyectoFinal\Data\recaudos_202512.parquet", engine='pyarrow')

rec_table.head()

homo_table = pd.read_csv(r"C:\Users\jjvt9593\OneDrive - Grupo Coomeva\Escritorio\Python\Power BI\ProyectoFinal\Data\Homologación.csv", sep=";")

# 1. Separamos la columna

gest_table = gest_table[gest_table['ACTOR_GESTION'].isin(['CSS','DIR NAC RECAUDO','PROCOBAS'])]

# leemos el arbol de gestion nuevo

arbol = pd.read_excel(r"\\coomeva.nal\DFSCoomeva\Cartera_Coomeva\CARTERA\9. Arbol de Gestion WSAC\Nuevo Arbol\ArbolModificadoCompleto202510.xlsx", sheet_name='CombinadasContactoSAC')

gestiones_validas = arbol['Accion'].unique().tolist()

# Filtramos la base de gestiones con solo las gestiones validas

gest_table = gest_table[gest_table['ACCION'].isin(gestiones_validas)]

gest_table[['CANAL','TIPO','ACTOR']] = gest_table['ACCION'].str.split(pat="_",n=2, expand=True)

# Vamosa separar la fecha_gestion

gest_table['FECHA'] = pd.to_datetime(gest_table['FECHA_GESTION']).dt.date

gest_table['HORA'] = pd.to_datetime(gest_table['FECHA_GESTION']).dt.hour

gest_table['HORA'] = np.where(gest_table['HORA'].isin([1,2,3,4,5,6,12]), gest_table['HORA'].astype(str) + " pm",
    gest_table['HORA'].astype(str) + " am")

# generamos agrupación para la columna de horas
gest_table.columns

gest_table = gest_table.merge(right=homo_table,
                              how='left',
                              on='RESPUESTA')

# Quitamos nulos
gest_table.dropna(inplace=True)

# Diferencias agente virtual

gest_table['CANAL'] = np.where(gest_table['OBSERVACION_INTERNA'].str.split(" ",n=1).str[0].astype(str) == 'AGENTEVIRTUAL', 
                               'BOT',
                               gest_table['CANAL']) # :d

# columnas necesarias 

gest_table = gest_table[gest_table['TIPO'] == 'OUT']

col = ['ID','FECHA','HORA','CANAL','ACTOR','Contacto Directo','Efectivo']

gest_table = gest_table[col]

len(gest_table)

# Agrupamos y rezamos

tbl_agr = gest_table.groupby(by=['ID','FECHA','HORA','CANAL','ACTOR']).agg(Contacto_Directo = ('Contacto Directo','max'),
                                                                           Efectivo = ('Efectivo','max')).reset_index().sort_values(by=['ID','FECHA'], ascending=True)


niveles = ['7 am', '8 am', '9 am',
           '10 am','11 am','12 pm',
           '1 pm', '2 pm', '3 pm',
           '4 pm', '5 pm', '6pm']

tbl_agr['HORA'] = tbl_agr['HORA'].astype(pd.CategoricalDtype(categories=niveles, ordered=True))

# tabla efectivas

tbl_efectivas = tbl_agr[tbl_agr['Efectivo'] == 1.0]


# tabla recaudos

tbl_rec_minima = rec_table.groupby(by=['ID'])['FECHA_RECAUDO'].min().reset_index()

# nos traemos la fecha minima de recaudo para gestiones

tbl_efectivas = tbl_efectivas.merge(right=tbl_rec_minima,
                                    how='left',
                                    on='ID')

tbl_efectivas['FECHA_RECAUDO'] = tbl_efectivas['FECHA_RECAUDO']

tbl_efectivas['Diff'] = (pd.to_datetime(tbl_efectivas['FECHA_RECAUDO'])- pd.to_datetime(tbl_efectivas['FECHA'])).dt.days

tbl_efectivas = tbl_efectivas[(tbl_efectivas['Diff'] >= 0) & (tbl_efectivas['Diff'] != np.nan)]

tbl_efectivas_final = tbl_efectivas.sort_values(by='Diff', ascending=True).drop_duplicates(subset='ID',keep='first')

tbl_efectivas_final['Recaudada'] = 1

# merge final :D

tbl_gestiones_final = pd.merge(left=tbl_agr, right=tbl_efectivas_final[['ID','FECHA','HORA','CANAL','ACTOR','Recaudada']],
                               how='left',
                               on=['ID','FECHA','HORA','CANAL','ACTOR'])

tbl_gestiones_final['Recaudada'] = tbl_gestiones_final['Recaudada'].fillna(0)

tbl_gestiones_final.to_parquet(r"C:\Users\jjvt9593\OneDrive - Grupo Coomeva\Escritorio\Python\Power BI\ProyectoFinal\Output\FactTable.parquet", 
                               engine='pyarrow',index=False)