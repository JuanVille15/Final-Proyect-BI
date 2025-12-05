# Extract/Extract_files.py
import pandas as pd
import pyodbc
from datetime import date, timedelta
from pathlib import Path
from Settings.Config import CON_ORACLE, CON_BI


# Extracción tabla de gestiones
def ext_gestiones(periodo_inicio: str) -> pd.DataFrame:
    """Extraeremos el periodo solicitado de la tabla gestiones"""

    mes_siguiente = pd.to_datetime(periodo_inicio, dayfirst=True).replace(day=28) + timedelta(days=4)

    ultimo_dia = (mes_siguiente.replace(day=1) - timedelta(days=1)).strftime(format='%d/%m/%Y')

    xtr_gestiones = f"""
        SELECT
            ASO_IDENTIFICACION AS ID,
            GES_FECHA_GESTION AS FECHA_GESTION,
            AUX_USUARIO AS USUARIO_GESTION,
            ACT_ACTOR_GESTION AS ACTOR_GESTION,
            ACC_NOMBRE_ACCION AS ACCION,
            RES_NOMBRE_RESPUESTA AS RESPUESTA,
            HGES_OBSERVACION_INT AS OBSERVACION_INTERNA
        FROM GRC.GCC_HISTORIA_GESTIONES
        WHERE GES_FECHA_GESTION BETWEEN TO_DATE('{periodo_inicio}', 'DD/MM/YYYY')
        AND TO_DATE('{ultimo_dia}', 'DD/MM/YYYY')
        ORDER BY ID, GES_FECHA_GESTION ASC;
    """

    conn = pyodbc.connect(CON_ORACLE)
    base_gestiones = pd.read_sql(xtr_gestiones, conn)
    conn.close()
    return base_gestiones


# Extracción vista demográfica
def ext_demografica(periodo_inicio: str) -> pd.DataFrame:
    """Extraemos el periodo solicitado de la vista demográfica"""

    periodo = pd.to_datetime(periodo_inicio, dayfirst=True).strftime("%Y-%m-%d")

    xtr_demo = f"""
        SELECT *
        FROM BodegaCorporativa.Conocimiento.v_Demografica
        WHERE BodegaCorporativa.$partition.pf_mes(dtmFechaInsercion)
              = BodegaCorporativa.$partition.pf_mes('{periodo}')
    """

    conn = pyodbc.connect(CON_BI)
    v_demografica = pd.read_sql(xtr_demo, conn)
    # Eliminamos columna nombre -- anonimización
    v_demografica.drop("Nombre_Asociado", axis=1, inplace=True)
    conn.close()

    return v_demografica


# Extracción recaudos
def ext_recaudos(periodo_inicio: str) -> pd.DataFrame:
    """Extraemos recaudo total por cédula dentro del periodo"""

    periodo = pd.to_datetime(periodo_inicio, dayfirst=True).strftime("%Y%m")

    xtr_recaudos = f"""
        SELECT 
            ASO_IDENTIFICACION_ASOCIADO AS ID,
            HRCD_FECHA_RECAUDO AS FECHA_RECAUDO,
            SUM(HRCD_VALOR_RECAUDO) AS VALOR_RECAUDO
        FROM GRC.GCC_HISTORIA_RECAUDO
        WHERE HRCD_PERIODO = '{periodo}'
        GROUP BY ASO_IDENTIFICACION_ASOCIADO, HRCD_FECHA_RECAUDO
        ORDER BY ID, HRCD_FECHA_RECAUDO ASC;
    """

    conn = pyodbc.connect(CON_ORACLE)
    rec_df = pd.read_sql(xtr_recaudos, conn)
    conn.close()

    return rec_df


# Exportar archivos a parquet
def to_parquet(gestiones_mes: pd.DataFrame,
               v_demografica: pd.DataFrame,
               rec_df: pd.DataFrame) -> None:

    today = date.today().strftime("%Y%m")

    # Carpeta Data relativa a la raíz del proyecto
    base_dir = Path(__file__).resolve().parent.parent  # .../ProyectoFinal
    data_dir = base_dir / "Data"
    data_dir.mkdir(exist_ok=True)

    archivos = {
        "gestiones": gestiones_mes,
        "demografica": v_demografica,
        "recaudos": rec_df,
    }

    for nombre, df in archivos.items():
        ruta = data_dir / f"{nombre}_{today}.parquet"
        print(f"Exportando {nombre} → {ruta}")
        df.to_parquet(ruta, index=False, engine="pyarrow")
        print(f"{nombre} correctamente exportado")


# Función que instrumentaliza todo
def extr_fuentes(periodo_inicio: str) -> None:
    """Esta función instrumentaliza todas las consultas y sus salidas"""

    print("Iniciando Proceso de extracción", flush=True)

    gestiones_mes = ext_gestiones(periodo_inicio)
    print("Gestiones del mes consultadas", flush=True)

    v_demografica = ext_demografica(periodo_inicio)
    print("Vista Demográfica consultada", flush=True)

    rec_df = ext_recaudos(periodo_inicio)
    print("Recaudos consultados", flush=True)

    to_parquet(gestiones_mes, v_demografica, rec_df)
    print("Archivos correctamente exportados :D", flush=True)