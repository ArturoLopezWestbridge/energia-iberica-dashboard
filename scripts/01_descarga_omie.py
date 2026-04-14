"""
Script 01 - Descarga de precios spot OMIE
Mercado ibérico de electricidad - España y Portugal
Descarga datos históricos desde 2019 hasta hoy
y actualiza el CSV en la carpeta data/
"""

import datetime as dt
import pandas as pd
import os
from OMIEData.DataImport.omie_marginalprice_importer import OMIEMarginalPriceFileImporter
from OMIEData.Enums.all_enums import DataTypeInMarginalPriceFile

# ─────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────

OUTPUT_PATH = "data/omie_spot.csv"
FECHA_INICIO_HISTORICO = dt.datetime(2019, 1, 1)

# ─────────────────────────────────────────────
# LÓGICA DE DESCARGA
# ─────────────────────────────────────────────

def obtener_fecha_inicio():
    """
    Si ya existe el CSV, descarga solo desde el último dato.
    Si no existe, descarga todo desde 2019.
    """
    if os.path.exists(OUTPUT_PATH):
        df_existente = pd.read_csv(OUTPUT_PATH, parse_dates=["DATE"])
        ultima_fecha = df_existente["DATE"].max()
        # Empezamos desde el día siguiente al último dato
        fecha_inicio = ultima_fecha + dt.timedelta(days=1)
        print(f"✅ CSV existente encontrado. Descargando desde: {fecha_inicio.date()}")
        return df_existente, fecha_inicio
    else:
        print(f"📥 No existe CSV. Descarga histórica desde: {FECHA_INICIO_HISTORICO.date()}")
        return None, FECHA_INICIO_HISTORICO


def descargar_spot(fecha_inicio, fecha_fin):
    """
    Descarga precios marginales horarios de OMIE
    para España y Portugal.
    """
    if fecha_inicio >= fecha_fin:
        print("ℹ️  Ya tienes los datos al día. Nada que descargar.")
        return None

    print(f"⬇️  Descargando OMIE spot: {fecha_inicio.date()} → {fecha_fin.date()}")

    df = OMIEMarginalPriceFileImporter(
        date_ini=fecha_inicio,
        date_end=fecha_fin
    ).read_to_dataframe(verbose=True)

    return df


def limpiar_y_transformar(df):
    """
    Transforma el formato wide (H1..H24) a formato long:
    DATE | HOUR | PRICE_SP | PRICE_PT
    """
    # Separar precios España y Portugal
    df_sp = df[df["CONCEPT"] == "PRICE_SP"].copy()
    df_pt = df[df["CONCEPT"] == "PRICE_PT"].copy()

    # Columnas de horas
    hour_cols = [f"H{i}" for i in range(1, 25)]

    # Pasar a formato long
    df_sp_long = df_sp.melt(id_vars=["DATE"], value_vars=hour_cols,
                             var_name="HOUR", value_name="PRICE_SP")
    df_pt_long = df_pt.melt(id_vars=["DATE"], value_vars=hour_cols,
                             var_name="HOUR", value_name="PRICE_PT")

    # Unir España y Portugal
    df_final = pd.merge(df_sp_long, df_pt_long, on=["DATE", "HOUR"])

    # Limpiar columna HOUR: "H1" → 1
    df_final["HOUR"] = df_final["HOUR"].str.replace("H", "").astype(int)

    # Crear columna datetime completa
    df_final["DATETIME"] = pd.to_datetime(df_final["DATE"]) + \
                           pd.to_timedelta(df_final["HOUR"] - 1, unit="h")

    # Ordenar columnas
    df_final = df_final[["DATETIME", "DATE", "HOUR", "PRICE_SP", "PRICE_PT"]]
    df_final = df_final.sort_values(["DATE", "HOUR"]).reset_index(drop=True)

    # Redondear precios a 2 decimales
    df_final["PRICE_SP"] = df_final["PRICE_SP"].round(2)
    df_final["PRICE_PT"] = df_final["PRICE_PT"].round(2)

    return df_final


def guardar_csv(df_nuevo, df_existente):
    """
    Une datos nuevos con histórico existente y guarda el CSV.
    """
    os.makedirs("data", exist_ok=True)

    if df_existente is not None and df_nuevo is not None:
        df_final = pd.concat([df_existente, df_nuevo], ignore_index=True)
        df_final = df_final.drop_duplicates(subset=["DATETIME"]).sort_values("DATETIME")
    elif df_nuevo is not None:
        df_final = df_nuevo
    else:
        print("ℹ️  Sin datos nuevos que guardar.")
        return

    df_final.to_csv(OUTPUT_PATH, index=False)
    print(f"💾 CSV guardado: {OUTPUT_PATH}")
    print(f"   Total filas: {len(df_final):,}")
    print(f"   Período: {df_final['DATE'].min()} → {df_final['DATE'].max()}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

if __name__ == "__main__":
    fecha_fin = dt.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)

    # 1. Ver qué datos ya tenemos
    df_existente, fecha_inicio = obtener_fecha_inicio()

    # 2. Descargar datos nuevos
    df_raw = descargar_spot(fecha_inicio, fecha_fin)

    # 3. Limpiar y transformar
    if df_raw is not None:
        df_nuevo = limpiar_y_transformar(df_raw)
        print(f"\n📊 Muestra de datos descargados:")
        print(df_nuevo.head())
    else:
        df_nuevo = None

    # 4. Guardar
    guardar_csv(df_nuevo, df_existente)

    print("\n✅ Script 01 completado.")
