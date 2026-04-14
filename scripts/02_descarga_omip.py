"""
Script 02 - Descarga de futuros OMIP
Mercado ibérico de electricidad - MIBEL Futuros
Descarga todos los productos: semana, fin de semana,
mes, trimestre, año (Cal)
"""

import requests
import pandas as pd
from bs4 import BeautifulSoup
import os
import datetime as dt
import time

# ─────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────

OUTPUT_PATH = "data/omip_futuros.csv"

# URL base de OMIP para datos de mercado
OMIP_BASE_URL = "https://www.omip.pt/en/dados-mercado"

# Headers para simular un navegador
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9,es;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Tipos de producto OMIP
PRODUCT_TYPES = {
    "BL": "Base Load",      # Carga base (todos los días)
    "PL": "Peak Load",      # Horas pico
}

# Horizontes de tiempo que queremos capturar
HORIZONS = ["Week", "Weekend", "Month", "Quarter", "Year"]

# ─────────────────────────────────────────────
# DESCARGA VÍA ARCHIVO PÚBLICO OMIP
# ─────────────────────────────────────────────

def descargar_omip_csv_publico():
    """
    OMIP publica diariamente un archivo CSV/Excel con
    los precios de cierre de todos los contratos.
    Esta función descarga el archivo del día actual.
    """
    hoy = dt.datetime.today()

    # OMIP publica los datos del día anterior en días hábiles
    # Formato de URL de OMIP para datos históricos
    # https://www.omip.pt/en/file-access-list

    registros = []

    # Intentar los últimos 5 días (por si hay festivos)
    for dias_atras in range(0, 6):
        fecha = hoy - dt.timedelta(days=dias_atras)

        # OMIP no publica fines de semana
        if fecha.weekday() >= 5:  # 5=Sábado, 6=Domingo
            continue

        fecha_str = fecha.strftime("%Y%m%d")
        fecha_display = fecha.strftime("%Y-%m-%d")

        # URL del archivo de datos de OMIP
        url = f"https://www.omip.pt/sites/default/files/dados_mercado/{fecha_str}_EL.csv"

        try:
            print(f"⬇️  Intentando descargar OMIP: {fecha_display}")
            response = requests.get(url, headers=HEADERS, timeout=30)

            if response.status_code == 200:
                # Parsear el CSV de OMIP
                from io import StringIO
                df = pd.read_csv(StringIO(response.text), sep=";", decimal=",")
                df["FECHA_DESCARGA"] = fecha_display
                registros.append(df)
                print(f"✅ Descargado correctamente: {fecha_display}")
                time.sleep(1)  # Respetar el servidor
            else:
                print(f"⚠️  No disponible para {fecha_display} (HTTP {response.status_code})")

        except Exception as e:
            print(f"❌ Error descargando {fecha_display}: {e}")

    if registros:
        return pd.concat(registros, ignore_index=True)
    else:
        return None


def descargar_omip_historico():
    """
    Descarga el histórico completo de OMIP desde su
    sección de file access (archivos públicos).
    Se usa solo en la primera ejecución.
    """
    print("📥 Iniciando descarga histórica de OMIP desde 2019...")

    todos_los_datos = []
    fecha_inicio = dt.datetime(2019, 1, 2)  # OMIP empieza el 2 de enero
    fecha_fin = dt.datetime.today()

    fecha_actual = fecha_inicio

    while fecha_actual <= fecha_fin:
        # Solo días hábiles
        if fecha_actual.weekday() < 5:
            fecha_str = fecha_actual.strftime("%Y%m%d")
            fecha_display = fecha_actual.strftime("%Y-%m-%d")
            url = f"https://www.omip.pt/sites/default/files/dados_mercado/{fecha_str}_EL.csv"

            try:
                response = requests.get(url, headers=HEADERS, timeout=15)
                if response.status_code == 200:
                    from io import StringIO
                    df = pd.read_csv(StringIO(response.text), sep=";", decimal=",")
                    df["TRADE_DATE"] = fecha_display
                    todos_los_datos.append(df)

                    if len(todos_los_datos) % 50 == 0:
                        print(f"  ✅ {len(todos_los_datos)} días descargados... último: {fecha_display}")

                time.sleep(0.5)  # Respetar el servidor

            except Exception as e:
                print(f"  ⚠️  Error en {fecha_display}: {e}")

        fecha_actual += dt.timedelta(days=1)

    if todos_los_datos:
        return pd.concat(todos_los_datos, ignore_index=True)
    else:
        return None


def limpiar_futuros(df):
    """
    Limpia y estandariza el DataFrame de futuros OMIP.
    Extrae: tipo de producto, horizonte, precio de cierre,
    volumen, open interest.
    """
    if df is None or df.empty:
        return None

    # Renombrar columnas comunes de OMIP
    # (los nombres pueden variar según versión del CSV)
    column_mapping = {
        "Contract": "CONTRATO",
        "Close": "PRECIO_CIERRE",
        "Settlement": "PRECIO_LIQUIDACION",
        "Volume": "VOLUMEN_MWH",
        "Open Interest": "OPEN_INTEREST",
        "High": "PRECIO_MAXIMO",
        "Low": "PRECIO_MINIMO",
        "Open": "PRECIO_APERTURA",
    }

    df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})

    # Clasificar tipo de producto
    def clasificar_producto(contrato):
        if pd.isna(contrato):
            return "UNKNOWN"
        contrato = str(contrato).upper()
        if "CAL" in contrato or "YR" in contrato or "AN" in contrato:
            return "ANUAL"
        elif "Q" in contrato or "QUARTER" in contrato or "TR" in contrato:
            return "TRIMESTRAL"
        elif "M" in contrato or "MON" in contrato or "MES" in contrato:
            return "MENSUAL"
        elif "WK" in contrato or "WEEK" in contrato or "SEM" in contrato:
            return "SEMANAL"
        elif "WE" in contrato or "WEEKEND" in contrato or "FDS" in contrato:
            return "FIN_DE_SEMANA"
        else:
            return "OTRO"

    if "CONTRATO" in df.columns:
        df["TIPO_PRODUCTO"] = df["CONTRATO"].apply(clasificar_producto)

    # Asegurarse de que precios son numéricos
    for col in ["PRECIO_CIERRE", "PRECIO_LIQUIDACION", "PRECIO_MAXIMO",
                "PRECIO_MINIMO", "PRECIO_APERTURA"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def guardar_futuros(df_nuevo):
    """
    Une los nuevos datos con el histórico y guarda el CSV.
    """
    os.makedirs("data", exist_ok=True)

    if df_nuevo is None:
        print("❌ No hay datos nuevos para guardar.")
        return

    if os.path.exists(OUTPUT_PATH):
        df_existente = pd.read_csv(OUTPUT_PATH)
        df_final = pd.concat([df_existente, df_nuevo], ignore_index=True)

        # Eliminar duplicados
        if "TRADE_DATE" in df_final.columns and "CONTRATO" in df_final.columns:
            df_final = df_final.drop_duplicates(
                subset=["TRADE_DATE", "CONTRATO"], keep="last"
            )
    else:
        df_final = df_nuevo

    df_final.to_csv(OUTPUT_PATH, index=False)
    print(f"💾 Futuros guardados: {OUTPUT_PATH}")
    print(f"   Total filas: {len(df_final):,}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("OMIP - Descarga de Futuros de Electricidad")
    print("=" * 60)

    if os.path.exists(OUTPUT_PATH):
        # Actualización diaria
        print("🔄 Modo actualización diaria...")
        df_raw = descargar_omip_csv_publico()
    else:
        # Primera vez: descarga histórica completa
        print("🏗️  Primera ejecución: descarga histórica completa (puede tardar 15-30 min)")
        df_raw = descargar_omip_historico()

    # Limpiar
    df_limpio = limpiar_futuros(df_raw)

    if df_limpio is not None:
        print(f"\n📊 Muestra de datos:")
        print(df_limpio.head())
        print(f"\nTipos de producto encontrados:")
        if "TIPO_PRODUCTO" in df_limpio.columns:
            print(df_limpio["TIPO_PRODUCTO"].value_counts())

    # Guardar
    guardar_futuros(df_limpio)

    print("\n✅ Script 02 completado.")
