"""
Script 01 - Descarga de precios spot OMIE (v2)
Mercado ibérico de electricidad - España y Portugal

Genera 3 CSVs:
- omie_spot_15min.csv  : datos cada 15 min (desde oct 2025)
- omie_spot_horario.csv: datos cada hora (2019→hoy)
- omie_spot_diario.csv : promedio diario (2019→hoy)

Maneja el cambio de formato de OMIE:
- Antes oct 2025: 24 precios horarios
- Desde oct 2025: 96 precios cada 15 minutos
"""

import requests
import pandas as pd
import numpy as np
import os
import datetime as dt
import time
from io import StringIO

# ─────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────

OUTPUT_15MIN   = "data/omie_spot_15min.csv"
OUTPUT_HORARIO = "data/omie_spot_horario.csv"
OUTPUT_DIARIO  = "data/omie_spot_diario.csv"

FECHA_INICIO_HISTORICO = dt.datetime(2019, 1, 1)
FECHA_CAMBIO_15MIN     = dt.datetime(2025, 10, 1)  # Desde aquí OMIE usa 96 periodos

HEADERS_WEB = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
}

# ─────────────────────────────────────────────
# DESCARGA DESDE OMIE
# ─────────────────────────────────────────────

def construir_url_omie(fecha):
    """Construye la URL del archivo diario de OMIE."""
    anio = fecha.strftime("%Y")
    mes  = fecha.strftime("%m")
    dia  = fecha.strftime("%d")
    nombre = f"INT_PBC_EV_H_{dia}_{mes}_{anio}_{dia}_{mes}_{anio}.TXT"
    return f"https://www.omie.es/sites/default/files/dados/AGNO_{anio}/MES_{mes}/TXT/{nombre}"


def descargar_dia_omie(fecha):
    """
    Descarga el archivo TXT de OMIE para un día.
    Retorna DataFrame crudo o None si no hay datos.
    """
    url = construir_url_omie(fecha)
    try:
        r = requests.get(url, headers=HEADERS_WEB, timeout=15)
        if r.status_code == 200 and len(r.content) > 100:
            return r.text
        return None
    except Exception:
        return None


def parsear_archivo_omie(texto, fecha):
    """
    Parsea el archivo TXT de OMIE.
    Maneja ambos formatos: 24 horas y 96 periodos de 15 min.
    Retorna DataFrame con columnas: DATETIME, DATE, PERIOD, PRICE_SP, PRICE_PT
    """
    lineas = texto.strip().split('\n')

    # Detectar separador
    sep = ';' if ';' in lineas[0] else ','

    registros = []

    for linea in lineas:
        linea = linea.strip()
        if not linea or linea.startswith('*'):
            continue

        partes = [p.strip().replace(',', '.') for p in linea.split(sep)]

        if len(partes) < 4:
            continue

        # Formato OMIE: ANIO;MES;DIA;PERIODO;PRECIO_ES;PRECIO_PT
        try:
            anio    = int(partes[0])
            mes     = int(partes[1])
            dia     = int(partes[2])
            periodo = int(partes[3])

            if anio < 2000 or anio > 2030:
                continue
            if periodo < 1:
                continue

            precio_sp = float(partes[4]) if partes[4] else None
            precio_pt = float(partes[5]) if len(partes) > 5 and partes[5] else None

            registros.append({
                'anio': anio, 'mes': mes, 'dia': dia,
                'periodo': periodo,
                'PRICE_SP': precio_sp,
                'PRICE_PT': precio_pt
            })

        except (ValueError, IndexError):
            continue

    if not registros:
        return None

    df = pd.DataFrame(registros)
    df['DATE'] = pd.to_datetime(df[['anio', 'mes', 'dia']].rename(
        columns={'anio': 'year', 'mes': 'month', 'dia': 'day'}))

    # Determinar si es formato horario (24) o cuartohorario (96)
    max_periodo = df['periodo'].max()
    es_15min = max_periodo > 24

    if es_15min:
        # 96 periodos → intervalos de 15 min
        df['DATETIME'] = df['DATE'] + pd.to_timedelta((df['periodo'] - 1) * 15, unit='min')
        df['TIPO'] = '15min'
    else:
        # 24 periodos → horario
        df['DATETIME'] = df['DATE'] + pd.to_timedelta(df['periodo'] - 1, unit='h')
        df['TIPO'] = 'horario'

    # Limpiar y ordenar
    df = df[['DATETIME', 'DATE', 'periodo', 'PRICE_SP', 'PRICE_PT', 'TIPO']].copy()
    df = df.rename(columns={'periodo': 'PERIOD'})
    df = df.sort_values('DATETIME').reset_index(drop=True)

    return df


# ─────────────────────────────────────────────
# GESTIÓN DE CSVs
# ─────────────────────────────────────────────

def obtener_ultima_fecha(path):
    """Retorna la última fecha en el CSV o None si no existe."""
    if not os.path.exists(path):
        return None
    df = pd.read_csv(path, usecols=['DATE'], parse_dates=['DATE'])
    if df.empty:
        return None
    return df['DATE'].max().to_pydatetime()


def agregar_a_csv(df_nuevo, path):
    """Añade datos nuevos al CSV existente, eliminando duplicados."""
    os.makedirs("data", exist_ok=True)

    if os.path.exists(path):
        df_existente = pd.read_csv(path, parse_dates=['DATE', 'DATETIME'] 
                                   if 'DATETIME' in pd.read_csv(path, nrows=0).columns 
                                   else ['DATE'])
        df_final = pd.concat([df_existente, df_nuevo], ignore_index=True)
        key = 'DATETIME' if 'DATETIME' in df_final.columns else 'DATE'
        df_final = df_final.drop_duplicates(subset=[key]).sort_values(key)
    else:
        df_final = df_nuevo

    df_final.to_csv(path, index=False)
    return len(df_final)


# ─────────────────────────────────────────────
# CONSTRUCCIÓN DE LOS 3 CSVs
# ─────────────────────────────────────────────

def construir_horario_desde_15min(df_15min):
    """Convierte datos de 15 min a horarios promediando cada 4 periodos."""
    df = df_15min.copy()
    df['DATE'] = pd.to_datetime(df['DATE'])
    df['DATETIME'] = pd.to_datetime(df['DATETIME'])
    df['HORA'] = (df['PERIOD'] - 1) // 4  # 0-23
    df['DATETIME_HORA'] = df['DATE'] + pd.to_timedelta(df['HORA'], unit='h')

    df_horario = df.groupby(['DATE', 'DATETIME_HORA', 'HORA']).agg(
        PRICE_SP=('PRICE_SP', 'mean'),
        PRICE_PT=('PRICE_PT', 'mean')
    ).reset_index()

    df_horario = df_horario.rename(columns={'DATETIME_HORA': 'DATETIME', 'HORA': 'HOUR'})
    df_horario['HOUR'] = df_horario['HOUR'] + 1  # 1-24
    df_horario['PRICE_SP'] = df_horario['PRICE_SP'].round(2)
    df_horario['PRICE_PT'] = df_horario['PRICE_PT'].round(2)

    return df_horario[['DATETIME', 'DATE', 'HOUR', 'PRICE_SP', 'PRICE_PT']]


def construir_diario(df_horario):
    """Promedia datos horarios a diarios."""
    df = df_horario.copy()
    df['DATE'] = pd.to_datetime(df['DATE'])

    df_diario = df.groupby('DATE').agg(
        PRICE_SP=('PRICE_SP', 'mean'),
        PRICE_PT=('PRICE_PT', 'mean')
    ).reset_index()

    df_diario['PRICE_SP'] = df_diario['PRICE_SP'].round(2)
    df_diario['PRICE_PT'] = df_diario['PRICE_PT'].round(2)
    df_diario['DATE'] = df_diario['DATE'].dt.strftime('%Y-%m-%d')

    return df_diario


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("OMIE Spot - Descarga v2 (horario + 15min + diario)")
    print("=" * 60)

    hoy = dt.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)

    # Ver hasta dónde tenemos datos
    ultima_diario = obtener_ultima_fecha(OUTPUT_DIARIO)
    if ultima_diario:
        fecha_inicio = ultima_diario + dt.timedelta(days=1)
        print(f"✅ Datos existentes hasta: {ultima_diario.date()}")
    else:
        fecha_inicio = FECHA_INICIO_HISTORICO
        print(f"📥 Primera ejecución. Descargando desde: {fecha_inicio.date()}")

    fecha_fin = hoy - dt.timedelta(days=1)

    if fecha_inicio > fecha_fin:
        print("ℹ️  Datos ya al día. Nada que descargar.")
        exit(0)

    print(f"⬇️  Descargando: {fecha_inicio.date()} → {fecha_fin.date()}")

    # Acumuladores
    datos_15min   = []
    datos_horario = []
    dias_ok = 0
    dias_error = 0

    fecha_actual = fecha_inicio
    while fecha_actual <= fecha_fin:

        if fecha_actual.weekday() < 7:  # todos los días (OMIE publica fines de semana)
            texto = descargar_dia_omie(fecha_actual)

            if texto:
                df_dia = parsear_archivo_omie(texto, fecha_actual)

                if df_dia is not None and len(df_dia) > 0:
                    es_15min = df_dia['TIPO'].iloc[0] == '15min'

                    if es_15min:
                        # Guardar 15min
                        df_15 = df_dia[['DATETIME', 'DATE', 'PERIOD', 'PRICE_SP', 'PRICE_PT']].copy()
                        datos_15min.append(df_15)

                        # Construir horario desde 15min
                        df_h = construir_horario_desde_15min(df_dia)
                        datos_horario.append(df_h)
                    else:
                        # Formato horario clásico
                        df_h = df_dia[['DATETIME', 'DATE', 'PERIOD', 'PRICE_SP', 'PRICE_PT']].copy()
                        df_h = df_h.rename(columns={'PERIOD': 'HOUR'})
                        datos_horario.append(df_h)

                    dias_ok += 1
                    if dias_ok % 50 == 0:
                        print(f"  ✅ {dias_ok} días procesados... último: {fecha_actual.date()}")
                else:
                    dias_error += 1
            else:
                dias_error += 1

            time.sleep(0.2)

        fecha_actual += dt.timedelta(days=1)

    print(f"\n📊 Días OK: {dias_ok} | Sin datos: {dias_error}")

    # Guardar CSVs
    os.makedirs("data", exist_ok=True)

    if datos_15min:
        df_15min_total = pd.concat(datos_15min, ignore_index=True)
        df_15min_total['DATE'] = pd.to_datetime(df_15min_total['DATE']).dt.strftime('%Y-%m-%d')
        total = agregar_a_csv(df_15min_total, OUTPUT_15MIN)
        print(f"💾 {OUTPUT_15MIN}: {total:,} filas")

    if datos_horario:
        df_horario_total = pd.concat(datos_horario, ignore_index=True)
        df_horario_total['DATE'] = pd.to_datetime(df_horario_total['DATE']).dt.strftime('%Y-%m-%d')

        # Construir diario
        df_diario_total = construir_diario(df_horario_total)

        df_horario_total['DATETIME'] = pd.to_datetime(df_horario_total['DATETIME']).dt.strftime('%Y-%m-%d %H:%M:%S')
        total_h = agregar_a_csv(df_horario_total, OUTPUT_HORARIO)
        print(f"💾 {OUTPUT_HORARIO}: {total_h:,} filas")

        total_d = agregar_a_csv(df_diario_total, OUTPUT_DIARIO)
        print(f"💾 {OUTPUT_DIARIO}: {total_d:,} filas")

    print("\n✅ Script 01 v2 completado.")
