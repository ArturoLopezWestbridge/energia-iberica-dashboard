"""
Script 01 - OMIE Spot v3
Convierte omie_spot.csv existente en 3 granularidades:
- omie_spot_diario.csv  : promedio diario
- omie_spot_horario.csv : datos horarios limpios
- omie_spot_15min.csv   : datos 15 min (desde oct 2025)
"""

import requests
import pandas as pd
import os
import datetime as dt
import time

INPUT_SPOT     = "data/omie_spot.csv"
OUTPUT_DIARIO  = "data/omie_spot_diario.csv"
OUTPUT_HORARIO = "data/omie_spot_horario.csv"
OUTPUT_15MIN   = "data/omie_spot_15min.csv"

HEADERS_WEB = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
}

def convertir_spot_existente():
    if not os.path.exists(INPUT_SPOT):
        print(f"No encontrado: {INPUT_SPOT}")
        return None

    print(f"Leyendo {INPUT_SPOT}...")
    df = pd.read_csv(INPUT_SPOT, parse_dates=['DATE', 'DATETIME'])
    print(f"Filas: {len(df):,} | {df['DATE'].min().date()} -> {df['DATE'].max().date()}")

    # Corregir precios x100
    if df['PRICE_SP'].mean() > 500:
        print("Corrigiendo precios x100...")
        df['PRICE_SP'] = (df['PRICE_SP'] / 100).round(2)
        df['PRICE_PT'] = (df['PRICE_PT'] / 100).round(2)

    print(f"Precios SP: media={df['PRICE_SP'].mean():.2f} EUR/MWh")

    # CSV Horario
    df_h = df[['DATETIME','DATE','HOUR','PRICE_SP','PRICE_PT']].copy()
    df_h['DATE'] = df_h['DATE'].dt.strftime('%Y-%m-%d')
    df_h['DATETIME'] = df_h['DATETIME'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df_h = df_h.sort_values('DATETIME').drop_duplicates('DATETIME')
    df_h.to_csv(OUTPUT_HORARIO, index=False)
    print(f"Guardado {OUTPUT_HORARIO}: {len(df_h):,} filas")

    # CSV Diario
    df_d = df.groupby('DATE').agg(PRICE_SP=('PRICE_SP','mean'), PRICE_PT=('PRICE_PT','mean')).reset_index()
    df_d['PRICE_SP'] = df_d['PRICE_SP'].round(2)
    df_d['PRICE_PT'] = df_d['PRICE_PT'].round(2)
    df_d['DATE'] = df_d['DATE'].dt.strftime('%Y-%m-%d')
    df_d = df_d.sort_values('DATE')
    df_d.to_csv(OUTPUT_DIARIO, index=False)
    print(f"Guardado {OUTPUT_DIARIO}: {len(df_d):,} filas")

    return df['DATE'].max().date()


def construir_url_omie(fecha):
    d,m,y = fecha.strftime("%d"), fecha.strftime("%m"), fecha.strftime("%Y")
    nombre = f"INT_PBC_EV_H_{d}_{m}_{y}_{d}_{m}_{y}.TXT"
    return f"https://www.omie.es/sites/default/files/dados/AGNO_{y}/MES_{m}/TXT/{nombre}"


def parsear_omie_txt(texto, fecha):
    lineas = texto.strip().split('\n')
    sep = ';' if ';' in lineas[0] else ','
    registros = []
    for linea in lineas:
        linea = linea.strip()
        if not linea or linea.startswith('*'): continue
        partes = [p.strip().replace(',','.') for p in linea.split(sep)]
        if len(partes) < 5: continue
        try:
            anio = int(partes[0])
            if anio < 2000 or anio > 2030: continue
            periodo = int(partes[3])
            sp = float(partes[4]) if partes[4] else None
            pt = float(partes[5]) if len(partes) > 5 and partes[5] else None
            registros.append({'PERIOD': periodo, 'PRICE_SP': sp, 'PRICE_PT': pt})
        except: continue

    if not registros: return None
    df = pd.DataFrame(registros)
    df['DATE'] = fecha.strftime('%Y-%m-%d')
    max_p = df['PERIOD'].max()
    es_15min = max_p > 24

    if es_15min:
        df['DATETIME'] = (pd.to_datetime(fecha) + pd.to_timedelta((df['PERIOD']-1)*15, unit='min')).dt.strftime('%Y-%m-%d %H:%M:%S')
    else:
        df['DATETIME'] = (pd.to_datetime(fecha) + pd.to_timedelta(df['PERIOD']-1, unit='h')).dt.strftime('%Y-%m-%d %H:%M:%S')

    if df['PRICE_SP'].mean() > 500:
        df['PRICE_SP'] = (df['PRICE_SP'] / 100).round(2)
        df['PRICE_PT'] = (df['PRICE_PT'] / 100).round(2)

    df['es_15min'] = es_15min
    return df


def descargar_nuevos_datos(desde_fecha):
    hoy = dt.date.today()
    ayer = hoy - dt.timedelta(days=1)
    if desde_fecha >= ayer:
        print("Datos ya al dia.")
        return

    print(f"Descargando: {desde_fecha} -> {ayer}")
    nuevos_h, nuevos_15 = [], []
    dias_ok = 0

    fecha_actual = desde_fecha + dt.timedelta(days=1)
    while fecha_actual <= ayer:
        try:
            r = requests.get(construir_url_omie(fecha_actual), headers=HEADERS_WEB, timeout=15)
            if r.status_code == 200 and len(r.content) > 100:
                df_dia = parsear_omie_txt(r.text, fecha_actual)
                if df_dia is not None:
                    if df_dia['es_15min'].iloc[0]:
                        nuevos_15.append(df_dia[['DATETIME','DATE','PERIOD','PRICE_SP','PRICE_PT']])
                        df_dia['HORA'] = (df_dia['PERIOD']-1) // 4
                        df_h = df_dia.groupby(['DATE','HORA']).agg(PRICE_SP=('PRICE_SP','mean'),PRICE_PT=('PRICE_PT','mean')).reset_index()
                        df_h['HOUR'] = df_h['HORA']+1
                        df_h['DATETIME'] = (pd.to_datetime(df_h['DATE'])+pd.to_timedelta(df_h['HORA'],unit='h')).dt.strftime('%Y-%m-%d %H:%M:%S')
                        nuevos_h.append(df_h[['DATETIME','DATE','HOUR','PRICE_SP','PRICE_PT']])
                    else:
                        df_dia = df_dia.rename(columns={'PERIOD':'HOUR'})
                        nuevos_h.append(df_dia[['DATETIME','DATE','HOUR','PRICE_SP','PRICE_PT']])
                    dias_ok += 1
        except: pass
        time.sleep(0.2)
        fecha_actual += dt.timedelta(days=1)

    print(f"Dias OK: {dias_ok}")

    if nuevos_h:
        df_nh = pd.concat(nuevos_h, ignore_index=True)
        df_eh = pd.read_csv(OUTPUT_HORARIO)
        df_fh = pd.concat([df_eh, df_nh], ignore_index=True).drop_duplicates('DATETIME').sort_values('DATETIME')
        df_fh.to_csv(OUTPUT_HORARIO, index=False)
        print(f"Actualizado {OUTPUT_HORARIO}: {len(df_fh):,} filas")

        df_nh['DATE'] = pd.to_datetime(df_nh['DATE'])
        df_nd = df_nh.groupby('DATE').agg(PRICE_SP=('PRICE_SP','mean'),PRICE_PT=('PRICE_PT','mean')).reset_index()
        df_nd['DATE'] = df_nd['DATE'].dt.strftime('%Y-%m-%d')
        df_ed = pd.read_csv(OUTPUT_DIARIO)
        df_fd = pd.concat([df_ed, df_nd], ignore_index=True).drop_duplicates('DATE').sort_values('DATE')
        df_fd.to_csv(OUTPUT_DIARIO, index=False)
        print(f"Actualizado {OUTPUT_DIARIO}: {len(df_fd):,} filas")

    if nuevos_15:
        df_n15 = pd.concat(nuevos_15, ignore_index=True)
        if os.path.exists(OUTPUT_15MIN):
            df_e15 = pd.read_csv(OUTPUT_15MIN)
            df_f15 = pd.concat([df_e15, df_n15], ignore_index=True).drop_duplicates('DATETIME').sort_values('DATETIME')
        else:
            df_f15 = df_n15
        df_f15.to_csv(OUTPUT_15MIN, index=False)
        print(f"Actualizado {OUTPUT_15MIN}: {len(df_f15):,} filas")


if __name__ == "__main__":
    print("="*60)
    print("OMIE Spot v3 - Horario + 15min + Diario")
    print("="*60)
    os.makedirs("data", exist_ok=True)
    ultima_fecha = convertir_spot_existente()
    if ultima_fecha:
        descargar_nuevos_datos(ultima_fecha)
    print("\nScript 01 v3 completado.")
