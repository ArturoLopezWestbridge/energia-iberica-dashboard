"""
Script 01 - OMIE Spot v4 (CORREGIDO)
- Mantiene descarga original
- Usa 15min como fuente de verdad desde oct-2025
- Reconstruye horario y diario desde 15min
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
    "User-Agent": "Mozilla/5.0"
}

# ─────────────────────────────────────────────
# BASE HISTÓRICA
# ─────────────────────────────────────────────

def convertir_spot_existente():
    if not os.path.exists(INPUT_SPOT):
        print(f"No encontrado: {INPUT_SPOT}")
        return None

    df = pd.read_csv(INPUT_SPOT, parse_dates=['DATE', 'DATETIME'])

    # corregir x100
    if df['PRICE_SP'].mean() > 500:
        df['PRICE_SP'] = df['PRICE_SP'] / 100
        df['PRICE_PT'] = df['PRICE_PT'] / 100

    # horario histórico
    df_h = df[['DATETIME','DATE','HOUR','PRICE_SP','PRICE_PT']].copy()
    df_h['DATE'] = df_h['DATE'].dt.strftime('%Y-%m-%d')
    df_h['DATETIME'] = df_h['DATETIME'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df_h = df_h.sort_values('DATETIME').drop_duplicates('DATETIME')
    df_h.to_csv(OUTPUT_HORARIO, index=False)

    # diario histórico
    df_d = df.groupby('DATE').agg(
        PRICE_SP=('PRICE_SP','mean'),
        PRICE_PT=('PRICE_PT','mean')
    ).reset_index()

    df_d['DATE'] = df_d['DATE'].dt.strftime('%Y-%m-%d')
    df_d.to_csv(OUTPUT_DIARIO, index=False)

    return df['DATE'].max().date()

# ─────────────────────────────────────────────
# DESCARGA OMIE
# ─────────────────────────────────────────────

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
        if not linea or linea.startswith('*'):
            continue

        partes = [p.strip().replace(',','.') for p in linea.split(sep)]
        if len(partes) < 5:
            continue

        try:
            periodo = int(partes[3])
            sp = float(partes[4]) if partes[4] else None
            pt = float(partes[5]) if len(partes)>5 and partes[5] else None

            registros.append({
                'PERIOD': periodo,
                'PRICE_SP': sp,
                'PRICE_PT': pt
            })
        except:
            continue

    if not registros:
        return None

    df = pd.DataFrame(registros)
    df['DATE'] = fecha

    max_p = df['PERIOD'].max()
    es_15min = max_p > 24

    if es_15min:
        df['DATETIME'] = (
            pd.to_datetime(fecha) +
            pd.to_timedelta((df['PERIOD']-1)*15, unit='min')
        )
    else:
        df['DATETIME'] = (
            pd.to_datetime(fecha) +
            pd.to_timedelta(df['PERIOD']-1, unit='h')
        )

    # corregir x100
    if df['PRICE_SP'].mean() > 500:
        df['PRICE_SP'] = df['PRICE_SP'] / 100
        df['PRICE_PT'] = df['PRICE_PT'] / 100

    df['es_15min'] = es_15min
    return df

def descargar_nuevos_datos(desde_fecha):
    hoy = dt.date.today()
    ayer = hoy - dt.timedelta(days=1)

    if desde_fecha >= ayer:
        print("Datos al día")
        return

    nuevos_15 = []

    fecha_actual = desde_fecha + dt.timedelta(days=1)

    while fecha_actual <= ayer:
        try:
            r = requests.get(construir_url_omie(fecha_actual), timeout=15)
            if r.status_code == 200 and len(r.content) > 100:
                df_dia = parsear_omie_txt(r.text, fecha_actual)
                if df_dia is not None and df_dia['es_15min'].iloc[0]:
                    nuevos_15.append(df_dia[['DATETIME','DATE','PERIOD','PRICE_SP','PRICE_PT']])
        except:
            pass

        time.sleep(0.2)
        fecha_actual += dt.timedelta(days=1)

    if nuevos_15:
        df_n15 = pd.concat(nuevos_15, ignore_index=True)

        if os.path.exists(OUTPUT_15MIN):
            df_e15 = pd.read_csv(OUTPUT_15MIN)
            df_final = pd.concat([df_e15, df_n15], ignore_index=True)
            df_final = df_final.drop_duplicates('DATETIME')
        else:
            df_final = df_n15

        df_final = df_final.sort_values('DATETIME')
        df_final.to_csv(OUTPUT_15MIN, index=False)

        print(f"15min actualizado: {len(df_final):,}")

# ─────────────────────────────────────────────
# RECONSTRUCCIÓN DESDE 15 MIN (FUENTE REAL)
# ─────────────────────────────────────────────

def reconstruir_desde_15min():
    if not os.path.exists(OUTPUT_15MIN):
        print("No hay datos 15min")
        return

    df = pd.read_csv(OUTPUT_15MIN)
    df['DATE'] = pd.to_datetime(df['DATE'])

    # HORARIO
    df['HORA'] = (df['PERIOD'] - 1) // 4

    df_h = df.groupby(['DATE','HORA']).agg(
        PRICE_SP=('PRICE_SP','mean'),
        PRICE_PT=('PRICE_PT','mean')
    ).reset_index()

    df_h['HOUR'] = df_h['HORA'] + 1
    df_h['DATETIME'] = pd.to_datetime(df_h['DATE']) + pd.to_timedelta(df_h['HORA'], unit='h')

    df_h['DATE'] = df_h['DATE'].dt.strftime('%Y-%m-%d')
    df_h['DATETIME'] = df_h['DATETIME'].dt.strftime('%Y-%m-%d %H:%M:%S')

    df_h = df_h.sort_values('DATETIME')
    df_h.to_csv(OUTPUT_HORARIO, index=False)

    print(f"Horario reconstruido: {len(df_h):,}")

    # DIARIO
    df_d = df.groupby('DATE').agg(
        PRICE_SP=('PRICE_SP','mean'),
        PRICE_PT=('PRICE_PT','mean')
    ).reset_index()

    df_d['DATE'] = df_d['DATE'].dt.strftime('%Y-%m-%d')
    df_d = df_d.sort_values('DATE')

    df_d.to_csv(OUTPUT_DIARIO, index=False)

    print(f"Diario reconstruido: {len(df_d):,}")

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

if __name__ == "__main__":
    print("="*60)
    print("OMIE Spot v4 - FIX 15MIN")
    print("="*60)

    os.makedirs("data", exist_ok=True)

    ultima_fecha = convertir_spot_existente()

    if ultima_fecha:
        descargar_nuevos_datos(ultima_fecha)

    # 🔥 CLAVE: reconstrucción final consistente
    reconstruir_desde_15min()

    print("\nScript completado correctamente.")
