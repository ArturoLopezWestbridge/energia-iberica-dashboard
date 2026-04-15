"""
Script 04 - OMIE 15 minutos (v2)
Descarga datos de 15 min desde octubre 2025 usando
el nuevo formato de OMIE: marginalpdbc_YYYYMMDD.1
URL: https://www.omie.es/en/file-download?parents=marginalpdbc&filename=marginalpdbc_YYYYMMDD.1
"""

import requests
import pandas as pd
import os
import datetime as dt
import time

OUTPUT_15MIN = "data/omie_spot_15min.csv"
FECHA_INICIO_15MIN = dt.date(2025, 10, 1)

HEADERS_WEB = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}

def construir_url_omie_15min(fecha):
    """Nueva URL de OMIE para datos 15 min (desde oct 2025)."""
    fecha_str = fecha.strftime("%Y%m%d")
    return f"https://www.omie.es/en/file-download?parents=marginalpdbc&filename=marginalpdbc_{fecha_str}.1"

def parsear_marginalpdbc(texto, fecha):
    """
    Parsea el archivo marginalpdbc de OMIE.
    Formato nuevo (desde oct 2025):
    YYYY;MM;DD;H1Q1;PRECIO_ES;PRECIO_PT
    donde H1Q1 = hora 1, cuarto 1 (00:00-00:15)
    
    Formato antiguo (antes oct 2025):
    YYYY;MM;DD;1;PRECIO_ES;PRECIO_PT
    donde 1 = hora 1 (00:00-01:00)
    """
    lineas = texto.strip().split('\n')
    sep = ';' if ';' in lineas[0] else ','
    registros = []

    for linea in lineas:
        linea = linea.strip()
        if not linea or linea.startswith('*'):
            continue

        partes = [p.strip() for p in linea.split(sep)]
        if len(partes) < 5:
            continue

        try:
            anio = int(partes[0])
            if anio < 2000 or anio > 2030:
                continue

            periodo_raw = partes[3].strip()

            # Detectar formato: HxQy (nuevo) o número (antiguo)
            if 'H' in periodo_raw.upper() and 'Q' in periodo_raw.upper():
                # Nuevo formato: H1Q1, H1Q2, H1Q3, H1Q4, H2Q1...
                import re
                m = re.match(r'H(\d+)Q(\d+)', periodo_raw.upper())
                if m:
                    hora = int(m.group(1))    # 1-24
                    cuarto = int(m.group(2))  # 1-4
                    # Periodo 1-96: (hora-1)*4 + cuarto
                    periodo_num = (hora - 1) * 4 + cuarto
                else:
                    continue
            else:
                # Formato antiguo: número del 1 al 24
                periodo_num = int(periodo_raw)

            # Precio España y Portugal
            precio_sp_str = partes[4].replace(',', '.').strip()
            precio_pt_str = partes[5].replace(',', '.').strip() if len(partes) > 5 else ''

            precio_sp = float(precio_sp_str) if precio_sp_str else None
            precio_pt = float(precio_pt_str) if precio_pt_str else None

            registros.append({
                'PERIOD': periodo_num,
                'PRICE_SP': precio_sp,
                'PRICE_PT': precio_pt
            })

        except (ValueError, IndexError):
            continue

    if not registros:
        return None

    df = pd.DataFrame(registros)
    max_p = df['PERIOD'].max()
    es_15min = max_p > 24

    if not es_15min:
        return None  # Solo queremos datos de 15 min

    df['DATE'] = fecha.strftime('%Y-%m-%d')
    df['DATETIME'] = (
        pd.to_datetime(fecha) +
        pd.to_timedelta((df['PERIOD'] - 1) * 15, unit='min')
    ).dt.strftime('%Y-%m-%d %H:%M:%S')

    # Corregir precios x100 si es necesario
    mean_sp = df['PRICE_SP'].dropna().mean()
    if mean_sp > 500:
        df['PRICE_SP'] = (df['PRICE_SP'] / 100).round(2)
        df['PRICE_PT'] = (df['PRICE_PT'] / 100).round(2)
    else:
        df['PRICE_SP'] = df['PRICE_SP'].round(2)
        df['PRICE_PT'] = df['PRICE_PT'].round(2)

    return df[['DATETIME', 'DATE', 'PERIOD', 'PRICE_SP', 'PRICE_PT']]


def obtener_ultima_fecha():
    if os.path.exists(OUTPUT_15MIN):
        df = pd.read_csv(OUTPUT_15MIN, usecols=['DATE'])
        if len(df) > 0:
            return pd.to_datetime(df['DATE'].max()).date()
    return None


if __name__ == "__main__":
    print("=" * 60)
    print("OMIE 15 minutos v2 - marginalpdbc format")
    print("=" * 60)

    os.makedirs("data", exist_ok=True)
    ayer = dt.date.today() - dt.timedelta(days=1)

    ultima = obtener_ultima_fecha()
    if ultima:
        fecha_inicio = ultima + dt.timedelta(days=1)
        print(f"Continuando desde: {fecha_inicio}")
    else:
        fecha_inicio = FECHA_INICIO_15MIN
        print(f"Primera ejecucion. Desde: {fecha_inicio}")

    if fecha_inicio > ayer:
        print("Datos ya al dia.")
        exit(0)

    print(f"Descargando: {fecha_inicio} -> {ayer}")

    todos = []
    dias_ok = 0
    dias_error = 0
    fecha_actual = fecha_inicio

    while fecha_actual <= ayer:
        url = construir_url_omie_15min(fecha_actual)
        try:
            r = requests.get(url, headers=HEADERS_WEB, timeout=15)
            if r.status_code == 200 and len(r.content) > 50:
                texto = r.content.decode('latin-1', errors='replace')
                df_dia = parsear_marginalpdbc(texto, fecha_actual)
                if df_dia is not None and len(df_dia) > 0:
                    todos.append(df_dia)
                    dias_ok += 1
                    if dias_ok % 10 == 0:
                        print(f"  OK {dias_ok} dias... ultimo: {fecha_actual}")
                else:
                    dias_error += 1
            else:
                dias_error += 1
        except Exception as e:
            dias_error += 1

        time.sleep(0.3)
        fecha_actual += dt.timedelta(days=1)

    print(f"Dias OK: {dias_ok} | Sin datos: {dias_error}")

    if todos:
        df_nuevo = pd.concat(todos, ignore_index=True)

        if os.path.exists(OUTPUT_15MIN):
            df_existente = pd.read_csv(OUTPUT_15MIN)
            df_final = pd.concat([df_existente, df_nuevo], ignore_index=True)
            df_final = df_final.drop_duplicates('DATETIME').sort_values('DATETIME')
        else:
            df_final = df_nuevo.sort_values('DATETIME')

        df_final.to_csv(OUTPUT_15MIN, index=False)
        print(f"Guardado {OUTPUT_15MIN}: {len(df_final):,} filas")
        print(f"Periodo: {df_final['DATE'].min()} -> {df_final['DATE'].max()}")
    else:
        print("No se descargaron datos.")

    print("\nScript 04 v2 completado.")
