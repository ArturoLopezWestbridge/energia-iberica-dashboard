"""
Script 02 - Descarga de futuros OMIP
Versión 2 - Descarga por bloques para evitar timeout en GitHub Actions
"""

import requests
import pandas as pd
import os
import datetime as dt
import time
from io import StringIO

OUTPUT_PATH = "data/omip_futuros.csv"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
}
MAX_DIAS_POR_EJECUCION = 90

def obtener_fecha_inicio():
    if os.path.exists(OUTPUT_PATH):
        df = pd.read_csv(OUTPUT_PATH)
        if "TRADE_DATE" in df.columns and len(df) > 0:
            ultima = pd.to_datetime(df["TRADE_DATE"]).max()
            fecha_inicio = ultima + dt.timedelta(days=1)
            print(f"✅ CSV existente. Continuando desde: {fecha_inicio.date()}")
            return df, fecha_inicio
    print(f"📥 Primera ejecución. Empezando desde 2019-01-02")
    return None, dt.datetime(2019, 1, 2)

def descargar_dia(fecha):
    fecha_str = fecha.strftime("%Y%m%d")
    fecha_display = fecha.strftime("%Y-%m-%d")
    urls = [
        f"https://www.omip.pt/sites/default/files/dados_mercado/{fecha_str}_EL.csv",
        f"https://www.omip.pt/sites/default/files/dados_mercado/{fecha_str}_EL_v2.csv",
    ]
    for url in urls:
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            if r.status_code == 200 and len(r.content) > 100:
                try:
                    df = pd.read_csv(StringIO(r.text), sep=";", decimal=",")
                    if len(df) > 0:
                        df["TRADE_DATE"] = fecha_display
                        return df
                except Exception:
                    pass
        except Exception:
            pass
    return None

def clasificar_producto(contrato):
    if pd.isna(contrato):
        return "UNKNOWN"
    c = str(contrato).upper()
    if "CAL" in c or "YR" in c: return "ANUAL"
    elif "Q" in c and any(str(i) in c for i in range(1,5)): return "TRIMESTRAL"
    elif "WE" in c or "WEEKEND" in c: return "FIN_DE_SEMANA"
    elif "WK" in c or "WEEK" in c: return "SEMANAL"
    elif "M" in c: return "MENSUAL"
    else: return "OTRO"

def descargar_bloque(fecha_inicio, fecha_fin):
    todos = []
    dias_ok = 0
    fecha_actual = fecha_inicio
    hoy = dt.datetime.today()
    print(f"⬇️  Descargando: {fecha_inicio.date()} → {fecha_fin.date()}")
    while fecha_actual <= fecha_fin and fecha_actual < hoy:
        if fecha_actual.weekday() < 5:
            df_dia = descargar_dia(fecha_actual)
            if df_dia is not None:
                todos.append(df_dia)
                dias_ok += 1
                if dias_ok % 10 == 0:
                    print(f"  ✅ {dias_ok} días OK... último: {fecha_actual.date()}")
            time.sleep(0.3)
        fecha_actual += dt.timedelta(days=1)
    print(f"📊 Días con datos: {dias_ok}")
    if todos:
        df = pd.concat(todos, ignore_index=True)
        col = next((c for c in df.columns if "contract" in c.lower()), None)
        if col:
            df = df.rename(columns={col: "CONTRATO"})
            df["TIPO_PRODUCTO"] = df["CONTRATO"].apply(clasificar_producto)
        return df, fecha_actual
    return None, fecha_actual

def guardar(df_nuevo, df_existente):
    os.makedirs("data", exist_ok=True)
    if df_existente is not None and df_nuevo is not None:
        df_final = pd.concat([df_existente, df_nuevo], ignore_index=True)
        keys = [c for c in ["TRADE_DATE","CONTRATO"] if c in df_final.columns]
        if keys: df_final = df_final.drop_duplicates(subset=keys, keep="last")
        df_final = df_final.sort_values("TRADE_DATE").reset_index(drop=True)
    elif df_nuevo is not None:
        df_final = df_nuevo
    else:
        print("ℹ️  Sin datos nuevos."); return
    df_final.to_csv(OUTPUT_PATH, index=False)
    print(f"💾 Guardado: {OUTPUT_PATH} | Filas: {len(df_final):,}")

def guardar_progreso(fecha):
    os.makedirs("data", exist_ok=True)
    with open("data/omip_progreso.txt","w") as f: f.write(fecha.strftime("%Y-%m-%d"))
    print(f"📌 Progreso guardado: {fecha.date()}")

def leer_progreso():
    if os.path.exists("data/omip_progreso.txt"):
        with open("data/omip_progreso.txt","r") as f:
            return dt.datetime.strptime(f.read().strip(), "%Y-%m-%d")
    return None

if __name__ == "__main__":
    print("="*60)
    print("OMIP - Descarga de Futuros (v2 - bloques de 90 días)")
    print("="*60)
    hoy = dt.datetime.today()
    df_existente, fecha_inicio = obtener_fecha_inicio()
    progreso = leer_progreso()
    if progreso and progreso > fecha_inicio:
        print(f"🔄 Continuando desde: {progreso.date()}")
        fecha_inicio = progreso
    fecha_fin = min(fecha_inicio + dt.timedelta(days=MAX_DIAS_POR_EJECUCION), hoy - dt.timedelta(days=1))
    hay_mas = fecha_fin < hoy - dt.timedelta(days=2)
    df_nuevo, fecha_hasta = descargar_bloque(fecha_inicio, fecha_fin)
    guardar(df_nuevo, df_existente)
    if hay_mas:
        guardar_progreso(fecha_hasta)
        print(f"\n⚠️  Quedan datos desde {fecha_hasta.date()} — vuelve a ejecutar el workflow.")
    else:
        if os.path.exists("data/omip_progreso.txt"): os.remove("data/omip_progreso.txt")
        print("\n✅ Descarga OMIP completada.")
    print("\n✅ Script 02 completado.")
