"""
Script 04 - OMIE 15 minutos (v4)
- Corrige la asignacion ES/PT
- Soporta dias de 92, 96 y 100 cuartos (cambio horario)
- Deduplica por DATE + PERIOD, no por DATETIME
- Permite reconstruccion completa del historico 15 min

IMPORTANTE:
- Deja RECONSTRUIR_COMPLETO = True una vez
- Cuando ya quede bien, puedes volverlo a False
"""

import requests
import pandas as pd
import os
import datetime as dt
import time
import re

OUTPUT_15MIN = "data/omie_spot_15min.csv"
FECHA_INICIO_15MIN = dt.date(2025, 10, 1)

# PONER EN TRUE UNA VEZ PARA REHACER TODO EL HISTORICO
RECONSTRUIR_COMPLETO = False

HEADERS_WEB = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}


def construir_url_omie_15min(fecha):
    fecha_str = fecha.strftime("%Y%m%d")
    return f"https://www.omie.es/en/file-download?parents=marginalpdbc&filename=marginalpdbc_{fecha_str}.1"


def formatear_datetime_local_15min(fecha, period):
    total_min = (int(period) - 1) * 15
    hora = total_min // 60
    minuto = total_min % 60
    return f"{fecha.strftime('%Y-%m-%d')} {hora:02d}:{minuto:02d}:00"


def parsear_marginalpdbc(texto, fecha):
    """
    Formato esperado:
    YYYY;MM;DD;H1Q1;VALOR_1;VALOR_2

    En la práctica validada contra tu maestro:
    - España  = columna 6
    - Portugal = columna 5

    Soporta:
    - 92 periodos  -> dia de 23 horas
    - 96 periodos  -> dia normal
    - 100 periodos -> dia de 25 horas
    """

    lineas = texto.strip().split("\n")
    if not lineas:
        return None

    sep = ";" if ";" in lineas[0] else ","
    registros = []

    for linea in lineas:
        linea = linea.strip()

        if not linea or linea.startswith("*"):
            continue

        partes = [p.strip() for p in linea.split(sep)]
        if len(partes) < 6:
            continue

        try:
            anio = int(partes[0])
            if anio < 2000 or anio > 2035:
                continue

            periodo_raw = partes[3].strip().upper()

            if "H" in periodo_raw and "Q" in periodo_raw:
                m = re.match(r"H(\d+)Q(\d+)", periodo_raw)
                if not m:
                    continue

                hora = int(m.group(1))
                cuarto = int(m.group(2))

                if hora < 1 or hora > 25 or cuarto < 1 or cuarto > 4:
                    continue

                periodo_num = (hora - 1) * 4 + cuarto
            else:
                periodo_num = int(periodo_raw)

            # Validado contra tu maestro:
            # parts[5] = España
            # parts[4] = Portugal
            precio_pt_str = partes[4].replace(",", ".").strip()
            precio_sp_str = partes[5].replace(",", ".").strip()

            precio_sp = float(precio_sp_str) if precio_sp_str else None
            precio_pt = float(precio_pt_str) if precio_pt_str else None

            registros.append({
                "PERIOD": periodo_num,
                "PRICE_SP": precio_sp,
                "PRICE_PT": precio_pt,
            })

        except (ValueError, IndexError):
            continue

    if not registros:
        return None

    df = pd.DataFrame(registros)

    # Nos quedamos solo con 15 min reales
    if df["PERIOD"].max() <= 25:
        return None

    # Deduplicar por periodo y ordenar
    df = df.drop_duplicates(subset=["PERIOD"], keep="last").sort_values("PERIOD").reset_index(drop=True)

    n = len(df)
    if n not in (92, 96, 100):
        return None

    # Validar secuencia exacta
    periodos_esperados = list(range(1, n + 1))
    if df["PERIOD"].tolist() != periodos_esperados:
        return None

    # Validaciones de calidad
    if df["PRICE_SP"].isna().any() or df["PRICE_PT"].isna().any():
        return None

    if df["PRICE_SP"].max() > 1000 or df["PRICE_SP"].min() < -500:
        return None

    if df["PRICE_PT"].max() > 1000 or df["PRICE_PT"].min() < -500:
        return None

    # Corregir x100 si hace falta
    mean_sp = df["PRICE_SP"].mean()
    mean_pt = df["PRICE_PT"].mean()

    if pd.notna(mean_sp) and mean_sp > 500:
        df["PRICE_SP"] = df["PRICE_SP"] / 100

    if pd.notna(mean_pt) and mean_pt > 500:
        df["PRICE_PT"] = df["PRICE_PT"] / 100

    df["PRICE_SP"] = df["PRICE_SP"].round(2)
    df["PRICE_PT"] = df["PRICE_PT"].round(2)

    df["DATE"] = fecha.strftime("%Y-%m-%d")
    df["DATETIME"] = df["PERIOD"].apply(lambda p: formatear_datetime_local_15min(fecha, p))

    return df[["DATETIME", "DATE", "PERIOD", "PRICE_SP", "PRICE_PT"]]


def obtener_ultima_fecha():
    if os.path.exists(OUTPUT_15MIN):
        df = pd.read_csv(OUTPUT_15MIN, usecols=["DATE"])
        if len(df) > 0:
            return pd.to_datetime(df["DATE"]).max().date()
    return None


if __name__ == "__main__":
    print("=" * 60)
    print("OMIE 15 minutos v4 - corregido")
    print("=" * 60)

    os.makedirs("data", exist_ok=True)
    ayer = dt.date.today() - dt.timedelta(days=1)

    if RECONSTRUIR_COMPLETO:
        fecha_inicio = FECHA_INICIO_15MIN
        print(f"Reconstruccion completa activada. Desde: {fecha_inicio}")
        if os.path.exists(OUTPUT_15MIN):
            os.remove(OUTPUT_15MIN)
            print(f"Eliminado archivo previo: {OUTPUT_15MIN}")
    else:
        ultima = obtener_ultima_fecha()
        if ultima:
            fecha_inicio = ultima + dt.timedelta(days=1)
            print(f"Continuando desde: {fecha_inicio}")
        else:
            fecha_inicio = FECHA_INICIO_15MIN
            print(f"Primera ejecucion. Desde: {fecha_inicio}")

    if fecha_inicio > ayer:
        print("Datos ya al dia.")
        raise SystemExit(0)

    print(f"Descargando: {fecha_inicio} -> {ayer}")

    todos = []
    dias_ok = 0
    dias_error = 0
    fecha_actual = fecha_inicio

    while fecha_actual <= ayer:
        url = construir_url_omie_15min(fecha_actual)

        try:
            r = requests.get(url, headers=HEADERS_WEB, timeout=20)

            if r.status_code == 200 and len(r.content) > 50:
                texto = r.content.decode("latin-1", errors="replace")
                df_dia = parsear_marginalpdbc(texto, fecha_actual)

                if df_dia is not None and len(df_dia) in (92, 96, 100):
                    todos.append(df_dia)
                    dias_ok += 1

                    if dias_ok % 10 == 0:
                        print(f"  OK {dias_ok} dias... ultimo: {fecha_actual}")
                else:
                    dias_error += 1
            else:
                dias_error += 1

        except Exception:
            dias_error += 1

        time.sleep(0.3)
        fecha_actual += dt.timedelta(days=1)

    print(f"Dias OK: {dias_ok} | Sin datos/error: {dias_error}")

    if todos:
        df_nuevo = pd.concat(todos, ignore_index=True)

        if (not RECONSTRUIR_COMPLETO) and os.path.exists(OUTPUT_15MIN):
            df_existente = pd.read_csv(OUTPUT_15MIN)
            df_final = pd.concat([df_existente, df_nuevo], ignore_index=True)
        else:
            df_final = df_nuevo.copy()

        df_final = (
            df_final
            .drop_duplicates(subset=["DATE", "PERIOD"], keep="last")
            .sort_values(["DATE", "PERIOD"])
            .reset_index(drop=True)
        )

        df_final.to_csv(OUTPUT_15MIN, index=False)

        print(f"Guardado {OUTPUT_15MIN}: {len(df_final):,} filas")
        print(f"Periodo: {df_final['DATE'].min()} -> {df_final['DATE'].max()}")
    else:
        print("No se descargaron datos.")

    print("\nScript 04 v4 completado.")
