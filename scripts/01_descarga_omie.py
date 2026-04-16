"""
Script 01 - OMIE Spot v5.1
Genera:
- omie_spot_horario.csv
- omie_spot_diario.csv

Logica:
1) Usa omie_spot.csv como historico base
2) Mantiene/actualiza omie_spot_horario.csv
3) Hace overlay desde omie_spot_15min.csv a partir de 2025-10-01
4) Evita errores .dt forzando datetime antes de usar .dt
"""

import os
import time
import requests
import pandas as pd
import datetime as dt

INPUT_SPOT = "data/omie_spot.csv"
OUTPUT_DIARIO = "data/omie_spot_diario.csv"
OUTPUT_HORARIO = "data/omie_spot_horario.csv"
OUTPUT_15MIN = "data/omie_spot_15min.csv"

FECHA_INICIO_15MIN = dt.date(2025, 10, 1)

HEADERS_WEB = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
}

FIX_DIARIO_SP = {
    "2019-10-27": 53.10,
    "2020-03-29": 18.57,
    "2020-10-25": 14.78,
    "2021-10-31": 79.00,
    "2022-10-30": 135.62,
    "2023-10-29": 12.25,
    "2024-10-27": 77.67,
    "2025-10-26": 51.28,
}


def asegurar_datetime_col(df, col):
    df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def formatear_datetime_local_hora(fecha, hour):
    hora_cero = int(hour) - 1
    return f"{pd.to_datetime(fecha).strftime('%Y-%m-%d')} {hora_cero:02d}:00:00"


def aplicar_fixes_diario_sp(df_d):
    df = df_d.copy()
    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
    df["DATE_STR"] = df["DATE"].dt.strftime("%Y-%m-%d")

    for fecha_str, valor_sp in FIX_DIARIO_SP.items():
        mask = df["DATE_STR"] == fecha_str
        if mask.any():
            df.loc[mask, "PRICE_SP"] = round(float(valor_sp), 2)

    df = df.drop(columns=["DATE_STR"])
    return df


def convertir_spot_existente():
    if not os.path.exists(INPUT_SPOT):
        print(f"No encontrado: {INPUT_SPOT}")
        return None

    print(f"Leyendo {INPUT_SPOT}...")
    df = pd.read_csv(INPUT_SPOT, parse_dates=["DATE", "DATETIME"])
    print(f"Filas: {len(df):,} | {df['DATE'].min().date()} -> {df['DATE'].max().date()}")

    if df["PRICE_SP"].dropna().mean() > 500:
        print("Corrigiendo precios x100...")
        df["PRICE_SP"] = (df["PRICE_SP"] / 100).round(2)
        df["PRICE_PT"] = (df["PRICE_PT"] / 100).round(2)
    else:
        df["PRICE_SP"] = df["PRICE_SP"].round(2)
        df["PRICE_PT"] = df["PRICE_PT"].round(2)

    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
    df["DATETIME"] = pd.to_datetime(df["DATETIME"], errors="coerce")

    # Horario historico base
    df_h = df[["DATE", "HOUR", "PRICE_SP", "PRICE_PT"]].copy()
    df_h["DATE"] = pd.to_datetime(df_h["DATE"], errors="coerce")
    df_h["DATETIME"] = df_h.apply(lambda r: formatear_datetime_local_hora(r["DATE"], r["HOUR"]), axis=1)
    df_h["DATETIME"] = pd.to_datetime(df_h["DATETIME"], errors="coerce")
    df_h = df_h[["DATETIME", "DATE", "HOUR", "PRICE_SP", "PRICE_PT"]]
    df_h = df_h.dropna(subset=["DATE", "DATETIME"])
    df_h = df_h.drop_duplicates(subset=["DATE", "HOUR"], keep="last").sort_values(["DATE", "HOUR"]).reset_index(drop=True)

    df_h_out = df_h.copy()
    df_h_out["DATE"] = pd.to_datetime(df_h_out["DATE"], errors="coerce")
    df_h_out["DATETIME"] = pd.to_datetime(df_h_out["DATETIME"], errors="coerce")
    df_h_out["DATE"] = df_h_out["DATE"].dt.strftime("%Y-%m-%d")
    df_h_out["DATETIME"] = df_h_out["DATETIME"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df_h_out.to_csv(OUTPUT_HORARIO, index=False)
    print(f"Guardado {OUTPUT_HORARIO}: {len(df_h_out):,} filas")

    # Diario historico base
    df_d = (
        df.groupby("DATE")
        .agg(
            PRICE_SP=("PRICE_SP", "mean"),
            PRICE_PT=("PRICE_PT", "mean"),
        )
        .reset_index()
    )

    df_d["DATE"] = pd.to_datetime(df_d["DATE"], errors="coerce")
    df_d["PRICE_SP"] = df_d["PRICE_SP"].round(2)
    df_d["PRICE_PT"] = df_d["PRICE_PT"].round(2)
    df_d = aplicar_fixes_diario_sp(df_d)
    df_d = df_d.sort_values("DATE").reset_index(drop=True)

    df_d_out = df_d.copy()
    df_d_out["DATE"] = pd.to_datetime(df_d_out["DATE"], errors="coerce")
    df_d_out["DATE"] = df_d_out["DATE"].dt.strftime("%Y-%m-%d")
    df_d_out.to_csv(OUTPUT_DIARIO, index=False)
    print(f"Guardado {OUTPUT_DIARIO}: {len(df_d_out):,} filas")

    return df["DATE"].max().date()


def construir_url_omie(fecha):
    d = fecha.strftime("%d")
    m = fecha.strftime("%m")
    y = fecha.strftime("%Y")
    nombre = f"INT_PBC_EV_H_{d}_{m}_{y}_{d}_{m}_{y}.TXT"
    return f"https://www.omie.es/sites/default/files/dados/AGNO_{y}/MES_{m}/TXT/{nombre}"


def parsear_omie_txt(texto, fecha):
    lineas = texto.strip().split("\n")
    if not lineas:
        return None

    sep = ";" if ";" in lineas[0] else ","
    registros = []

    for linea in lineas:
        linea = linea.strip()
        if not linea or linea.startswith("*"):
            continue

        partes = [p.strip().replace(",", ".") for p in linea.split(sep)]
        if len(partes) < 5:
            continue

        try:
            anio = int(partes[0])
            if anio < 2000 or anio > 2035:
                continue

            periodo = int(partes[3])
            sp = float(partes[4]) if partes[4] else None
            pt = float(partes[5]) if len(partes) > 5 and partes[5] else None

            registros.append({
                "PERIOD": periodo,
                "PRICE_SP": sp,
                "PRICE_PT": pt,
            })
        except Exception:
            continue

    if not registros:
        return None

    df = pd.DataFrame(registros)
    df["DATE"] = pd.to_datetime(fecha)

    max_p = df["PERIOD"].max()
    es_15min = max_p > 25

    if df["PRICE_SP"].dropna().mean() > 500:
        df["PRICE_SP"] = (df["PRICE_SP"] / 100).round(2)
        df["PRICE_PT"] = (df["PRICE_PT"] / 100).round(2)
    else:
        df["PRICE_SP"] = df["PRICE_SP"].round(2)
        df["PRICE_PT"] = df["PRICE_PT"].round(2)

    df["es_15min"] = es_15min
    return df


def descargar_nuevos_datos(desde_fecha):
    hoy = dt.date.today()
    ayer = hoy - dt.timedelta(days=1)

    if desde_fecha >= ayer:
        print("Datos ya al dia.")
        return

    print(f"Descargando: {desde_fecha} -> {ayer}")

    nuevos_h = []
    nuevos_15 = []
    dias_ok = 0

    fecha_actual = desde_fecha + dt.timedelta(days=1)

    while fecha_actual <= ayer:
        try:
            r = requests.get(
                construir_url_omie(fecha_actual),
                headers=HEADERS_WEB,
                timeout=15,
            )

            if r.status_code == 200 and len(r.content) > 100:
                df_dia = parsear_omie_txt(r.text, fecha_actual)

                if df_dia is not None:
                    if df_dia["es_15min"].iloc[0]:
                        df_15 = df_dia.copy()
                        df_15["DATETIME"] = df_15["PERIOD"].apply(
                            lambda p: f"{fecha_actual.strftime('%Y-%m-%d')} {((p-1)*15)//60:02d}:{((p-1)*15)%60:02d}:00"
                        )
                        nuevos_15.append(
                            df_15[["DATETIME", "DATE", "PERIOD", "PRICE_SP", "PRICE_PT"]].copy()
                        )

                        df_tmp = df_dia.copy()
                        df_tmp["HORA0"] = (df_tmp["PERIOD"] - 1) // 4

                        df_h = (
                            df_tmp.groupby(["DATE", "HORA0"])
                            .agg(
                                PRICE_SP=("PRICE_SP", "mean"),
                                PRICE_PT=("PRICE_PT", "mean"),
                            )
                            .reset_index()
                        )
                        df_h["HOUR"] = df_h["HORA0"] + 1
                        df_h["DATETIME"] = df_h.apply(
                            lambda r_: formatear_datetime_local_hora(r_["DATE"], r_["HOUR"]),
                            axis=1
                        )

                        nuevos_h.append(
                            df_h[["DATETIME", "DATE", "HOUR", "PRICE_SP", "PRICE_PT"]].copy()
                        )
                    else:
                        df_h = df_dia.copy().rename(columns={"PERIOD": "HOUR"})
                        df_h["DATETIME"] = df_h.apply(
                            lambda r_: formatear_datetime_local_hora(r_["DATE"], r_["HOUR"]),
                            axis=1
                        )

                        nuevos_h.append(
                            df_h[["DATETIME", "DATE", "HOUR", "PRICE_SP", "PRICE_PT"]].copy()
                        )

                    dias_ok += 1

        except Exception:
            pass

        time.sleep(0.2)
        fecha_actual += dt.timedelta(days=1)

    print(f"Dias OK: {dias_ok}")

    if nuevos_h:
        df_nh = pd.concat(nuevos_h, ignore_index=True)

        if os.path.exists(OUTPUT_HORARIO):
            df_eh = pd.read_csv(OUTPUT_HORARIO)
            df_fh = pd.concat([df_eh, df_nh], ignore_index=True)
        else:
            df_fh = df_nh

        df_fh["DATE"] = pd.to_datetime(df_fh["DATE"], errors="coerce")
        df_fh["DATETIME"] = pd.to_datetime(df_fh["DATETIME"], errors="coerce")
        df_fh = df_fh.dropna(subset=["DATE", "DATETIME"])
        df_fh = df_fh.drop_duplicates(subset=["DATE", "HOUR"], keep="last").sort_values(["DATE", "HOUR"]).reset_index(drop=True)

        df_fh_out = df_fh.copy()
        df_fh_out["DATE"] = pd.to_datetime(df_fh_out["DATE"], errors="coerce")
        df_fh_out["DATETIME"] = pd.to_datetime(df_fh_out["DATETIME"], errors="coerce")
        df_fh_out["DATE"] = df_fh_out["DATE"].dt.strftime("%Y-%m-%d")
        df_fh_out["DATETIME"] = df_fh_out["DATETIME"].dt.strftime("%Y-%m-%d %H:%M:%S")
        df_fh_out.to_csv(OUTPUT_HORARIO, index=False)
        print(f"Actualizado {OUTPUT_HORARIO}: {len(df_fh_out):,} filas")

    if nuevos_15:
        df_n15 = pd.concat(nuevos_15, ignore_index=True)

        if os.path.exists(OUTPUT_15MIN):
            df_e15 = pd.read_csv(OUTPUT_15MIN)
            df_f15 = pd.concat([df_e15, df_n15], ignore_index=True)
        else:
            df_f15 = df_n15

        df_f15["DATE"] = pd.to_datetime(df_f15["DATE"], errors="coerce")
        df_f15 = df_f15.dropna(subset=["DATE"])
        df_f15 = df_f15.drop_duplicates(subset=["DATE", "PERIOD"], keep="last").sort_values(["DATE", "PERIOD"]).reset_index(drop=True)

        df_f15_out = df_f15.copy()
        df_f15_out["DATE"] = pd.to_datetime(df_f15_out["DATE"], errors="coerce")
        df_f15_out["DATE"] = df_f15_out["DATE"].dt.strftime("%Y-%m-%d")
        df_f15_out.to_csv(OUTPUT_15MIN, index=False)
        print(f"Actualizado {OUTPUT_15MIN}: {len(df_f15_out):,} filas")


def overlay_desde_15min():
    corte = pd.Timestamp(FECHA_INICIO_15MIN)

    if not os.path.exists(OUTPUT_HORARIO):
        print(f"No existe {OUTPUT_HORARIO}")
        return

    if not os.path.exists(OUTPUT_DIARIO):
        print(f"No existe {OUTPUT_DIARIO}")
        return

    if not os.path.exists(OUTPUT_15MIN):
        print(f"No existe {OUTPUT_15MIN}; se mantienen horario y diario tal cual.")
        return

    print(f"Aplicando overlay desde {FECHA_INICIO_15MIN} usando {OUTPUT_15MIN}...")

    df_h_base = pd.read_csv(OUTPUT_HORARIO)
    df_d_base = pd.read_csv(OUTPUT_DIARIO)
    df_15 = pd.read_csv(OUTPUT_15MIN)

    df_h_base["DATE"] = pd.to_datetime(df_h_base["DATE"], errors="coerce")
    df_h_base["DATETIME"] = pd.to_datetime(df_h_base["DATETIME"], errors="coerce")
    df_d_base["DATE"] = pd.to_datetime(df_d_base["DATE"], errors="coerce")
    df_15["DATE"] = pd.to_datetime(df_15["DATE"], errors="coerce")

    df_h_base = df_h_base.dropna(subset=["DATE"])
    df_d_base = df_d_base.dropna(subset=["DATE"])
    df_15 = df_15.dropna(subset=["DATE"])

    df_h_hist = df_h_base[df_h_base["DATE"] < corte].copy()
    df_d_hist = df_d_base[df_d_base["DATE"] < corte].copy()

    df_15["HORA0"] = (df_15["PERIOD"] - 1) // 4

    df_h_15 = (
        df_15.groupby(["DATE", "HORA0"])
        .agg(
            PRICE_SP=("PRICE_SP", "mean"),
            PRICE_PT=("PRICE_PT", "mean"),
        )
        .reset_index()
    )

    df_h_15["HOUR"] = df_h_15["HORA0"] + 1
    df_h_15["DATETIME"] = df_h_15.apply(
        lambda r_: formatear_datetime_local_hora(r_["DATE"], r_["HOUR"]),
        axis=1
    )
    df_h_15["DATETIME"] = pd.to_datetime(df_h_15["DATETIME"], errors="coerce")
    df_h_15 = df_h_15[["DATETIME", "DATE", "HOUR", "PRICE_SP", "PRICE_PT"]].copy()

    df_d_15 = (
        df_15.groupby("DATE")
        .agg(
            PRICE_SP=("PRICE_SP", "mean"),
            PRICE_PT=("PRICE_PT", "mean"),
        )
        .reset_index()
    )

    df_h_final = pd.concat([df_h_hist, df_h_15], ignore_index=True)
    df_h_final["DATE"] = pd.to_datetime(df_h_final["DATE"], errors="coerce")
    df_h_final["DATETIME"] = pd.to_datetime(df_h_final["DATETIME"], errors="coerce")
    df_h_final = df_h_final.dropna(subset=["DATE", "DATETIME"])
    df_h_final = df_h_final.drop_duplicates(subset=["DATE", "HOUR"], keep="last").sort_values(["DATE", "HOUR"]).reset_index(drop=True)
    df_h_final["PRICE_SP"] = df_h_final["PRICE_SP"].round(2)
    df_h_final["PRICE_PT"] = df_h_final["PRICE_PT"].round(2)

    df_d_final = pd.concat([df_d_hist, df_d_15], ignore_index=True)
    df_d_final["DATE"] = pd.to_datetime(df_d_final["DATE"], errors="coerce")
    df_d_final = df_d_final.dropna(subset=["DATE"])
    df_d_final = df_d_final.drop_duplicates(subset=["DATE"], keep="last").sort_values("DATE").reset_index(drop=True)
    df_d_final["PRICE_SP"] = df_d_final["PRICE_SP"].round(2)
    df_d_final["PRICE_PT"] = df_d_final["PRICE_PT"].round(2)

    df_d_final = aplicar_fixes_diario_sp(df_d_final)

    df_h_out = df_h_final.copy()
    df_h_out["DATE"] = pd.to_datetime(df_h_out["DATE"], errors="coerce")
    df_h_out["DATETIME"] = pd.to_datetime(df_h_out["DATETIME"], errors="coerce")
    df_h_out = df_h_out.dropna(subset=["DATE", "DATETIME"])
    df_h_out["DATE"] = df_h_out["DATE"].dt.strftime("%Y-%m-%d")
    df_h_out["DATETIME"] = df_h_out["DATETIME"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df_h_out.to_csv(OUTPUT_HORARIO, index=False)

    df_d_out = df_d_final.copy()
    df_d_out["DATE"] = pd.to_datetime(df_d_out["DATE"], errors="coerce")
    df_d_out = df_d_out.dropna(subset=["DATE"])
    df_d_out["DATE"] = df_d_out["DATE"].dt.strftime("%Y-%m-%d")
    df_d_out.to_csv(OUTPUT_DIARIO, index=False)

    print(f"Horario final: {len(df_h_out):,} filas")
    print(f"Diario final: {len(df_d_out):,} filas")


if __name__ == "__main__":
    print("=" * 60)
    print("OMIE Spot v5.1 - historico + overlay 15min + fixes DST")
    print("=" * 60)

    os.makedirs("data", exist_ok=True)

    ultima_fecha = convertir_spot_existente()

    if ultima_fecha:
        descargar_nuevos_datos(ultima_fecha)

    overlay_desde_15min()

    print("\nScript 01 v5.1 completado.")
