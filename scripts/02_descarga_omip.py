import os
import re
import time
import datetime as dt
from io import StringIO

import pandas as pd
import requests

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) if "__file__" in globals() else os.getcwd()
OUTPUT_PATH = os.path.join(BASE_DIR, "data", "omip_futuros_es.csv")
PROGRESS_PATH = os.path.join(BASE_DIR, "data", "omip_futuros_es_progreso.txt")

START_DATE = dt.date(2019, 1, 1)
MAX_DIAS_POR_EJECUCION = 3
BASE_URL = "https://www.omip.pt/es/dados-mercado"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}

MONTH_MAP_OMIP_TO_EXCEL = {
    "JAN": "Jan",
    "FEB": "Feb",
    "MAR": "Mrz",
    "APR": "Apr",
    "MAY": "Mai",
    "JUN": "Jun",
    "JUL": "Jul",
    "AUG": "Aug",
    "SEP": "Sep",
    "OCT": "Okt",
    "NOV": "Nov",
    "DEC": "Dez",
}

NA_VALUES = {"", "-", "--", "n.a.", "n.a", "na", "nan", "none"}


def flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = []
    for col in df.columns:
        if isinstance(col, tuple):
            parts = []
            for part in col:
                part = str(part).strip()
                if not part or part.lower().startswith("unnamed"):
                    continue
                parts.append(part)
            col_name = " ".join(parts)
        else:
            col_name = str(col).strip()
        col_name = re.sub(r"\s+", " ", col_name)
        cols.append(col_name)
    df.columns = cols
    return df


def find_contract_col(columns) -> str | None:
    for col in columns:
        c = col.lower()
        if "contract" in c and "name" in c:
            return col
    return None


def find_d_col(columns) -> str | None:
    for col in columns:
        c = col.lower()
        if "d-1" in c:
            continue
        if re.search(r"(^|\s)d\s*\(", c):
            return col
    return None


def find_d1_col(columns) -> str | None:
    for col in columns:
        if "d-1" in col.lower():
            return col
    return None


def parse_price(value):
    if pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        return float(value)

    s = str(value).strip().lower()
    if s in NA_VALUES:
        return None

    s = re.sub(r"[^0-9,.-]", "", s)
    if not s:
        return None

    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s:
        s = s.replace(",", ".")

    try:
        return float(s)
    except ValueError:
        return None


def contract_to_excel_header(contract_name: str) -> str | None:
    c = re.sub(r"\s+", " ", str(contract_name).upper()).strip()

    m = re.search(r"FTB\s+M\s+([A-Z]{3})[- ]?(\d{2})", c)
    if m:
        month = MONTH_MAP_OMIP_TO_EXCEL.get(m.group(1))
        year = m.group(2)
        if month:
            return f"{month} {year}"

    q = re.search(r"FTB\s+Q([1-4])[- ]?(\d{2})", c)
    if q:
        return f"Q{q.group(1)} {q.group(2)}"

    y = re.search(r"FTB\s+(?:CAL|YR)[- ]?(\d{2})", c)
    if y:
        return f"Cal {y.group(1)}"

    return None


def extract_tables_from_html(html: str) -> pd.DataFrame:
    try:
        tables = pd.read_html(StringIO(html))
    except ValueError:
        return pd.DataFrame()

    extracted = []

    for table in tables:
        if table.empty:
            continue

        table = flatten_columns(table)
        contract_col = find_contract_col(table.columns)
        d_col = find_d_col(table.columns)
        d1_col = find_d1_col(table.columns)

        if not contract_col or (not d_col and not d1_col):
            continue

        tmp = pd.DataFrame()
        tmp["CONTRACT_NAME"] = table[contract_col].astype(str).str.strip()
        tmp = tmp[tmp["CONTRACT_NAME"].str.startswith("FTB", na=False)].copy()

        if d_col:
            tmp["PRICE_D"] = table[d_col].apply(parse_price)
        else:
            tmp["PRICE_D"] = None

        if d1_col:
            tmp["PRICE_D_1"] = table[d1_col].apply(parse_price)
        else:
            tmp["PRICE_D_1"] = None

        tmp["EXCEL_HEADER"] = tmp["CONTRACT_NAME"].apply(contract_to_excel_header)
        tmp = tmp[tmp["EXCEL_HEADER"].notna()].copy()

        if not tmp.empty:
            extracted.append(tmp)

    if not extracted:
        return pd.DataFrame()

    out = pd.concat(extracted, ignore_index=True)
    out = out.drop_duplicates(subset=["CONTRACT_NAME"], keep="first")
    return out


def descargar_dia(fecha: dt.date, session: requests.Session) -> pd.DataFrame:
    params = {
        "date": fecha.isoformat(),
        "product": "EL",
        "zone": "ES",
        "instrument": "FTB",
    }

    try:
        response = session.get(BASE_URL, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
    except Exception as exc:
        print(f"  Error HTTP {fecha}: {exc}")
        return pd.DataFrame()

    print(f"  URL {fecha}: {response.url}")
    print(f"  HTML size {fecha}: {len(response.text)}")

    try:
        raw_tables = pd.read_html(StringIO(response.text))
        print(f"  Tablas encontradas {fecha}: {len(raw_tables)}")
        for i, table in enumerate(raw_tables[:8]):
            table = flatten_columns(table)
            print(f"    Tabla {i}: columnas={list(table.columns)} filas={len(table)}")
    except Exception as exc:
        print(f"  read_html fallo {fecha}: {exc}")
        return pd.DataFrame()

    df = extract_tables_from_html(response.text)

    if df.empty:
        print(f"  Sin filas válidas para {fecha}")
        return df

    print(f"  Filas válidas {fecha}: {len(df)}")

    df["TRADE_DATE"] = pd.Timestamp(fecha)
    df["ZONE"] = "ES"
    df["PRICE_USED"] = df["PRICE_D"]
    df["PRICE_SOURCE"] = "D"

    mask = df["PRICE_USED"].isna() & df["PRICE_D_1"].notna()
    df.loc[mask, "PRICE_USED"] = df.loc[mask, "PRICE_D_1"]
    df.loc[mask, "PRICE_SOURCE"] = "D-1"

    df = df[df["PRICE_USED"].notna()].copy()
    df = df.drop_duplicates(subset=["TRADE_DATE", "EXCEL_HEADER"], keep="first")

    print(f"  Filas con precio {fecha}: {len(df)}")

    return df[[
        "TRADE_DATE",
        "ZONE",
        "CONTRACT_NAME",
        "EXCEL_HEADER",
        "PRICE_D",
        "PRICE_D_1",
        "PRICE_USED",
        "PRICE_SOURCE",
    ]]


def leer_csv_existente() -> pd.DataFrame | None:
    if not os.path.exists(OUTPUT_PATH):
        return None
    df = pd.read_csv(OUTPUT_PATH)
    if "TRADE_DATE" in df.columns and not df.empty:
        df["TRADE_DATE"] = pd.to_datetime(df["TRADE_DATE"])
    return df


def obtener_fecha_inicio(df_existente: pd.DataFrame | None) -> dt.date:
    if os.path.exists(PROGRESS_PATH):
        with open(PROGRESS_PATH, "r", encoding="utf-8") as f:
            return dt.datetime.strptime(f.read().strip(), "%Y-%m-%d").date()

    if df_existente is not None and not df_existente.empty and "TRADE_DATE" in df_existente.columns:
        ultima = pd.to_datetime(df_existente["TRADE_DATE"]).max().date()
        return ultima + dt.timedelta(days=1)

    return START_DATE


def guardar(df_nuevo: pd.DataFrame | None, df_existente: pd.DataFrame | None):
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    if df_nuevo is None or df_nuevo.empty:
        print("Sin datos nuevos para guardar.")
        return

    if df_existente is not None and not df_existente.empty:
        df_final = pd.concat([df_existente, df_nuevo], ignore_index=True)
    else:
        df_final = df_nuevo.copy()

    df_final["TRADE_DATE"] = pd.to_datetime(df_final["TRADE_DATE"])
    df_final = df_final.drop_duplicates(subset=["TRADE_DATE", "EXCEL_HEADER"], keep="last")
    df_final = df_final.sort_values(["TRADE_DATE", "EXCEL_HEADER"]).reset_index(drop=True)
    df_final.to_csv(OUTPUT_PATH, index=False)

    print(f"Guardado: {OUTPUT_PATH} | Filas: {len(df_final):,}")


def guardar_progreso(fecha: dt.date):
    os.makedirs(os.path.dirname(PROGRESS_PATH), exist_ok=True)
    with open(PROGRESS_PATH, "w", encoding="utf-8") as f:
        f.write(fecha.strftime("%Y-%m-%d"))
    print(f"Progreso guardado: {fecha}")


def main():
    print("=" * 60)
    print("OMIP ES - Descarga histórica pública")
    print("=" * 60)

    df_existente = leer_csv_existente()
    fecha_inicio = obtener_fecha_inicio(df_existente)
    hoy = dt.date.today()
    fecha_fin = min(fecha_inicio + dt.timedelta(days=MAX_DIAS_POR_EJECUCION - 1), hoy)

    print(f"Descargando: {fecha_inicio} -> {fecha_fin}")

    session = requests.Session()
    frames = []
    dias_ok = 0
    fecha_actual = fecha_inicio

    while fecha_actual <= fecha_fin:
        df_dia = descargar_dia(fecha_actual, session)
        if not df_dia.empty:
            frames.append(df_dia)
            dias_ok += 1
            if dias_ok % 10 == 0:
                print(f"  {dias_ok} días con datos. Último: {fecha_actual}")
        time.sleep(0.35)
        fecha_actual += dt.timedelta(days=1)

    print(f"Días con datos: {dias_ok}")

    if frames:
        df_nuevo = pd.concat(frames, ignore_index=True)
        guardar(df_nuevo, df_existente)
    else:
        print("Sin datos nuevos.")

    siguiente = fecha_fin + dt.timedelta(days=1)
    if siguiente <= hoy:
        guardar_progreso(siguiente)
        print(f"Quedan datos desde {siguiente}. Vuelve a ejecutar el workflow.")
    else:
        if os.path.exists(PROGRESS_PATH):
            os.remove(PROGRESS_PATH)
        print("Descarga completada.")


if __name__ == "__main__":
    main()
