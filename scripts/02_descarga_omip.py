import os
import re
import time
import datetime as dt

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) if "__file__" in globals() else os.getcwd()
OUTPUT_PATH = os.path.join(BASE_DIR, "data", "omip_futuros_es.csv")
PROGRESS_PATH = os.path.join(BASE_DIR, "data", "omip_futuros_es_progreso.txt")

START_DATE = dt.date(2019, 1, 1)
MAX_DIAS_POR_EJECUCION = 10

BASE_URL = "https://www.omip.pt/en/dados-mercado"
PRODUCT = "EL"
ZONE = "ES"
INSTRUMENT = "FTB"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "Connection": "keep-alive",
}

MONTH_MAP = {
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

NA_VALUES = {"", "n.a.", "n.a", "na", "-", "—", "--"}


def clean_text(s):
    s = "" if s is None else str(s)
    s = s.replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def to_number(x):
    s = clean_text(x)
    if not s or s.lower() in NA_VALUES:
        return None
    s = s.replace(",", ".")
    s = re.sub(r"[^0-9.\-]", "", s)
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def normalize_label(label):
    s = clean_text(label)
    up = s.upper()

    m = re.search(r"\bQ([1-4])[- ]?(\d{2,4})\b", up)
    if m:
        return f"Q{m.group(1)} {m.group(2)[-2:]}"

    m = re.search(r"\b(?:CAL|YR|Y)[- ]?(\d{2,4})\b", up)
    if m:
        return f"Cal {m.group(1)[-2:]}"

    y = re.search(r"(\d{2,4})", up)
    yy = y.group(1)[-2:] if y else None

    for token, out_abbr in MONTH_MAP.items():
        if re.search(rf"\b{re.escape(token)}\b", up):
            if yy:
                return f"{out_abbr} {yy}"

    return s


def extract_product_label(contract_text):
    s = clean_text(contract_text)

    m = re.search(r"\b(Q[1-4]-\d{2,4})\b", s, flags=re.IGNORECASE)
    if m:
        return normalize_label(m.group(1))

    m = re.search(r"\b(?:CAL|YR|Y)[- ]?(\d{2,4})\b", s, flags=re.IGNORECASE)
    if m:
        return f"Cal {m.group(1)[-2:]}"

    for token in MONTH_MAP.keys():
        m = re.search(rf"\b({token})[- ]?(\d{{2,4}})\b", s, flags=re.IGNORECASE)
        if m:
            return normalize_label(f"{m.group(1)}-{m.group(2)}")

    parts = s.split()
    if parts:
        return normalize_label(parts[-1])

    return s


def find_heading(soup, text):
    for tag in soup.find_all(["h1", "h2", "h3", "h4"]):
        t = clean_text(tag.get_text(" ", strip=True))
        if t == text or text.lower() in t.lower():
            return tag
    return None


def parse_omip_table(table):
    rows = table.find_all("tr")
    header_idx = None
    headers = None

    for i, tr in enumerate(rows):
        txt = clean_text(tr.get_text(" ", strip=True)).lower()
        if ("contract name" in txt) and ("best bid" in txt) and ("d-1" in txt):
            header_idx = i
            tds = tr.find_all(["th", "td"])
            headers = [clean_text(td.get_text(" ", strip=True)) for td in tds]
            break

    if header_idx is None or headers is None:
        return []

    keep_pos = [idx for idx, h in enumerate(headers) if clean_text(h) != ""]
    headers_kept = [headers[idx] for idx in keep_pos]

    out = []
    for tr in rows[header_idx + 1:]:
        classes = tr.get("class") or []
        if "chart-td" in classes:
            continue

        tds = tr.find_all("td")
        if not tds:
            continue

        if len(tds) == 1 and tds[0].get("colspan"):
            continue

        vals = [clean_text(td.get_text(" ", strip=True)) for td in tds]

        if len(vals) < len(headers):
            vals += [""] * (len(headers) - len(vals))
        elif len(vals) > len(headers):
            vals = vals[: len(headers)]

        vals_kept = [vals[idx] for idx in keep_pos]
        if not vals_kept:
            continue

        contract = vals_kept[0]
        if "FTB" not in contract.upper():
            continue

        rec = dict(zip(headers_kept, vals_kept))
        out.append(rec)

    return out


def fetch_omip_ref_prices(target_date, session):
    params = {
        "date": target_date.strftime("%Y-%m-%d"),
        "product": PRODUCT,
        "zone": ZONE,
        "instrument": INSTRUMENT,
    }

    r = session.get(BASE_URL, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    records = []

    for section in ("Month", "Quarter", "Year"):
        heading = find_heading(soup, f"SPEL Base Futures - {section}")
        if not heading:
            continue

        table = heading.find_next("table")
        if not table:
            continue

        rows = parse_omip_table(table)

        for rec in rows:
            contract_full = rec.get("Contract name")
            if not contract_full:
                continue

            product_label = extract_product_label(contract_full)

            val_d1 = None
            val_d = None
            val_price = None

            for k, v in rec.items():
                ku = k.upper()
                if re.search(r"\bD\s*[-–]\s*1\b", ku):
                    val_d1 = to_number(v)
                elif re.search(r"^D\b", ku):
                    val_d = to_number(v)
                elif re.search(r"^PRICE\b", ku):
                    val_price = to_number(v)

            ref = val_d
            if ref is None:
                ref = val_price
            if ref is None:
                ref = val_d1

            if ref is not None:
                records.append(
                    {
                        "TRADE_DATE": pd.Timestamp(target_date),
                        "ZONE": ZONE,
                        "SECTION": section,
                        "CONTRACT_NAME": clean_text(contract_full),
                        "EXCEL_HEADER": product_label,
                        "PRICE_D": val_d,
                        "PRICE_D_1": val_d1,
                        "PRICE_PRICE": val_price,
                        "PRICE_USED": ref,
                        "PRICE_SOURCE": (
                            "D" if ref == val_d
                            else "PRICE" if ref == val_price
                            else "D-1"
                        ),
                    }
                )

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df = df.sort_values(["TRADE_DATE", "EXCEL_HEADER", "PRICE_SOURCE"])
    df = df.drop_duplicates(subset=["TRADE_DATE", "EXCEL_HEADER"], keep="first")
    return df


def leer_csv_existente():
    if not os.path.exists(OUTPUT_PATH):
        return None
    df = pd.read_csv(OUTPUT_PATH)
    if "TRADE_DATE" in df.columns and not df.empty:
        df["TRADE_DATE"] = pd.to_datetime(df["TRADE_DATE"])
    return df


def obtener_fecha_inicio(df_existente):
    if os.path.exists(PROGRESS_PATH):
        with open(PROGRESS_PATH, "r", encoding="utf-8") as f:
            return dt.datetime.strptime(f.read().strip(), "%Y-%m-%d").date()

    if df_existente is not None and not df_existente.empty and "TRADE_DATE" in df_existente.columns:
        ultima = pd.to_datetime(df_existente["TRADE_DATE"]).max().date()
        return ultima + dt.timedelta(days=1)

    return START_DATE


def guardar(df_nuevo, df_existente):
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


def guardar_progreso(fecha):
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
        try:
            df_dia = fetch_omip_ref_prices(fecha_actual, session)
            if not df_dia.empty:
                frames.append(df_dia)
                dias_ok += 1
                print(f"  Día con datos: {fecha_actual} | filas={len(df_dia)}")
            else:
                print(f"  Sin datos: {fecha_actual}")
        except Exception as exc:
            print(f"  Error {fecha_actual}: {exc}")

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
