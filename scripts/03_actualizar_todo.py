import pandas as pd
import os

# ----------------------------
# PATHS (ajustados a /scripts/)
# ----------------------------
CSV_PATH = "../data/omip_futuros.csv"
EXCEL_TEMPLATE = "../inputs/OMIP_Template.xlsx"
OUTPUT_PATH = "../data/OMIP_actualizado.xlsx"

# ----------------------------
# LIMPIAR NOMBRE DE CONTRATO
# ----------------------------
def limpiar_contrato(c):
    if pd.isna(c):
        return None

    c = str(c).upper()

    # Quitar prefijos típicos OMIP
    c = c.replace("FTB M ", "")
    c = c.replace("FTB Q ", "")
    c = c.replace("FTB CAL ", "")
    c = c.replace("FTB YR ", "")

    return c.title().strip()

# ----------------------------
# MAIN
# ----------------------------
def main():

    # 1. Leer histórico Excel
    if not os.path.exists(EXCEL_TEMPLATE):
        raise Exception(f"No existe: {EXCEL_TEMPLATE}")

    df_hist = pd.read_excel(EXCEL_TEMPLATE)

    if "Date" not in df_hist.columns:
        raise Exception("El Excel debe tener columna 'Date'")

    df_hist["Date"] = pd.to_datetime(df_hist["Date"], dayfirst=True)

    print(f"Histórico cargado: {len(df_hist)} filas")

    # 2. Leer CSV OMIP
    if not os.path.exists(CSV_PATH):
        raise Exception(f"No existe: {CSV_PATH}")

    df = pd.read_csv(CSV_PATH)

    if "TRADE_DATE" not in df.columns:
        raise Exception("CSV sin columna TRADE_DATE")

    df["TRADE_DATE"] = pd.to_datetime(df["TRADE_DATE"])

    # Detectar columna de precio automáticamente
    posibles = [c for c in df.columns if "SETTLEMENT" in c.upper() or "PRICE" in c.upper()]
    if not posibles:
        raise Exception("No se encontró columna de precio (SETTLEMENT/PRICE)")

    PRECIO_COL = posibles[0]
    print(f"Usando columna precio: {PRECIO_COL}")

    df = df[["TRADE_DATE", "CONTRATO", PRECIO_COL]].copy()

    # 3. Limpiar contratos
    df["CONTRATO"] = df["CONTRATO"].apply(limpiar_contrato)

    # 4. Pivot (fechas vs contratos)
    pivot = df.pivot(index="TRADE_DATE", columns="CONTRATO", values=PRECIO_COL)

    pivot = pivot.reset_index().rename(columns={"TRADE_DATE": "Date"})

    print(f"Pivot generado: {pivot.shape}")

    # 5. Merge con histórico
    df_final = pd.concat([df_hist, pivot], ignore_index=True)

    df_final = df_final.drop_duplicates(subset=["Date"], keep="last")
    df_final = df_final.sort_values("Date")

    print(f"Total fechas tras merge: {len(df_final)}")

    # 6. Rellenar calendario completo (incluye fines de semana)
    df_final = df_final.set_index("Date").asfreq("D")

    # Forward fill (precios)
    df_final = df_final.ffill()

    df_final = df_final.reset_index()

    print("Fines de semana rellenados")

    # 7. Guardar Excel
    os.makedirs("../data", exist_ok=True)

    df_final.to_excel(OUTPUT_PATH, index=False)

    print(f"Excel actualizado guardado en: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
