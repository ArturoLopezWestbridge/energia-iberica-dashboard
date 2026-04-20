import os
import re
import sys
import calendar
from typing import Optional, Tuple

import pandas as pd


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

INPUT_FUTURES_ES = os.path.join(DATA_DIR, "omip_futuros_es.csv")
INPUT_FUTURES_PT = os.path.join(DATA_DIR, "omip_futuros_pt.csv")
INPUT_SPOT_DAILY = os.path.join(DATA_DIR, "omie_spot_diario.csv")

OUTPUT_FUTURES = os.path.join(DATA_DIR, "market_futures_long.csv")
OUTPUT_SPOT = os.path.join(DATA_DIR, "market_spot_long.csv")
OUTPUT_DIM_CONTRACTS = os.path.join(DATA_DIR, "dim_contracts.csv")


MONTH_MAP = {
    "Jan": 1,
    "Feb": 2,
    "Mar": 3,
    "Apr": 4,
    "May": 5,
    "Jun": 6,
    "Jul": 7,
    "Aug": 8,
    "Sep": 9,
    "Oct": 10,
    "Nov": 11,
    "Dec": 12,
    # variantes si aparecieran
    "Mrz": 3,
    "Mai": 5,
    "Okt": 10,
    "Dez": 12,
}

REQUIRED_FUTURES_COLUMNS = {
    "TRADE_DATE",
    "ZONE",
    "SECTION",
    "CONTRACT_NAME",
    "EXCEL_HEADER",
    "PRICE_USED",
    "PRICE_SOURCE",
}

REQUIRED_SPOT_COLUMNS = {
    "DATE",
    "PRICE_SP",
    "PRICE_PT",
}


class ValidationError(Exception):
    pass


def log(msg: str) -> None:
    print(msg)


def ensure_file(path: str) -> None:
    if not os.path.exists(path):
        raise ValidationError(f"No existe el archivo requerido: {path}")


def load_csv(path: str, required_columns: set) -> pd.DataFrame:
    ensure_file(path)
    df = pd.read_csv(path)

    missing = required_columns.difference(df.columns)
    if missing:
        raise ValidationError(
            f"El archivo {os.path.basename(path)} no contiene las columnas requeridas: {sorted(missing)}"
        )

    if df.empty:
        raise ValidationError(f"El archivo {os.path.basename(path)} está vacío.")

    return df


def normalize_country(zone: str) -> str:
    z = "" if pd.isna(zone) else str(zone).strip().upper()

    if z == "ES":
        return "Spain"
    if z == "PT":
        return "Portugal"

    return z


def parse_excel_header(
    header: str,
) -> Tuple[str, Optional[int], Optional[int], Optional[int], Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    s = "" if pd.isna(header) else str(header).strip()
    if not s:
        return "Unknown", None, None, None, None, None

    s = re.sub(r"\s+", " ", s)

    # Quarter: Q2 26
    m = re.fullmatch(r"Q([1-4])\s+(\d{2,4})", s, flags=re.IGNORECASE)
    if m:
        quarter = int(m.group(1))
        year = 2000 + int(m.group(2)[-2:])
        start_month = (quarter - 1) * 3 + 1
        start_date = pd.Timestamp(year=year, month=start_month, day=1)
        end_date = start_date + pd.offsets.QuarterEnd()
        return "Quarter", year, None, quarter, start_date, end_date.normalize()

    # Year: Cal 26 / CAL 26 / Yr 26 / Y 26
    m = re.fullmatch(r"(?:Cal|CAL|Yr|YR|Y)\s+(\d{2,4})", s)
    if m:
        year = 2000 + int(m.group(1)[-2:])
        start_date = pd.Timestamp(year=year, month=1, day=1)
        end_date = pd.Timestamp(year=year, month=12, day=31)
        return "Year", year, None, None, start_date, end_date

    # Month: Apr 26
    m = re.fullmatch(r"([A-Za-z]{3})\s+(\d{2,4})", s)
    if m:
        month_abbr = m.group(1).title()
        month = MONTH_MAP.get(month_abbr)
        if month is None:
            return "Unknown", None, None, None, None, None

        year = 2000 + int(m.group(2)[-2:])
        last_day = calendar.monthrange(year, month)[1]
        start_date = pd.Timestamp(year=year, month=month, day=1)
        end_date = pd.Timestamp(year=year, month=month, day=last_day)
        return "Month", year, month, None, start_date, end_date

    return "Unknown", None, None, None, None, None


def build_contract_key(country: str, contract_type: str, delivery_year: Optional[int],
                       delivery_month: Optional[int], delivery_quarter: Optional[int],
                       contract_label: str) -> str:
    country_code = "ES" if country == "Spain" else "PT" if country == "Portugal" else country.upper()

    if contract_type == "Year" and delivery_year is not None:
        return f"{country_code}_Y_{delivery_year}"

    if contract_type == "Quarter" and delivery_year is not None and delivery_quarter is not None:
        return f"{country_code}_Q_{delivery_year}_Q{delivery_quarter}"

    if contract_type == "Month" and delivery_year is not None and delivery_month is not None:
        return f"{country_code}_M_{delivery_year}_{delivery_month:02d}"

    safe_label = re.sub(r"[^A-Za-z0-9]+", "_", str(contract_label).strip()).strip("_").upper()
    return f"{country_code}_UNK_{safe_label}"


def build_contract_sort(contract_type: str, delivery_year: Optional[int],
                        delivery_month: Optional[int], delivery_quarter: Optional[int]) -> Optional[int]:
    if delivery_year is None:
        return None

    # Regla:
    # Year    -> YYYY00
    # Quarter -> YYYY(Q*10) => 202620 para Q2 2026
    # Month   -> YYYYMM     => 202604
    if contract_type == "Year":
        return delivery_year * 100

    if contract_type == "Quarter" and delivery_quarter is not None:
        return delivery_year * 100 + delivery_quarter * 10

    if contract_type == "Month" and delivery_month is not None:
        return delivery_year * 100 + delivery_month

    return None


def build_futures_dataset() -> Tuple[pd.DataFrame, pd.DataFrame]:
    log("\n[1/3] Construyendo dataset de futuros...")

    frames = []
    for path in (INPUT_FUTURES_ES, INPUT_FUTURES_PT):
        df = load_csv(path, REQUIRED_FUTURES_COLUMNS).copy()
        log(f"  Leído {os.path.basename(path)}: {len(df):,} filas")
        frames.append(df)

    futures = pd.concat(frames, ignore_index=True)

    futures["TRADE_DATE"] = pd.to_datetime(futures["TRADE_DATE"], errors="coerce").dt.normalize()
    futures["PRICE_USED"] = pd.to_numeric(futures["PRICE_USED"], errors="coerce")

    bad_trade_dates = futures["TRADE_DATE"].isna().sum()
    bad_prices = futures["PRICE_USED"].isna().sum()

    if bad_trade_dates:
        log(f"  Aviso: {bad_trade_dates:,} filas con TRADE_DATE inválido serán descartadas")
    if bad_prices:
        log(f"  Aviso: {bad_prices:,} filas con PRICE_USED inválido serán descartadas")

    futures = futures.dropna(subset=["TRADE_DATE", "PRICE_USED", "EXCEL_HEADER", "ZONE"]).copy()

    parsed = futures["EXCEL_HEADER"].apply(parse_excel_header)
    parsed_df = pd.DataFrame(
        parsed.tolist(),
        columns=[
            "ContractType",
            "DeliveryYear",
            "DeliveryMonth",
            "DeliveryQuarter",
            "StartDate",
            "EndDate",
        ],
        index=futures.index,
    )

    futures = pd.concat([futures, parsed_df], axis=1)

    unknown_contracts = futures["ContractType"].eq("Unknown").sum()
    if unknown_contracts:
        sample = (
            futures.loc[futures["ContractType"].eq("Unknown"), "EXCEL_HEADER"]
            .drop_duplicates()
            .head(10)
            .tolist()
        )
        log(f"  Aviso: {unknown_contracts:,} filas con EXCEL_HEADER no reconocible. Muestra: {sample}")

    futures["AsOfDate"] = futures["TRADE_DATE"]
    futures["Country"] = futures["ZONE"].apply(normalize_country)
    futures["Commodity"] = "Power"
    futures["Contract"] = futures["EXCEL_HEADER"].astype(str).str.strip()
    futures["Price"] = futures["PRICE_USED"].round(2)
    futures["PriceSource"] = futures["PRICE_SOURCE"].astype(str).str.strip()

    futures["ContractKey"] = futures.apply(
        lambda row: build_contract_key(
            country=row["Country"],
            contract_type=row["ContractType"],
            delivery_year=row["DeliveryYear"],
            delivery_month=row["DeliveryMonth"],
            delivery_quarter=row["DeliveryQuarter"],
            contract_label=row["Contract"],
        ),
        axis=1,
    )

    futures["ContractSort"] = futures.apply(
        lambda row: build_contract_sort(
            contract_type=row["ContractType"],
            delivery_year=row["DeliveryYear"],
            delivery_month=row["DeliveryMonth"],
            delivery_quarter=row["DeliveryQuarter"],
        ),
        axis=1,
    )

    futures_out = futures[
        [
            "AsOfDate",
            "Country",
            "Commodity",
            "Contract",
            "ContractKey",
            "ContractSort",
            "ContractType",
            "DeliveryYear",
            "DeliveryMonth",
            "DeliveryQuarter",
            "StartDate",
            "EndDate",
            "Price",
            "PriceSource",
        ]
    ].copy()

    futures_out = futures_out.drop_duplicates(subset=["AsOfDate", "Country", "Contract"], keep="last")
    futures_out = futures_out.sort_values(
        ["Country", "AsOfDate", "ContractSort", "StartDate", "Contract"]
    ).reset_index(drop=True)

    dim_contracts = futures_out[
        [
            "Country",
            "Commodity",
            "Contract",
            "ContractKey",
            "ContractSort",
            "ContractType",
            "DeliveryYear",
            "DeliveryMonth",
            "DeliveryQuarter",
            "StartDate",
            "EndDate",
        ]
    ].drop_duplicates().sort_values(
        ["Country", "ContractSort", "StartDate", "Contract"]
    ).reset_index(drop=True)

    if futures_out.empty:
        raise ValidationError("El dataset final de futuros quedó vacío.")

    log(f"  OK futuros: {len(futures_out):,} filas")
    log(f"  OK dim_contracts: {len(dim_contracts):,} filas")
    return futures_out, dim_contracts


def build_spot_dataset() -> pd.DataFrame:
    log("\n[2/3] Construyendo dataset spot...")

    spot = load_csv(INPUT_SPOT_DAILY, REQUIRED_SPOT_COLUMNS).copy()
    log(f"  Leído {os.path.basename(INPUT_SPOT_DAILY)}: {len(spot):,} filas")

    spot["DATE"] = pd.to_datetime(spot["DATE"], errors="coerce").dt.normalize()
    spot["PRICE_SP"] = pd.to_numeric(spot["PRICE_SP"], errors="coerce")
    spot["PRICE_PT"] = pd.to_numeric(spot["PRICE_PT"], errors="coerce")

    bad_dates = spot["DATE"].isna().sum()
    if bad_dates:
        log(f"  Aviso: {bad_dates:,} filas con DATE inválido serán descartadas")

    spot_long = spot.melt(
        id_vars=["DATE"],
        value_vars=["PRICE_SP", "PRICE_PT"],
        var_name="PriceField",
        value_name="Price",
    )

    spot_long = spot_long.dropna(subset=["DATE", "Price"]).copy()
    spot_long["Country"] = spot_long["PriceField"].map(
        {"PRICE_SP": "Spain", "PRICE_PT": "Portugal"}
    )
    spot_long["Commodity"] = "Power"
    spot_long["Date"] = spot_long["DATE"]
    spot_long["Price"] = pd.to_numeric(spot_long["Price"], errors="coerce").round(2)

    spot_out = spot_long[["Date", "Country", "Commodity", "Price"]].copy()
    spot_out = spot_out.dropna(subset=["Date", "Country", "Price"])
    spot_out = spot_out.drop_duplicates(subset=["Date", "Country"], keep="last")
    spot_out = spot_out.sort_values(["Country", "Date"]).reset_index(drop=True)

    if spot_out.empty:
        raise ValidationError("El dataset final de spot quedó vacío.")

    log(f"  OK spot: {len(spot_out):,} filas")
    return spot_out


def validate_output(futures: pd.DataFrame, spot: pd.DataFrame, dim_contracts: pd.DataFrame) -> None:
    log("\n[3/3] Validando outputs...")

    futures_required = {
        "AsOfDate",
        "Country",
        "Commodity",
        "Contract",
        "ContractKey",
        "ContractSort",
        "ContractType",
        "DeliveryYear",
        "DeliveryMonth",
        "DeliveryQuarter",
        "StartDate",
        "EndDate",
        "Price",
        "PriceSource",
    }

    spot_required = {
        "Date",
        "Country",
        "Commodity",
        "Price",
    }

    dim_required = {
        "Country",
        "Commodity",
        "Contract",
        "ContractKey",
        "ContractSort",
        "ContractType",
        "DeliveryYear",
        "DeliveryMonth",
        "DeliveryQuarter",
        "StartDate",
        "EndDate",
    }

    if futures_required.difference(futures.columns):
        raise ValidationError("Faltan columnas requeridas en market_futures_long.csv")

    if spot_required.difference(spot.columns):
        raise ValidationError("Faltan columnas requeridas en market_spot_long.csv")

    if dim_required.difference(dim_contracts.columns):
        raise ValidationError("Faltan columnas requeridas en dim_contracts.csv")

    dup_futures = futures.duplicated(subset=["AsOfDate", "Country", "Contract"]).sum()
    dup_spot = spot.duplicated(subset=["Date", "Country"]).sum()
    dup_dim = dim_contracts.duplicated(subset=["Country", "Contract"]).sum()

    if dup_futures:
        raise ValidationError(
            f"Hay {dup_futures:,} duplicados en futuros por AsOfDate + Country + Contract"
        )

    if dup_spot:
        raise ValidationError(
            f"Hay {dup_spot:,} duplicados en spot por Date + Country"
        )

    if dup_dim:
        raise ValidationError(
            f"Hay {dup_dim:,} duplicados en dim_contracts por Country + Contract"
        )

    null_contract_keys = futures["ContractKey"].isna().sum()
    if null_contract_keys:
        raise ValidationError(f"Hay {null_contract_keys:,} filas sin ContractKey en futures")

    if futures["Country"].nunique() < 2:
        log("  Aviso: futures contiene menos de 2 países")

    if spot["Country"].nunique() < 2:
        log("  Aviso: spot contiene menos de 2 países")

    log("  Validaciones completadas")


def save_csv(df: pd.DataFrame, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")
    log(f"  Guardado {os.path.basename(path)} | filas={len(df):,}")


def main() -> int:
    log("=" * 70)
    log("BUILD POWER BI DATASET - MERCADO ELÉCTRICO IBÉRICO")
    log("=" * 70)

    try:
        futures, dim_contracts = build_futures_dataset()
        spot = build_spot_dataset()
        validate_output(futures, spot, dim_contracts)

        save_csv(futures, OUTPUT_FUTURES)
        save_csv(spot, OUTPUT_SPOT)
        save_csv(dim_contracts, OUTPUT_DIM_CONTRACTS)

        log("\nProceso completado correctamente.")
        return 0

    except ValidationError as exc:
        log(f"\nERROR DE VALIDACIÓN: {exc}")
        return 1

    except Exception as exc:
        log(f"\nERROR INESPERADO: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
