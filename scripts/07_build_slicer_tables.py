"""
07_build_slicer_tables.py
"""
import pandas as pd
import os

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

# Mapa para normalizar nombres de meses al inglés
# Cubre alemán, español y cualquier locale del sistema
MONTH_NORMALIZE = {
    "Jan": "Jan", "Feb": "Feb", "Mrz": "Mar", "Mär": "Mar", "Mar": "Mar",
    "Apr": "Apr", "Mai": "May", "May": "May", "Jun": "Jun",
    "Jul": "Jul", "Aug": "Aug", "Sep": "Sep",
    "Okt": "Oct", "Oct": "Oct", "Nov": "Nov", "Dez": "Dec", "Dic": "Dec", "Dec": "Dec"
}

def normalize_month_label(label: str) -> str:
    """Convierte 'Mrz 19' → 'Mar 19', 'Mai 25' → 'May 25', etc."""
    parts = label.split(" ")
    if len(parts) == 2:
        month_abbr = parts[0]
        year = parts[1]
        normalized = MONTH_NORMALIZE.get(month_abbr, month_abbr)
        return f"{normalized} {year}"
    return label

def build_slicer_year(dim: pd.DataFrame) -> pd.DataFrame:
    years = dim[dim["ContractType"] == "Year"][["Contract", "ContractSort"]].copy()
    years = years.drop_duplicates().sort_values("ContractSort").reset_index(drop=True)
    years.columns = ["YearLabel", "YearSort"]
    return years

def build_slicer_quarter(dim: pd.DataFrame) -> pd.DataFrame:
    qtrs = dim[dim["ContractType"] == "Quarter"][["Contract", "ContractSort"]].copy()
    qtrs = qtrs.drop_duplicates().sort_values("ContractSort").reset_index(drop=True)
    qtrs.columns = ["QtrLabel", "QtrSort"]
    return qtrs

def build_slicer_month(dim: pd.DataFrame) -> pd.DataFrame:
    mons = dim[dim["ContractType"] == "Month"][["Contract", "ContractSort"]].copy()
    mons = mons.drop_duplicates().sort_values("ContractSort").reset_index(drop=True)
    mons.columns = ["MonLabel", "MonSort"]
    # Normalizar al inglés sin cambiar el formato base
    mons["MonLabel"] = mons["MonLabel"].apply(normalize_month_label)
    return mons

def build_slicer_granularity() -> pd.DataFrame:
    return pd.DataFrame({
        "Granularity": ["Day", "Month", "Quarter", "Year"],
        "GranularitySort": [1, 2, 3, 4]
    })

def build_slicer_country() -> pd.DataFrame:
    return pd.DataFrame({
        "CountryLabel": ["Spain", "Portugal"],
        "CountrySort": [1, 2]
    })

def main():
    dim_path = os.path.join(DATA_DIR, "dim_contracts.csv")
    print(f"Leyendo {dim_path}...")
    dim = pd.read_csv(dim_path)

    print(f"  Contratos únicos: {dim['Contract'].nunique()}")
    print(f"  Tipos: {dim['ContractType'].unique().tolist()}")

    slicer_year = build_slicer_year(dim)
    slicer_quarter = build_slicer_quarter(dim)
    slicer_month = build_slicer_month(dim)
    slicer_granularity = build_slicer_granularity()
    slicer_country = build_slicer_country()

    outputs = {
        "slicer_year.csv": slicer_year,
        "slicer_quarter.csv": slicer_quarter,
        "slicer_month.csv": slicer_month,
        "slicer_granularity.csv": slicer_granularity,
        "slicer_country.csv": slicer_country,
    }

    for filename, df in outputs.items():
        path = os.path.join(DATA_DIR, filename)
        df.to_csv(path, index=False)
        print(f"  ✓ {filename}: {len(df)} filas")

    print("\nSlicer tables generadas correctamente.")

if __name__ == "__main__":
    main()
