"""
07_build_slicer_tables.py
Genera las tablas de slicer desconectadas para Power BI.

Estas tablas NO se relacionan en el modelo. Power BI las usa
como fuente de selección, y las medidas DAX aplican los filtros
virtualmente sobre fact_futures via SELECTEDVALUE / VALUES.

Ejecutar después de 06_build_powerbi_dataset.py.

Entrada:  data/dim_contracts.csv
Salida:   data/slicer_year.csv
          data/slicer_quarter.csv
          data/slicer_month.csv
          data/slicer_granularity.csv
          data/slicer_country.csv
"""

import pandas as pd
import os

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


def build_slicer_year(dim: pd.DataFrame) -> pd.DataFrame:
    """Tabla de slicer para contratos anuales (Year)."""
    years = dim[dim["ContractType"] == "Year"][["Contract", "ContractSort"]].copy()
    years = years.drop_duplicates().sort_values("ContractSort").reset_index(drop=True)
    years.columns = ["YearLabel", "YearSort"]
    return years


def build_slicer_quarter(dim: pd.DataFrame) -> pd.DataFrame:
    """Tabla de slicer para contratos trimestrales (Quarter)."""
    qtrs = dim[dim["ContractType"] == "Quarter"][["Contract", "ContractSort"]].copy()
    qtrs = qtrs.drop_duplicates().sort_values("ContractSort").reset_index(drop=True)
    qtrs.columns = ["QtrLabel", "QtrSort"]
    return qtrs


def build_slicer_month(dim: pd.DataFrame) -> pd.DataFrame:
    """Tabla de slicer para contratos mensuales (Month)."""
    mons = dim[dim["ContractType"] == "Month"][["Contract", "ContractSort"]].copy()
    mons = mons.drop_duplicates().sort_values("ContractSort").reset_index(drop=True)
    mons.columns = ["MonLabel", "MonSort"]
    return mons


def build_slicer_granularity() -> pd.DataFrame:
    """Tabla estática para selector de granularidad del eje X."""
    return pd.DataFrame({
        "Granularity": ["Day", "Month", "Quarter", "Year"],
        "GranularitySort": [1, 2, 3, 4]
    })


def build_slicer_country() -> pd.DataFrame:
    """Tabla estática para selector de país."""
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

    # --- Generar slicers ---
    slicer_year = build_slicer_year(dim)
    slicer_quarter = build_slicer_quarter(dim)
    slicer_month = build_slicer_month(dim)
    slicer_granularity = build_slicer_granularity()
    slicer_country = build_slicer_country()

    # --- Guardar ---
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
    print("Subir a GitHub y conectar en Power BI como tablas sin relación.")


if __name__ == "__main__":
    main()
