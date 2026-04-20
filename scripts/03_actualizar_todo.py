import os
import sys
import subprocess
import datetime as dt

def run_script(script_path):
    print(f"\n=== Ejecutando: {script_path} ===")
    result = subprocess.run([sys.executable, script_path], capture_output=True, text=True)

    print(result.stdout)

    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"Error ejecutando {script_path}")

def main():
    print("=" * 60)
    print("PIPELINE MERCADO IBÉRICO - ACTUALIZACIÓN COMPLETA")
    print(f"Fecha ejecución: {dt.datetime.now()}")
    print("=" * 60)

    # 1. OMIE diario
    run_script("scripts/01_descarga_omie.py")

    # 2. OMIP futuros (descarga incremental)
    run_script("scripts/02_descarga_omip.py")

    # 3. OMIE 15min (si aplica en tu flujo)
    if os.path.exists("scripts/04_descarga_omie_15min.py"):
        run_script("scripts/04_descarga_omie_15min.py")

    # 4. Consolidación OMIP → Excel final
    run_script("scripts/05_consolidar_omip.py")

        # 5. Capa Power BI desacoplada del Excel operativo
    run_script("scripts/06_build_powerbi_dataset.py")

    print("\n" + "=" * 60)
    print("PIPELINE COMPLETADO CORRECTAMENTE")
    print("=" * 60)

if __name__ == "__main__":
    main()
