"""
Script 03 - Actualizador maestro (ORDEN CORREGIDO)
Ejecuta todos los scripts de descarga en orden
y genera un log de la ejecución.
Diseñado para correr diariamente via GitHub Actions.

ORDEN NUEVO:
1) 04_descarga_omie_15min.py   -> actualiza 15min primero
2) 01_descarga_omie.py         -> reconstruye horario/diario usando el 15min ya actualizado
3) 02_descarga_omip.py         -> futuros
"""

import subprocess
import sys
import datetime as dt
import os

LOG_PATH = "logs/actualizacion.log"

SCRIPTS = [
    "scripts/04_descarga_omie_15min.py",
    "scripts/01_descarga_omie.py",
    "scripts/02_descarga_omip.py",
]

def log(mensaje):
    timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    linea = f"[{timestamp}] {mensaje}"
    print(linea)
    os.makedirs("logs", exist_ok=True)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(linea + "\n")

if __name__ == "__main__":
    log("=" * 50)
    log("INICIO - Actualizacion diaria mercado iberico")
    log("=" * 50)

    errores = []

    for script in SCRIPTS:
        log(f"Ejecutando: {script}")
        try:
            resultado = subprocess.run(
                [sys.executable, script],
                capture_output=True,
                text=True,
                timeout=1800
            )

            if resultado.returncode == 0:
                log(f"OK: {script}")
                if resultado.stdout:
                    for linea in resultado.stdout.strip().split("\n"):
                        log(f"   {linea}")
            else:
                log(f"ERROR en {script}")
                if resultado.stderr:
                    for linea in resultado.stderr.strip().split("\n"):
                        log(f"   {linea}")
                errores.append(script)

        except subprocess.TimeoutExpired:
            log(f"TIMEOUT en {script} (>30 min)")
            errores.append(script)

        except Exception as e:
            log(f"EXCEPCION en {script}: {e}")
            errores.append(script)

    log("=" * 50)

    if errores:
        log(f"Completado con errores en: {', '.join(errores)}")
        sys.exit(1)
    else:
        log("TODOS LOS SCRIPTS COMPLETADOS CORRECTAMENTE")
        sys.exit(0)
