"""
Script 03 - Actualizador maestro
Ejecuta todos los scripts de descarga en orden
y genera un log de la ejecución.
Diseñado para correr diariamente via GitHub Actions.
"""

import subprocess
import sys
import datetime as dt
import os

# ─────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────

LOG_PATH = "logs/actualizacion.log"
SCRIPTS = [
    "scripts/01_descarga_omie.py",
    "scripts/02_descarga_omip.py",
]

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────

def log(mensaje):
    timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    linea = f"[{timestamp}] {mensaje}"
    print(linea)
    os.makedirs("logs", exist_ok=True)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(linea + "\n")

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

if __name__ == "__main__":
    log("=" * 50)
    log("🚀 INICIO - Actualización diaria mercado ibérico")
    log("=" * 50)

    errores = []

    for script in SCRIPTS:
        log(f"▶️  Ejecutando: {script}")
        try:
            resultado = subprocess.run(
                [sys.executable, script],
                capture_output=True,
                text=True,
                timeout=1800  # 30 minutos máximo por script
            )
            if resultado.returncode == 0:
                log(f"✅ {script} completado correctamente")
                if resultado.stdout:
                    for linea in resultado.stdout.strip().split("\n"):
                        log(f"   {linea}")
            else:
                log(f"❌ Error en {script}")
                log(f"   {resultado.stderr}")
                errores.append(script)

        except subprocess.TimeoutExpired:
            log(f"⏱️  TIMEOUT en {script} (>30 min)")
            errores.append(script)
        except Exception as e:
            log(f"❌ Excepción en {script}: {e}")
            errores.append(script)

    log("=" * 50)
    if errores:
        log(f"⚠️  Completado con errores en: {', '.join(errores)}")
        sys.exit(1)
    else:
        log("✅ TODOS LOS SCRIPTS COMPLETADOS CORRECTAMENTE")
        sys.exit(0)
