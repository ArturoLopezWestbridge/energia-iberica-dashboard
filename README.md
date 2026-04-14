# ⚡ energia-iberica-dashboard

Dashboard de precios de energía del mercado ibérico (España y Portugal).
Datos de electricidad (spot + futuros) actualizados automáticamente cada día.

---

## 📁 Estructura del proyecto

```
energia-iberica-dashboard/
│
├── .github/
│   └── workflows/
│       └── actualizar_datos.yml    ← GitHub Actions (corre cada día a las 9:00)
│
├── scripts/
│   ├── 01_descarga_omie.py         ← Spot horario OMIE (España + Portugal)
│   ├── 02_descarga_omip.py         ← Futuros OMIP (semana, mes, trimestre, año)
│   └── 03_actualizar_todo.py       ← Script maestro
│
├── data/                           ← CSVs generados (auto-actualizado)
│   ├── omie_spot.csv               ← Precios spot horarios desde 2019
│   └── omip_futuros.csv            ← Futuros de electricidad desde 2019
│
├── logs/
│   └── actualizacion.log           ← Log de ejecuciones
│
├── requirements.txt                ← Dependencias Python
└── README.md
```

---

## 🚀 Configuración inicial

### 1. Clonar el repositorio
```bash
git clone https://github.com/ArturoLopezWestbridge/energia-iberica-dashboard.git
cd energia-iberica-dashboard
```

### 2. Instalar dependencias
```bash
pip install -r requirements.txt
```

### 3. Primera descarga histórica (desde 2019)
```bash
python scripts/01_descarga_omie.py   # ~5-10 min
python scripts/02_descarga_omip.py   # ~15-30 min
```

### 4. GitHub Actions (actualización automática)
El archivo `.github/workflows/actualizar_datos.yml` está configurado para:
- Ejecutarse **lunes a viernes a las 9:00 AM** (hora Madrid)
- Descargar solo los datos nuevos del día anterior
- Guardar los CSV actualizados directamente en el repositorio

También puedes ejecutarlo manualmente desde la pestaña **Actions** en GitHub.

---

## 📊 Datos disponibles

### omie_spot.csv
| Columna | Descripción |
|---|---|
| DATETIME | Fecha y hora completa |
| DATE | Fecha |
| HOUR | Hora (1-24) |
| PRICE_SP | Precio marginal España (€/MWh) |
| PRICE_PT | Precio marginal Portugal (€/MWh) |

### omip_futuros.csv
| Columna | Descripción |
|---|---|
| TRADE_DATE | Fecha de negociación |
| CONTRATO | Código del contrato (ej: Cal-26, Q1-26, M+1) |
| TIPO_PRODUCTO | ANUAL / TRIMESTRAL / MENSUAL / SEMANAL / FIN_DE_SEMANA |
| PRECIO_CIERRE | Precio de cierre (€/MWh) |
| PRECIO_LIQUIDACION | Precio de liquidación (€/MWh) |
| VOLUMEN_MWH | Volumen negociado (MWh) |
| OPEN_INTEREST | Interés abierto |

---

## 🔗 Fuentes de datos

- **OMIE** (spot): https://www.omie.es — datos públicos gratuitos
- **OMIP** (futuros): https://www.omip.pt — archivos públicos diarios

---

## 📈 Próximos pasos (Fase 2)
- [ ] Añadir datos de gas (MIBGAS)
- [ ] Calcular volatilidad histórica
- [ ] Conectar con ENTSO-E para mercado europeo
- [ ] Dashboard Power BI conectado a este repositorio
