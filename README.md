# Data Warehouse MTPE - Mercado Laboral Peruano

[![Python](https://img.shields.io/badge/Python-3.13+-blue.svg)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15+-336791.svg)](https://www.postgresql.org/)
[![Power BI](https://img.shields.io/badge/Power%20BI-Desktop-F2C811.svg)](https://powerbi.microsoft.com/)
[![NeonDB](https://img.shields.io/badge/NeonDB-Serverless-00E599.svg)](https://neon.tech/)

##  Descripción

Proyecto universitario para el curso de Data Warehouse con el objetivo de analizar el mercado laboral peruano con **modelo de constelación**.

**Datos Procesados**:
- 2,705 postulantes | 53,233 vacantes | 5,062 empresas
- Gap crítico: ~51,000 vacantes sin cubrir

---

##  Arquitectura

**ETL 3 Capas**: RAW → CLEANED → INTEGRATED
**Modelo**: Constelación con 8 dimensiones + 5 tablas de hechos
**Destino**: NeonDB (PostgreSQL Serverless)

---

##  Instalación

```bash
git clone <repo-url> && cd MTPE
python -m venv .venv && .venv\Scripts\activate
pip install -r requirements.txt
cp 2_ETL_INTEGRATION/config/.env.example 2_ETL_INTEGRATION/config/.env
# Editar .env con credenciales de NeonDB
```

---

##  Estructura

```
MTPE/
├── data/                    # RAW → CLEANED → INTEGRATED
├── 1_LIMPIEZA_DATOS/        # Scripts de limpieza (6 datasets)
└── 2_ETL_INTEGRATION/       # Pipeline ETL (extract, transform, load)
```

---

##  Pipeline ETL

```bash
# 1. Limpieza
cd 1_LIMPIEZA_DATOS/scripts && python clean_*.py

# 2. ETL
cd ../../2_ETL_INTEGRATION/src
python extract/extract_cleaned_data.py
python transform/transform_to_constellation.py
python load/load_to_neondb.py
```
---
##  Stack

Python 3.13+ | PostgreSQL 15 | NeonDB | Power BI | Pandas | psycopg2

**Fuente de Datos**: [Plataforma Nacional de Datos Abiertos - Gobierno del Perú](https://www.datosabiertos.gob.pe/)

---

