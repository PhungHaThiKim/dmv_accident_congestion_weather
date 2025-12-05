
# 🚦 US Accident – Congestion – Weather Data Lakehouse  
### End-to-End ETL Pipeline with Airflow, ClickHouse & Superset

---

## 📌 1. Overview

This project implements a complete **data lakehouse pipeline** for three large-scale real-world datasets:

| Dataset | Source | Rows |
|--------|--------|------|
| **US Accidents (2016–2022)** | Kaggle | ~7.7M |
| **US Traffic Congestions (2016–2022)** | Kaggle | ~26M |
| **US Weather Events (2016–2022)** | Kaggle | ~2.5M |

The pipeline follows a structured **Bronze → Silver → Gold** architecture and enables analytics through **Superset dashboards**.

Technologies used:

- **ClickHouse** — Fast OLAP Data Warehouse  
- **Apache Airflow** — Orchestration  
- **Docker Compose** — Infrastructure provisioning  
- **Superset** — BI dashboards  
- **Python** — ETL utilities  

---

## 🐳 2. Running with Docker

### Start everything
```bash
make up-clickhouse
make create-database

make build-airflow
make up-airflow
make up-superset
```
- Initialize schema: ```python src/init_data_clickhouse.py```
---


## 🧱 3. Architecture Overview

### **Bronze Layer**
Raw ingestion of CSV files with **no transformation**.

### **Silver Layer**
Standardized, cleaned, normalized data:

✔ UTC normalization  
✔ Null cleaning  
✔ Type corrections  
✔ Derived fields  
✔ Weather + infra unified schemas  

Silver tables:

- `silver.accidents`
- `silver.traffic_congestion`
- `silver.weather_events`

---

## 🟨 4. Gold Layer (Star Schema)

### ⭐ Dimensions

| Dimension | Description |
|----------|-------------|
| `dim_datetime` | full date–hour dimension in UTC |
| `dim_location` | hashed location surrogate keys |
| `dim_airport` | airport metadata |
| `dim_weather_type` | normalized weather types |
| `dim_city_climate` | city climate zones |
| `dim_infrastructure` | POI flags |

### 📊 Fact Tables

- `fact_accident`  
- `fact_congestion`  
- `fact_weather_events`  

### 🔗 Bridge Tables

- `bridge_accident_weather_event`  
- `bridge_accident_congestion`  

Bridge logic uses **hour-based equi-joins** (fa.time_sk = fw.time_sk ± 0/1 hr) to avoid memory explosion.

---

## 🚀 5. Airflow Pipeline

Main DAG:



## 📁 2. Project Structure

