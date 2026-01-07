# End-to-End E-Commerce Data Engineering Pipeline

This project demonstrates a production-style data engineering pipeline that ingests raw e-commerce data, processes it through a modern ELT workflow, and makes it analytics-ready using industry-standard tools.

The goal of the project is to showcase:
Data Ingestion
Orchestration with Airflow
Cloud Storage (AWS S3)
Transformation with dbt
Testing and data quality checks
Analytics-ready data modelling

---

## 🚀 Architecture Overview:
CSV Files → Ingestion Scripts → AWS S3 (Raw)
        → Airflow Orchestration
        → Data Cleaning Layer
        → dbt Transformations
        → Analytics Tables

**Tech Stack**
- **Python** – ingestion & transformations
- **AWS S3** – raw + cleaned data storage
- **Apache Airflow** – orchestration
- **dbt** – transformations & tests
- **Postgres** – analytics warehouse
- **Docker** – reproducible environment

---

## 🧩 Pipeline Flow

1. Generate run timestamp

2. Ingestion
   - Reads raw CSV files (orders, customers, products, payments, order_items)
   - Adds ingestion metadata
   - Uploads versioned data to S3

2. Transformation
   - Cleans and standardizes raw data
   - Moves curated data to clean S3 layer

3. Analytics Modeling (dbt)
   - Staging models for source normalization
   - Fact & dimension tables:
     - `fact_orders`
     - `fact_order_items`
     - `dim_customers`
     - `dim_products`

4. Orchestration
   - End-to-end pipeline automated with Airflow DAG
   - Tasks:
     - `generate_run_ts`
     - `ingest_raw_data`
     - `clean_raw_to_clean_s3`
     - `dbt_run`
     - `dbt_test`

---

## Repository Structure

ecommerce-data-pipeline/
│
├── airflow/
│   ├── dags/                # Airflow DAGs
│   ├── plugins/
│   ├── logs/
│
├── ingestion/
│   └── ingest_orders.py     # Raw data ingestion to S3
│
├── transform/
│   └── clean_tables.py      # Cleaning layer
│
├── ecommerce_dbt/
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   └── tests/
│
└── data/
    └── raw/                 # Source CSV files (local)

