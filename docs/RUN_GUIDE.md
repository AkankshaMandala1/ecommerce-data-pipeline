## How to Run the Pipeline ##

This project runs fully in Docker using Airflow, Postgres, and dbt.

Prerequisites
Make sure you have installed:
    Docker
    Docker Compose
    Git
    Python 

1. Clone the Repository
    git clone https://github.com/AkankshaMandala1/ecommerce-data-pipeline.git
    cd ecommerce-data-pipeline

2. Environment Setup
Create a .env file in the project root:
    touch .env

    Add:
    AWS_REGION=us-east-1
    S3_BUCKET_NAME=<your-s3-bucket>

    PG_HOST=postgres
    PG_DB=airflow
    PG_USER=airflow
    PG_PASSWORD=airflow

3. Start the Platform
    docker compose up -d

    Check containers:
    docker ps

4. Open Airflow

    Go to:
    http://localhost:8080
    Login:
    Username: airflow
    Password: airflow

5. Run the Pipeline

    Open the DAG: ecommerce_end_to_end
    Click -> Trigger DAG
    Watch tasks run in order:
        ->generate_run_ts
        -> ingest_raw_data
        -> clean_raw_to_clean_s3
        -> load_stage_postgres
        -> dbt_run
        -> dbt_test

6. Verify Results
    Check Postgres
    docker exec -it airflow-webserver bash
    PGPASSWORD=airflow psql -h postgres -U airflow -d airflow


List analytics views:

    \dv analytics.*

    You should see:
        stg_orders
        stg_customers
        stg_payments
        stg_products
        fact_orders
        fact_order_items
        dim_customers
        dim_products

7. View dbt Test Results

    From container:

    cd /opt/airflow/repo
    dbt test --project-dir ecommerce_dbt --profiles-dir /opt/airflow/.dbt


All tests should pass

8. Stop Everything
docker compose down