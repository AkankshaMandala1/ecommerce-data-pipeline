from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from datetime import datetime, timezone

REPO = "/opt/airflow/repo"

with DAG(
    dag_id="ecommerce_end_to_end",
    start_date=datetime(2026, 1, 1),
    schedule=None,          # Airflow 2.8+ prefers schedule=
    catchup=False,
    tags=["ecommerce", "portfolio"],
) as dag:

    @task
    def generate_run_ts() -> str:
        return datetime.now(timezone.utc).isoformat()

    run_ts = generate_run_ts()

    ingest_raw = BashOperator(
        task_id="ingest_raw_data",
        bash_command=(
            f"cd {REPO} && "
            f"python ingestion/ingest_orders.py "
            "\"{{ ti.xcom_pull(task_ids='generate_run_ts') }}\""
        ),
    )

    clean_s3 = BashOperator(
        task_id="clean_raw_to_clean_s3",
        bash_command=(
            f"cd {REPO} && "
            f"python transform/clean_tables.py "
            "\"{{ ti.xcom_pull(task_ids='generate_run_ts') }}\""
        ),
    )

    load_stage = BashOperator(
    task_id="load_stage_postgres",
    bash_command=(
        f"cd {REPO} && "
        f"python transform/load_stage_postgres.py "
"\"{{ ti.xcom_pull(task_ids='generate_run_ts') }}\""
    ),
)

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {REPO} && "
            f"dbt run --project-dir ecommerce_dbt --profiles-dir /opt/airflow/.dbt"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {REPO} && "
            f"dbt test --project-dir ecommerce_dbt --profiles-dir /opt/airflow/.dbt"
        ),
    )

    run_ts >> ingest_raw >> clean_s3 >> load_stage >> dbt_run >> dbt_test
