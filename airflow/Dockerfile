FROM apache/airflow:2.8.1

USER root
RUN apt-get update && apt-get install -y gcc build-essential && apt-get clean

USER airflow
RUN pip install --no-cache-dir pandas boto3 python-dotenv psycopg2-binary dbt-postgres