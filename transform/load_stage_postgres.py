import os
import sys
import boto3
import csv
import psycopg2
from psycopg2 import sql
from io import StringIO

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_DB = os.getenv("PG_DB", "airflow")
PG_USER = os.getenv("PG_USER", "airflow")
PG_PASSWORD = os.getenv("PG_PASSWORD", "airflow")

AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")

TABLES = ["orders", "order_items", "payments", "customers", "products"]

def ts_to_parts(ts_iso):
    date_part = ts_iso.split("T")[0]
    year, month, day = date_part.split("-")
    ts_file = ts_iso.replace(":", "").replace("-", "").replace("T", "_").replace("+", "")
    return year, month, day, ts_file

def build_clean_key(table, run_ts):
    year, month, day, ts_file = ts_to_parts(run_ts)
    return f"clean/{table}/{year}/{month}/{day}/{table}_clean_{ts_file}.csv"

def main():
    if len(sys.argv) != 2:
        print("Usage: python load_stage_postgres.py <run_ts>")
        sys.exit(1)

    run_ts = sys.argv[1]

    if not S3_BUCKET or not AWS_REGION:
        raise ValueError("Missing AWS env vars")

    s3 = boto3.client("s3", region_name=AWS_REGION)

    conn = psycopg2.connect(
        host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD
    )
    conn.autocommit = True

    with conn.cursor() as cur:
        cur.execute("create schema if not exists stage;")

        for table in TABLES:
            key = build_clean_key(table, run_ts)

            print(f"Loading {table} from s3://{S3_BUCKET}/{key}")

            obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
            body = obj["Body"].read().decode("utf-8")

            reader = csv.reader(StringIO(body))
            headers = next(reader)

            col_defs = sql.SQL(", ").join(
                sql.SQL("{} text").format(sql.Identifier(c)) for c in headers
            )

            cur.execute(sql.SQL("drop table if exists stage.{} cascade;").format(sql.Identifier(table)))

            cur.execute(
                sql.SQL("create table stage.{} ({})").format(sql.Identifier(table), col_defs)
            )

            copy_sql = sql.SQL("copy stage.{} ({}) from stdin with csv header").format(
                sql.Identifier(table),
                sql.SQL(", ").join(sql.Identifier(c) for c in headers),
            )

            buffer = StringIO(body)
            cur.copy_expert(copy_sql.as_string(conn), buffer)

            print(f"Loaded {table} into stage")

    conn.close()

if __name__ == "__main__":
    main()
