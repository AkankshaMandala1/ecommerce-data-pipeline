'''This module is responsible for cleaning and transforming raw table data before further processing.
The cleaned data is saved back to S3 for downstream tasks.'''

'''This script is supposed to produce. 5 clean tables in S3 from raw tables ingested earlier.'''

import os
import pandas as pd
import boto3
import sys
from datetime import datetime, timezone
from dotenv import load_dotenv
load_dotenv()

table_config = {
    "orders":{
        "allowed_columns": [
            "order_id", "customer_id", "order_status",
            "order_purchase_timestamp", "order_approved_at",
            "order_delivered_customer_date", "order_estimated_delivery_date",
            "ingested_at"
        ],
        "primary_key": ["order_id"],
        "required_non_null": ["order_id", "customer_id", "order_purchase_timestamp"],
        "type_casts": {
            "order_id": "string",
            "customer_id": "string",
            "order_status": "string",
        }
    },
    "order_items": {
        "allowed_columns": [
            "order_id", "order_item_id", "product_id", "seller_id",
            "price", "freight_value",
            "ingested_at"
        ],
        "primary_key": ["order_id", "order_item_id"],
        "required_non_null": ["order_id", "order_item_id", "product_id", "price"],
        "type_casts": {
            "order_item_id": "Int64",
            "price": "numeric coercion",
            "freight_value": "numeric coercion"
        }
    },
    "payments": {
        "allowed_columns": [
            "order_id", "payment_sequential", "payment_type",
            "payment_installments", "payment_value",
            "ingested_at"
        ],
        "primary_key": ["order_id", "payment_sequential"],
        "required_non_null": ["order_id", "payment_sequential", "payment_value"],
        "type_casts": {
            "payment_sequential": "Int64",
            "payment_installments": "Int64",
            "payment_value": "numeric coercion"
        }
    },
    "customers": {
        "allowed_columns": [
            "customer_id", "customer_unique_id", "customer_city", "customer_state",
            "ingested_at"
        ],
        "primary_key": ["customer_id"],
        "required_non_null": ["customer_id", "customer_unique_id"],
        "type_casts": {
            "customer_id": "string",
            "customer_unique_id": "string",
            "customer_city": "string",
            "customer_state": "string"
        }
    },
    "products": {
        "allowed_columns": [
            "product_id", "product_category_name",
            "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm",
            "ingested_at"
        ],
        "primary_key": ["product_id"],
        "required_non_null": ["product_id"],
        "type_casts": {
            "product_weight_g": "float",
            "product_length_cm": "float",
            "product_height_cm": "float",
            "product_width_cm": "float"
        }
    }
}

def ts_to_date_parts(ts_iso):
    """Convert timestamp to date parts."""
    date_part=ts_iso.split('T')[0] # Extract date part YYYY-MM-DD
    year,month,day=date_part.split('-')
    return year, month, day

def ts_to_filename(ts_iso):
    """Convert timestamp to filename friendly format."""
    ts_file = ts_iso.replace(':','').replace('-','').replace('T','_').replace('+','')
    return ts_file

def build_raw_key(table_name, run_ts):
    """Build S3 key for raw data based on table name and timestamp."""
    # Normalize timestamp to support both datetime and string inputs
    year, month, day = ts_to_date_parts(run_ts)
    ts_file = ts_to_filename(run_ts)
    s3_key = f"raw/{table_name}/{year}/{month}/{day}/{table_name}_{ts_file}.csv"
    return s3_key

def build_clean_key(table_name, run_ts):
    """Build S3 key for cleaned data based on table name and timestamp."""
    year, month, day = ts_to_date_parts(run_ts)
    ts_file = ts_to_filename(run_ts)
    s3_key = f"clean/{table_name}/{year}/{month}/{day}/{table_name}_clean_{ts_file}.csv"
    return s3_key

def read_csv_from_s3(s3_client, bucket_name, s3_key):
    """Read CSV file from S3 into a pandas DataFrame."""
    try:
        obj = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        df = pd.read_csv(obj['Body'])
        return df
    except s3_client.exceptions.NoSuchKey:
        raise ValueError(
            f"Raw file not found at s3://{bucket_name}/{s3_key}"
        )

def write_csv_to_s3(s3_client, bucket_name, s3_key, df, ts_iso):
    """Write pandas DataFrame to S3 as CSV."""
    csv_buffer = df.to_csv(index=False)
    s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=csv_buffer,
        ContentType="text/csv",
        Metadata={'cleaned_at_ts': ts_iso}
    )

def clean_table(df, table_name):
    """Clean the DataFrame based on the provided configuration."""
    # Keep only allowed columns
    if table_name in table_config:
        allowed = table_config[table_name]['allowed_columns']
        df = df[allowed].copy()
    else:
        raise ValueError(f"No configuration found for table: {table_name}")
    
    #trim strings
    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].astype("string").str.strip()

    #datetime conversion 
    if table_name == "orders":
        for c in [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]:
            if c in df.columns:
                df[c] = pd.to_datetime(df[c], errors="coerce", utc=True)
    
    # Type casting
    for col, dtype in table_config[table_name]['type_casts'].items():
        if dtype == "numeric coercion":
            df[col] = pd.to_numeric(df[col], errors='coerce')
        else:
            df[col] = df[col].astype(dtype)

    # Drop rows with nulls in required non-null columns
    df = df.dropna(subset=table_config[table_name]['required_non_null'])

    # Drop duplicates based on primary key
    pk = table_config[table_name]['primary_key']
    df = df.drop_duplicates(subset=pk, keep='last')

    return df

if __name__ == "__main__":
    #Main execution flow
    try:
        # Step 1: Generate run timestamp in ISO format
        if len(sys.argv) != 2:
            print("Usage: python clean_tables.py <run_timestamp_ISO>")
            sys.exit(1)
        run_ts = sys.argv[1]

        bucket_name=os.getenv("S3_BUCKET_NAME")
        aws_region=os.getenv("AWS_REGION")

        if not bucket_name:
            raise ValueError("Missing S3_BUCKET_NAME in environment/.env")
        if not aws_region:
            raise ValueError ("Missing AWS_REGION in the environment/.env")
        
        #create s3 client
        s3=boto3.client('s3', region_name=aws_region)

        for table_name in table_config.keys():
            # build raw s3 key
            raw_key = build_raw_key(table_name, run_ts)
            # read raw data
            df_raw = read_csv_from_s3(s3, bucket_name, raw_key)
            # clean data
            df_clean = clean_table(df_raw, table_name)
            # build clean s3 key
            clean_key = build_clean_key(table_name, run_ts)
            # write cleaned data to s3
            write_csv_to_s3(s3, bucket_name, clean_key, df_clean, run_ts)
            print(f"Successfully cleaned {len(df_clean)} rows to s3://{os.getenv('S3_BUCKET_NAME')}/{clean_key} at {run_ts}")
    except Exception as e:
        print(f"Error during cleaning process: {e}")
        sys.exit(1)