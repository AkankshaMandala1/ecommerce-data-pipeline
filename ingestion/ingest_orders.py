''' The data is being ingested from a local CSV file instead of a database.
    It is order level data. This raw data is immutable and versioned for traceability. 
    The data is uploaded to S3 for further processing.

    Input file: data/raw/orders.csv
    Bucket: <bucket-name-from-env>
    Key pattern: raw/orders/YYYY/MM/DD/orders_<ts>.csv
    Logs: rows ingested, s3_key, ingestion_ts

    below are the functions defined in this module:
    1. file_existence: checks if the file exists at the given path.
    2. read_csv_file: reads the CSV file from the given path and returns a DataFrame and add metadata.
    3. ingestion_timestamp: generate single ingestion timestamp (UTC) for all records in the file and also for the S3 file.
    4. upload_to_s3: build s3 key using timestamp and date structure and uploads the DataFrame to the specified S3 bucket and path. Logs the S3 path after successful upload.

    Execution flow: 
    1. start main execution
    2. generate timestamp once
    3. validate file existence
    4. read CSV file and add metadata
    5. upload to S3 and exit successfully

    Exception handling: If any step fails, log the error and exit with failure.'''

import os
import pandas as pd
import boto3
from datetime import datetime, timezone
import sys
from dotenv import load_dotenv
load_dotenv()


def file_existence(file_path):
    ''' Check if the file exists at the given path. '''
    if os.path.isfile(file_path):
        return True
    else:
        raise FileNotFoundError(f"File not found: {file_path}")

def ingestion_timestamp():
    ''' Generate a single ingestion timestamp (UTC) for all records in the file and also for the S3 file. '''
    return datetime.now(timezone.utc).isoformat()
'''utc timestamp is generated in ISO 8601 format
    eg: '2023-10-05T14:48:00.000Z'
    ISO eg: YYYY-MM-DDTHH:MM:SS.sssZ
'''
def read_csv_file(filepath, ingested_ts):
    '''Reads csv file from given path and records the timestamp that the file was ingested.'''
    try:
        df=pd.read_csv(filepath)
        df['ingested_at']=ingested_ts
        return df
    except Exception as e:
        return f"Error reading CSV file: {e}"

def upload_to_s3(df, ts,table_name):

    """
    Upload a pandas DataFrame to S3 as a versioned raw CSV.

    Args:
        df: pandas DataFrame (already has ingested_at column)
        ts: ingestion timestamp (can be a datetime OR string)

    Returns:
        s3_key (str)

    Raises:
        ValueError if required env vars are missing
        Any boto3/client exception if upload fails
    """

    bucket_name=os.getenv("S3_BUCKET_NAME")
    aws_region=os.getenv("AWS_REGION")

    if not bucket_name:
        raise ValueError("Missing S3_BUCKET_NAME in environment/.env")
    if not aws_region:
        raise ValueError("Missing AWS_REGION in environment/.env")
    
    #normalize timestamp to support both datetime and string inputs
    #hasattr used to check if ts is datetime object
    if hasattr(ts, 'isoformat'):
        ts_iso=ts.isoformat()
        date_part=ts.date().isoformat() #YYYY-MM-DD
        #strftime to format datetime object to desired string format
        ts_file=ts.strftime("%Y%m%d_%H%M%S_%f")
    else:
        ts_iso=str(ts)
        date_part=ts_iso.split('T')[0] # Extract date part YYYY-MM-DD
        ts_file=(ts_iso.replace(':','').replace('-','').replace('T','_').replace('+',''))
    year,month,day=date_part.split('-')
    s3_key=f"raw/{table_name}/{year}/{month}/{day}/{table_name}_{ts_file}.csv"

    # Create S3 client (region explicitly set)
    s3=boto3.client('s3', region_name=aws_region)
    
    # Convert DataFrame to CSV format in memory
    csv_buffer=df.to_csv(index=False)
    
    # Uploading the CSV data to S3
    # put_object creates a new object or replaces an existing object in S3 bucket
    s3.put_object(Bucket=bucket_name, 
                  Key=s3_key, 
                  Body=csv_buffer, 
                  ContentType="text/csv", 
                  Metadata={'ingested_at': ts_iso},)
    # Log the S3 key after successful upload
    return s3_key

if __name__=="__main__":
    try:
        # If Airflow passed a run timestamp, use it. Otherwise generate.
        if len(sys.argv) == 2:
            ingestion_ts = sys.argv[1]
        else:
            ingestion_ts = ingestion_timestamp()

        BASE_PATH = os.getenv("REPO_PATH", "/opt/airflow/repo")

        file_paths = {
            "orders": f"{BASE_PATH}/data/raw/orders.csv",
            "order_items": f"{BASE_PATH}/data/raw/order_items.csv",
            "payments": f"{BASE_PATH}/data/raw/payments.csv",
            "customers": f"{BASE_PATH}/data/raw/customers.csv",
            "products": f"{BASE_PATH}/data/raw/products.csv",
        }

        for table_name, file_path in file_paths.items():
            file_existence(file_path)
            df = read_csv_file(file_path, ingestion_ts)
            if isinstance(df, str):
                raise Exception(df)

            s3_key = upload_to_s3(df, ingestion_ts, table_name)
            print(f"Successfully ingested {len(df)} rows to s3://{os.getenv('S3_BUCKET_NAME')}/{s3_key} at {ingestion_ts}")

    except Exception as e:
        print(f"Data ingestion failed: {e}")
        exit(1)