from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSBucketCreateAclEntryOperator
from google.cloud import storage
import yfinance as yf
import pandas as pd
import io
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

# Load environment variables
load_dotenv()

# Read GCP bucket name from .env
GCP_BUCKET_NAME = os.getenv('GCP_BUCKET_NAME')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'upload_sp500_to_gcs',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

def fetch_and_upload_to_gcs():
    sp500_data = yf.download('^GSPC', period='1d')

    csv_buffer = io.StringIO()
    sp500_data.to_csv(csv_buffer)
    csv_buffer.seek(0)

    client = storage.Client()  # Make sure credentials are set properly
    bucket = client.bucket(GCP_BUCKET_NAME)
    blob = bucket.blob(f'sp500_data_{datetime.today().strftime("%Y%m%d")}.csv')
    blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')

upload_task = PythonOperator(
    task_id='fetch_and_upload_sp500',
    python_callable=fetch_and_upload_to_gcs,
    dag=dag
)

set_acl_task = GCSBucketCreateAclEntryOperator(
    task_id='set_acl',
    bucket_name=GCP_BUCKET_NAME,
    entity='allUsers',
    role='READER',
    dag=dag
)

upload_task >> set_acl_task
