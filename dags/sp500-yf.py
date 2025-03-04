from datetime import datetime, timedelta
import yfinance as yf
import io
from airflow import DAG
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator
import os

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")

def fetch_and_upload_sp500():
    """Fetch S&P 500 data using yfinance and upload it to GCS"""
    # GCS details
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    bucket_name = "cgpdata"  # GCS landing bucket name
    object_name = f"s&p500_data_{datetime.today().strftime('%Y%m%d')}.csv"  # File name based on date

    # Fetch the S&P 500 data using yfinance
    sp500_data = yf.download('^GSPC', period='1d', interval='1d')

    # Convert DataFrame to CSV in-memory
    csv_buffer = io.StringIO()
    sp500_data.to_csv(csv_buffer)
    csv_buffer.seek(0)

    # Upload the CSV data to Google Cloud Storage
    gcs_hook.upload(
        bucket_name=bucket_name,
        object_name=object_name,
        data=csv_buffer.getvalue(),
        mime_type='text/csv'
    )
    print(f"Successfully uploaded {object_name} to GCS bucket {bucket_name}")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'sp500_to_gcs',
    default_args=default_args,
    description='Fetch daily S&P 500 data and upload to GCS',
    schedule_interval='@daily',
    catchup=False,
) as dag:


    fetch_and_upload_task = PythonOperator(
        task_id='fetch_and_upload_sp500',
        python_callable=fetch_and_upload_sp500,
    )

fetch_and_upload_task
