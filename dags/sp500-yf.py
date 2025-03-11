from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import os
import sys

# Add parent dir to PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from src.extract.yfinance import fetch_and_upload_sp500


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 1),
    'dagrun_timeout':timedelta(minutes=30),  # Set a timeout in case something goes wrong
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'sp500_to_gcs',
    default_args=default_args,
    description='Fetch daily S&P 500 data and upload to GCS',
    schedule_interval="0 23 * * 1-5",  # Runs daily at 11 PM (only weekdays),
    catchup=True,
) as dag:


    fetch_and_upload_task = PythonOperator(
        task_id='fetch_and_upload_sp500',
        python_callable=fetch_and_upload_sp500,
    )

fetch_and_upload_task
