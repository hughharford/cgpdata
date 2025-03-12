from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pytz

import os
import sys

# Add parent dir to PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from src.extract.fetch_data import fetch_yfinance_data
from src.extract.upload_gcs import upload_to_gcs, is_last_trading_day

# List of stocks to fetch (S&P 500 ETF + others)
#SYMBOLS = ["^GSPC", "AAPL", "TSLA"]
SYMBOLS = ["^GSPC"]
source = 'yfinance'

# ---------------- FUNCTION TO RUN TASKS ----------------
def fetch_and_upload_data(data_type, period, execution_date, **kwargs):
    """
    Fetch data from Yahoo Finance and upload to GCS.
    Skips execution if it's not the correct schedule.
    """
    est = pytz.timezone("America/New_York")
    execution_date = execution_date.astimezone(est)

    # Skip conditions
    if data_type == "weekly" and execution_date.weekday() != 4:  # Friday
        print(f"Skipping Weekly Task: Not Friday ({execution_date})")
        return

    if data_type == "monthly" and not is_last_trading_day(execution_date):
        print(f"Skipping Monthly Task: Not Last Trading Day ({execution_date})")
        return

    # Fetch and Upload
    for symbol in SYMBOLS:
        data = fetch_yfinance_data(symbol, period)
        if data:
            object_name = f"raw/yfinance/{symbol}/{data_type}/{symbol}_{data_type}_{execution_date.strftime('%Y%m%d')}.csv"
            upload_to_gcs(data, object_name)

# ---------------- AIRFLOW DAG ----------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1, tzinfo=pytz.utc),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "yfinance_pipeline",
    default_args=default_args,
    description="Fetch daily, weekly, and monthly SP500 data from Yahoo Finance and upload to GCS",
    schedule_interval="@daily",
    catchup=True,
) as dag:

    '''fetch_daily = PythonOperator(
        task_id="fetch_daily",
        python_callable=fetch_and_upload_data,
        op_kwargs={"data_type": "daily", "period": "1d"},
        provide_context=True
    )

    fetch_weekly = PythonOperator(
        task_id="fetch_weekly",
        python_callable=fetch_and_upload_data,
        op_kwargs={"data_type": "weekly", "period": "1wk"},
        provide_context=True
    )

    fetch_monthly = PythonOperator(
        task_id="fetch_monthly",
        python_callable=fetch_and_upload_data,
        op_kwargs={"data_type": "monthly", "period": "1mo"},
        provide_context=True
    )

    fetch_daily >> fetch_weekly >> fetch_monthly'''
