from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pytz

import os
import sys

# Add parent dir to PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from src.extract.fetch_data import fetch_alphavan_data
from src.extract.upload_gcs import upload_to_gcs, is_last_trading_day

# Constants
SYMBOL = "SPY"

# ---------------- FUNCTION TO RUN TASKS ----------------
def fetch_and_upload_data(data_type, function, execution_date, **kwargs):
    """
    Fetch data from Alpha Vantage and upload to GCS.
    Skips execution if not the correct schedule.
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
    data = fetch_alphavan_data(SYMBOL, function)
    if data:
        object_name = f"raw/alpha_vantage/{SYMBOL}/{data_type}/{SYMBOL}_{data_type}_{execution_date.strftime('%Y%m%d')}.json"
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
    "alpha_vantage_pipeline",
    default_args=default_args,
    description="Fetch daily, weekly, and monthly SPY data from Alpha Vantage and upload to GCS",
    schedule_interval="@daily",
    catchup=False,
) as dag:

    fetch_daily = PythonOperator(
        task_id="fetch_daily",
        python_callable=fetch_and_upload_data,
        op_kwargs={"data_type": "daily", "function": "TIME_SERIES_DAILY"},
        provide_context=True
    )

    fetch_weekly = PythonOperator(
        task_id="fetch_weekly",
        python_callable=fetch_and_upload_data,
        op_kwargs={"data_type": "weekly", "function": "TIME_SERIES_WEEKLY"},
        provide_context=True
    )

    fetch_monthly = PythonOperator(
        task_id="fetch_monthly",
        python_callable=fetch_and_upload_data,
        op_kwargs={"data_type": "monthly", "function": "TIME_SERIES_MONTHLY"},
        provide_context=True
    )

    fetch_daily >> fetch_weekly >> fetch_monthly
