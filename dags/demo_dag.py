from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import requests
import pandas as pd
import yfinance as yf
import io
import os
from datetime import datetime


# Constants
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
BUCKET_NAME = "cgpdata"
GCP_PATH = "raw/"
INTERVAL = "5min"

# Stock Symbols
STOCKS = {
    "alpha_vantage": "SPY",  # S&P 500 ETF
    "yfinance": "^GSPC",
}

def get_last_trading_day(execution_date):
    """Finds the last trading day of the month for the given execution date."""
    last_day = execution_date.replace(day=1) + pd.DateOffset(months=1) - pd.DateOffset(days=1)

    while last_day.weekday() >= 5:  # Ensure it's a business day
        last_day -= pd.DateOffset(days=1)

    return last_day

def upload_to_gcs(df, filename):
    """Uploads a DataFrame as a CSV file to Google Cloud Storage."""
    if df.empty:
        raise ValueError(f"No data to upload for {filename}.")

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")

    if gcs_hook.exists(BUCKET_NAME, filename):
        print(f"File {filename} already exists in GCS. Skipping upload.")
        return

    gcs_hook.upload(
        bucket_name=BUCKET_NAME,
        object_name=filename,
        data=csv_buffer.getvalue(),
        mime_type="text/csv",
    )
    print(f"Uploaded {filename} to GCS.")

def fetch_alpha_vantage_data(execution_date):
    """Fetches S&P 500 data from Alpha Vantage and uploads it to GCS."""
    last_trading_day = get_last_trading_day(execution_date)
    start_date = execution_date.replace(day=1)

    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": STOCKS["alpha_vantage"],
        "interval": INTERVAL,
        "apikey": ALPHA_VANTAGE_API_KEY,
        "outputsize": "full",
    }

    response = requests.get(url, params=params)
    data = response.json()
    time_series_key = f"Time Series ({INTERVAL})"

    if time_series_key not in data:
        raise ValueError(f"Error fetching Alpha Vantage data: {data}")

    df = pd.DataFrame.from_dict(data[time_series_key], orient="index")
    df.reset_index(inplace=True)
    df.columns = ["timestamp", "open", "high", "low", "close", "volume"]
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values(by="timestamp")

    # Filter data for the required month
    df = df[(df["timestamp"] >= start_date) & (df["timestamp"] <= last_trading_day)]

    filename = f"{GCP_PATH}{start_date.strftime('%Y-%m')}-{STOCKS['alpha_vantage']}-{INTERVAL}-alphavantage.csv"
    upload_to_gcs(df, filename)

def fetch_yfinance_data(execution_date):
    """Fetches S&P 500 data from Yahoo Finance and uploads it to GCS."""
    last_trading_day = get_last_trading_day(execution_date)
    start_date = execution_date.replace(day=1)

    ticker = yf.Ticker(STOCKS["yfinance"])
    df = ticker.history(start=start_date, end=last_trading_day, interval="5m")

    if df.empty:
        raise ValueError("No data retrieved from Yahoo Finance.")

    df.reset_index(inplace=True)
    df.rename(columns={"Datetime": "timestamp"}, inplace=True)

    filename = f"{GCP_PATH}{start_date.strftime('%Y-%m')}-{STOCKS['yfinance']}-{INTERVAL}-yfinance.csv"
    upload_to_gcs(df, filename)

# Define the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 1),
    "retries": 1,
}

with DAG(
    "fetch_monthly_sp500_data",
    default_args=default_args,
    schedule_interval="@monthly",
    catchup=True,
) as dag:

    fetch_yfinance_task = PythonOperator(
        task_id="fetch_sp500_yfinance",
        python_callable=fetch_yfinance_data,
    )


    fetch_alpha_task = PythonOperator(
        task_id="fetch_sp500_alpha",
        python_callable=fetch_alpha_vantage_data,
    )

    fetch_yfinance_task
    fetch_alpha_task
