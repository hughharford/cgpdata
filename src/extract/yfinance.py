from datetime import datetime, timedelta
import yfinance as yf
import io
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import os

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")

def fetch_and_upload_sp500():
    interval = '5m'
    period='1d'
    tickername='^GSPC'
    source ='yf'
    today = datetime.today().strftime('%Y-%m-%d')


    # GCS details
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    bucket_name = "cgpdata"  # GCS landing bucket name
    gcs_path ='raw/sp500_yf/'
    object_name = f"{gcs_path}{today}_{interval}_{tickername}_{source}.csv"  # File name based on date

    # Fetch the S&P 500 data using yfinance
    sp500_data = yf.download(tickername, start=today, end=today, interval=interval)

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
