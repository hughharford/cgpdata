from datetime import datetime, timedelta
import requests
import io
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import os
import pandas as pd

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")

def fetch_and_upload_spy():
    interval = '5m'
    tickername='SPY'
    source ='alphavan'
    today = datetime.today().strftime('%Y-%m-%d')


    API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
    url = (
        f"https://www.alphavantage.co/query?"
        f"function=TIME_SERIES_INTRADAY&"
        f"symbol={tickername}&"
        f"interval={interval}&"
        f"apikey={API_KEY}"
    )

    response = requests.get(url)
    data = response.json()

    spy_data = pd.DataFrame.from_dict(data["Time Series (5min)"], orient="index")



    # GCS details
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    bucket_name = "cgpdata"  # GCS landing bucket name
    gcs_path ='raw/spy_alphavan/'
    object_name = f"{gcs_path}{today}_{interval}_{tickername}_{source}.csv"  # File name based on date


    # Convert DataFrame to CSV in-memory
    csv_buffer = io.StringIO()
    spy_data.to_csv(csv_buffer)
    csv_buffer.seek(0)

    # Upload the CSV data to Google Cloud Storage
    gcs_hook.upload(
        bucket_name=bucket_name,
        object_name=object_name,
        data=csv_buffer.getvalue(),
        mime_type='text/csv'
    )
    print(f"Successfully uploaded {object_name} to GCS bucket {bucket_name}")
