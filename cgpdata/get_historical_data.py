from datetime import datetime, timedelta
import os

import requests
import pandas as pd

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")

KEY2 = os.getenv("HSTH_AV_KEY")


def get_historicals():
    print("looking to get data")
    API_KEY = KEY2
    # url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=SPY&interval=5min&apikey={API_KEY}"
    url = f"https://www.alphavantage.co/query?function=HISTORICAL_OPTIONS&symbol=IBM&date=2017-11-15&apikey={API_KEY}"
    response = requests.get(url)
    data = response.json()

    df = pd.DataFrame.from_dict(data["Time Series"], orient="index")
    print(df.head())
    df.to_csv("data/test-2017-11-15.csv")


# def fetch_and_upload_sp500():
#     """Fetch S&P 500 data using yfinance and upload it to GCS"""
#     # GCS details
#     gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
#     bucket_name = "cgpdata"  # GCS landing bucket name
#     object_name = f"s&p500_data_{datetime.today().strftime('%Y%m%d')}.csv"  # File name based on date

#     # Fetch the S&P 500 data using yfinance
#     sp500_data = yf.download('^GSPC', period='1d', interval='1d')

#     # Convert DataFrame to CSV in-memory
#     csv_buffer = io.StringIO()
#     sp500_data.to_csv(csv_buffer)
#     csv_buffer.seek(0)

#     # Upload the CSV data to Google Cloud Storage
#     gcs_hook.upload(
#         bucket_name=bucket_name,
#         object_name=object_name,
#         data=csv_buffer.getvalue(),
#         mime_type='text/csv'
#     )
#     print(f"Successfully uploaded {object_name} to GCS bucket {bucket_name}")

if __name__ == "__main__":
    get_historicals()
