from datetime import datetime, timedelta
import os

import requests
import pandas as pd

from google.cloud import storage


AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
KEY2 = os.getenv("HSTH_AV_KEY")
GCP_KEY_FILE = os.getenv("GCP_KEY_FILE")

SOURCE="alphavantage"
API_KEY = KEY2
YYYY="2019"
MM="13" # start from 13 to get the full year
INTERVAL="5min"
TICKER="SPY"

BUCKET_NAME = "cgpdata"
GCPBUCKET_FOLDER_STRING = "bronze"
LOCAL_PATH="/home/hugh.harford/code/hughharford/cgpdata/data"
YESTERDAY=(datetime.today()- timedelta(1)).strftime('%Y-%m-%d')


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket:        bucket_name = "your-bucket-name"
    # The path to your file to upload:  source_file_name = "local/path/to/file"
    # The ID of your GCS object:        destination_blob_name = "storage-object-name"

    storage_client = storage.Client.from_service_account_json(GCP_KEY_FILE)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Optional: set a generation-match precondition to avoid potential race conditions
    # and data corruptions. The request to upload is aborted if the object's
    # generation number does not match your precondition. For a destination
    # object that does not yet exist, set the if_generation_match precondition to 0.
    # If the destination object already exists in your bucket, set instead a
    # generation-match precondition using its generation number.
    generation_match_precondition = 0

    blob.upload_from_filename(source_file_name, if_generation_match=generation_match_precondition)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )

def get_historicals_from_alphavantage(ticker=TICKER, interval=INTERVAL, yyyy=YYYY, mm=MM):
    print(f"getting Monthly data from {SOURCE} for {yyyy}_{mm}")

    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={ticker}&interval={interval}&month={yyyy}-{mm}&outputsize=full&apikey={API_KEY}"
    response = requests.get(url)
    data = response.json()
    return data
    # return 1

def source_backward_months_data():
    yyyy=int(YYYY)
    mm=int(MM)

    bucket_name = BUCKET_NAME
    gcpbucket_folder_string = GCPBUCKET_FOLDER_STRING


    for m in range(mm,1,-1):
        mm=mm-1
        print(f'yyyy={str(yyyy)} and mm={str(mm)}')
        data = get_historicals_from_alphavantage(yyyy=yyyy, mm=mm)
        if 'Information' in data.keys():
            print(f"{data['Information']}")
            raise IndexError('ERROR: stopping data retrieval and upload now')
        df = pd.DataFrame.from_dict(data["Time Series (5min)"], orient="index")
        csv_name = f"{yyyy}-{mm}-{TICKER}_{INTERVAL}_{SOURCE}.csv"
        df.to_csv(f"{LOCAL_PATH}/{csv_name}")
        # if df["Time Series (5min)"][0][:10] != YESTERDAY:
        source_file_name = f"{LOCAL_PATH}/{csv_name}"
        destination_blob_name = f"{gcpbucket_folder_string}/{csv_name}"
        upload_blob(bucket_name, source_file_name, destination_blob_name)
        # else:
        #     print(f'date time from retrieved data is yesteday: {df["Time Series (5min)"][0][:10]}')
        #     print('stopping data retrieval and upload now')
        #     break

if __name__ == "__main__":
    source_backward_months_data()
