import json
from datetime import timedelta
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import os

# Google Cloud Storage Bucket Name
BUCKET_NAME = 'cgpdata'

def upload_to_gcs(data, object_name):
    """
    Uploads JSON data to Google Cloud Storage (GCS).
    """
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    gcs_hook.upload(
        bucket_name=BUCKET_NAME,
        object_name=object_name,
        data=json.dumps(data),
        mime_type="application/json"
    )

def is_last_trading_day(execution_date):
    """
    Determines if the given date is the last trading day of the month.
    - If the next business day is in the next month, today is the last trading day.
    """
    next_day = execution_date + timedelta(days=1)

    # Move forward until we reach a business day
    while next_day.weekday() >= 5:  # Skip Saturday(5) and Sunday(6)
        next_day += timedelta(days=1)

    return next_day.month != execution_date.month  # If month changes, it's the last trading day
