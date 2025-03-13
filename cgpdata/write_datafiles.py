
from google.cloud import storage
import os

import sys
sys.path.insert(0, '/home/hugh.harford/code/hughharford/cgpdata')
from dbdefs import crud

GCP_PROJ = os.getenv("GCP_PROJECT_ID")
GCP_KEY_FILE = os.getenv("GCP_KEY_FILE")

BUCKET_NAME = "cgpdata"
STORAGE_CLIENT = storage.Client.from_service_account_json(GCP_KEY_FILE)

db_gen = crud.get_db()
DB = next(db_gen)

def write_datafile_records():
    """
    Checks GCS for .csv files, checks already not listed in database,
    writes there if not already present.
    """
    existing = get_listed_blobs_in_cgpbackbone()
    blob_list = list_blobs_on_gcs()
    written_files = 0
    for blob in blob_list:
        if blob not in existing:
            tempdict = get_blob_metadata(bucket_name=BUCKET_NAME, blob_name=blob)
            crud.create_data_file_record(DB, tempdict)
            written_files = written_files + 1
    print(f"written {written_files} files to the cgpbackbone db")

# blob name must include prefix
def get_blob_metadata(bucket_name, blob_name, storage_client=STORAGE_CLIENT):
    """Gets a blob's metadata."""

    # storage_client = storage.Client.from_service_account_json(GCP_KEY_FILE)
    bucket = storage_client.bucket(bucket_name)

    # Retrieve a blob, and its metadata, from Google Cloud Storage.
    # Note that `get_blob` differs from `Bucket.blob`, which does not
    # make an HTTP request.
    blob = bucket.get_blob(blob_name)

    # print(f"Blob: {blob.name}")
    # print(f"Size: {blob.size} bytes")
    # print(f"Updated: {blob.updated}")
            # print(f"Bucket: {blob.bucket.name}")
            # print(f"Storage class: {blob.storage_class}")
            # print(f"ID: {blob.id}")
            # print(f"Generation: {blob.generation}")
            # print(f"Metageneration: {blob.metageneration}")
            # print(f"Etag: {blob.etag}")
            # print(f"Owner: {blob.owner}")
            # print(f"Component count: {blob.component_count}")
            # print(f"Crc32c: {blob.crc32c}")
            # print(f"md5_hash: {blob.md5_hash}")
            # print(f"Cache-control: {blob.cache_control}")
            # print(f"Content-type: {blob.content_type}")
            # print(f"Content-disposition: {blob.content_disposition}")
            # print(f"Content-encoding: {blob.content_encoding}")
            # print(f"Content-language: {blob.content_language}")
            # print(f"Metadata: {blob.metadata}")
            # print(f"Medialink: {blob.media_link}")
            # print(f"Custom Time: {blob.custom_time}")
            # print("Temporary hold: ", "enabled" if blob.temporary_hold else "disabled")
            # print(
            #     "Event based hold: ",
            #     "enabled" if blob.event_based_hold else "disabled",
            # )
            # print(f"Retention mode: {blob.retention.mode}")
            # print(f"Retention retain until time: {blob.retention.retain_until_time}")
            # if blob.retention_expiration_time:
            #     print(
            #         f"retentionExpirationTime: {blob.retention_expiration_time}"
            #     )

    abt_datafile = dict()
    abt_datafile['datafilename'] = blob_name
    abt_datafile["uploadtime"] = blob.updated
    actualname = blob_name.split("/")[-1]
    year_string = f"{actualname}".split("-")[0]
    month_string = f"{actualname}".split("-")[1]
    abt_datafile["year"] = int(year_string)
    abt_datafile["month"] = int(month_string)

    return abt_datafile


def list_blobs_on_gcs(bucket_name=BUCKET_NAME, storage_client=STORAGE_CLIENT):
    """Lists all the blobs in the bucket."""
    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name)

    # Note: The call returns a response only when the iterator is consumed.
    gcs_blobs = []
    count = 0
    for blob in blobs:
        if blob.name[-4:] == ".csv":
            # print(blob.name)
            count = count + 1
            gcs_blobs.append(blob.name)
    print(f"found {count} .csv files on GCS")
    return gcs_blobs

def get_listed_blobs_in_cgpbackbone():
    '''Lists .csv files already in the db'''
    files_found = crud.read_data_files(DB)
    checker = []
    for f in files_found:
        checker.append(f.data_file_name)
        # print(f.data_file_name)
    print(f"found {len(checker)} .csv files in cgpbackbone")
    return checker

if __name__ == "__main__":
    write_datafile_records()
