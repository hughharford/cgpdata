import os

from airflow import DAG

# $IMPORT_BEGIN
# noreorder
import pendulum

from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator

# $IMPORT_END


DBT_DIR = os.getenv("DBT_DIR")
GCP_PROJECT_ID=os.getenv("GCP_PROJECT_ID")
DATASET_NAME = os.getenv("GCP_BQ_DATASET_NAME")
GCP_BQ_BRONZE_STAGING_TABLE = os.getenv("GCP_BQ_BRONZE_STAGING_TABLE")
GCP_GCS_BUCKET_NAME = os.getenv("GCP_GCS_BUCKET_NAME")
SILVER_PREFIX = "silver"

dt = pendulum.now().to_date_string()
SILVER_BUCKET_FILE = f"{dt}_e_silver_total_output"

with DAG(
    "bronze_to_silver_dbt",
    # $CODE_BEGIN
    default_args={ "depends_on_past": False, },
    start_date=pendulum.today("UTC").add(days=-1),
    schedule_interval="@daily",
    catchup=False,
    # $CODE_END
) as dag:
    # dbt_dag_test = BashOperator(
    #     task_id="dbt_dag_test",
    #     bash_command=f"dbt source freshness --project-dir {DBT_DIR}",
    # )

    d_bronze_staging_table = BashOperator(
        task_id="d_bronze_staging_table",
        bash_command=f"dbt run -s d_bronze_staging_table --project-dir {DBT_DIR}",
    )

    e_bronze_bigquery_to_silver_gcs = BigQueryToGCSOperator(
        task_id="e_bronze_bigquery_to_silver_gcs",
        gcp_conn_id = 'gcs',
        source_project_dataset_table=f"{GCP_PROJECT_ID}.{DATASET_NAME}.{GCP_BQ_BRONZE_STAGING_TABLE}",
        destination_cloud_storage_uris=[f"gs://{GCP_GCS_BUCKET_NAME}/{SILVER_PREFIX}/{SILVER_BUCKET_FILE}.csv"],
    )

    d_bronze_staging_table >> e_bronze_bigquery_to_silver_gcs
