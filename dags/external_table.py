from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.utils.dates import days_ago
import os

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")

TABLE_SCHEMA = [
    {"name": "Date", "type": "DATE"},
    {"name": "Open", "type": "FLOAT"},
    {"name": "High", "type": "FLOAT"},
    {"name": "Low", "type": "FLOAT"},
    {"name": "Close", "type": "FLOAT"},
    {"name": "Adj Close", "type": "FLOAT"},
    {"name": "Volume", "type": "INTEGER"},
]

with DAG(
    "create_bigquery_external_hist",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
) as dag:

    create_external_table = BigQueryCreateExternalTableOperator(
        task_id="create_external_table",
        gcp_conn_id="google_cloud_default",
        table_resource={
            "tableReference": {
                "projectId": "condorgp-451516",
                "datasetId": "cgpdata_lwb",
                "tableId": "external_table"
            },
            "externalDataConfiguration": {
                "sourceUris": [f"gs://cgpdata/hist/*.csv"],
                "sourceFormat": "CSV",
                "csvOptions": {"skipLeadingRows": 1},
                "autodetect": False,
                "schema": TABLE_SCHEMA,
            },
        },
    )

    create_external_table
