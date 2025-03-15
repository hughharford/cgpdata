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
    {"name": "Volume", "type": "INTEGER"},
    {"name": "Ticker", "type": "STRING"},
]

with DAG(
    "create_bigquery_external_hist_silver",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
) as dag:

    create_bigquery_external_hist_silver = BigQueryCreateExternalTableOperator(
        task_id="create_bigquery_external_hist_silver",
        gcp_conn_id="gcs",
        table_resource={
            "tableReference": {
                "projectId": "condorgp-451516",
                "datasetId": "cgpdata_lwb",
                "tableId": "external_table_silver"
            },
            "externalDataConfiguration": {
                "sourceUris": [f"gs://cgpdata/silver/*.csv"],
                "sourceFormat": "CSV",
                "csvOptions": {"skipLeadingRows": 1},
                "autodetect": False,
                "schema": TABLE_SCHEMA,
            },
        },
    )

    create_bigquery_external_hist_silver
