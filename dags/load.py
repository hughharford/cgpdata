import os
from datetime import datetime

from airflow import DAG

from airflow.sensors.external_task import ExternalTaskSensor

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")

with DAG(
    "load",
    default_args={"depends_on_past": True},
    start_date=datetime(2021, 6, 1),
    end_date=datetime(2021, 12, 31),
    schedule_interval="@monthly",
) as dag:
    date = "{{ ds[:7] }}"
    filtered_data_file = f"{AIRFLOW_HOME}/data/silver/yellow_tripdata_{date}.csv"
    gcp_bucket = f"hsth_de_airflow_taxi_silver"
    gold_dataset_name = 'de_airflow_taxi_gold'

    wait_for_transform_task = ExternalTaskSensor(
        task_id="transform_sensor",
        external_dag_id='transform',
        timeout=10*60,
        poke_interval=10,
        allowed_states=['success']
    )

    upload_local_file_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id="upload_local_file_to_gcs",
        src=filtered_data_file,
        dst="/",
        bucket=gcp_bucket,
        gcp_conn_id='google_cloud_connection'
    )

    create_dataset_task = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id=gold_dataset_name,
        gcp_conn_id='google_cloud_connection'
    )

    create_table_task = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        dataset_id=gold_dataset_name,
        table_id = 'trips',
        schema_fields=[{"name": "date", "type": "STRING", "mode": "NULLABLE"},
                       {"name": "trip_distance", "type": "FLOAT64", "mode": "NULLABLE"},
                       {"name": "total_amount", "type": "FLOAT64", "mode": "NULLABLE"}],
        gcp_conn_id='google_cloud_connection'
    )

    remove_existing_data_task = BigQueryInsertJobOperator(
        task_id="remove_existing_data",
        configuration={
            "query": {
                "query": f'''DELETE FROM {gold_dataset_name}.trips WHERE date = "{date}"''',
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        location="US",
        gcp_conn_id='google_cloud_connection'
    )

    load_to_bigquery_task = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket=,
        source_objects=['bigquery/us-states/us-states.csv'],
        destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
        schema_fields=[
            {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'post_abbr', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        write_disposition='WRITE_TRUNCATE'
    )

    # Organise your tasks hierachy here
    wait_for_transform_task >> upload_local_file_to_gcs_task
    upload_local_file_to_gcs_task >> create_dataset_task
    create_dataset_task >> create_table_task
    create_table_task >> remove_existing_data_task
