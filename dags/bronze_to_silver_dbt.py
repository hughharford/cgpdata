import os

from airflow import DAG

# $IMPORT_BEGIN
# noreorder
import pendulum

from airflow.operators.bash import BashOperator

# $IMPORT_END


DBT_DIR = os.getenv("DBT_DIR")


with DAG(
    "bronze_to_silver_dbt",
    # $CODE_BEGIN
    default_args={ "depends_on_past": False, },
    start_date=pendulum.today("UTC").add(days=-1),
    schedule_interval="@daily",
    catchup=True,
    # $CODE_END
) as dag:
    dbt_dag_test = BashOperator(
        task_id="dbt_dag_test",
        bash_command=f"dbt source freshness --project-dir {DBT_DIR}",
    )

    d_bronze_staging_table = BashOperator(
        task_id="d_bronze_staging_table",
        bash_command=f"dbt run -s d_bronze_staging_table --project-dir {DBT_DIR}",
    )

    # e_write_to_gcs_silver = BashOperator(
    #     task_id="e_write_to_gcs_silver",
    #     bash_command=f"dbt run -s d_create_bronze_staging_table --project-dir {DBT_DIR}",
    # )


    dbt_dag_test >> d_bronze_staging_table
    # >> e_write_to_gcs_silver
