import os

from airflow import DAG

# $IMPORT_BEGIN
# noreorder
import pendulum

from airflow.operators.bash import BashOperator

# $IMPORT_END


DBT_DIR = os.getenv("DBT_DIR")


with DAG(
    "bronze_dbt",
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

    a_external_table_trans = BashOperator(
        task_id="a_external_table_trans",
        bash_command=f"dbt run -s a_external_table_trans --project-dir {DBT_DIR}",
    )

    b_decimal_trans = BashOperator(
        task_id="b_decimal_trans",
        bash_command=f"dbt run -s b_decimal_trans --project-dir {DBT_DIR}",
    )

    c_silver_trans = BashOperator(
        task_id="c_silver_trans",
        bash_command=f"dbt run -s c_silver_trans --project-dir {DBT_DIR}",
    )


    dbt_dag_test >> a_external_table_trans >> b_decimal_trans >> c_silver_trans
