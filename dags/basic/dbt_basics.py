import os

from airflow import DAG

# $IMPORT_BEGIN
# noreorder
import pendulum

from airflow.operators.bash import BashOperator

# $IMPORT_END


DBT_DIR = os.getenv("DBT_DIR")


with DAG(
    "dbt_basics",
    # $CODE_BEGIN
    default_args={ "depends_on_past": False, },
    start_date=pendulum.today("UTC").add(days=-1),
    schedule_interval="@daily",
    catchup=True,
    # $CODE_END
) as dag:
    dbt_dag_test = BashOperator(
        task_id="dbt_dag_test",
        bash_command=f"echo {DBT_DIR}",
    )

    dbt_dag_test_2 = BashOperator(
        task_id="dbt_dag_test_2",
        bash_command=f"dbt --version",
    )

    dbt_dag_test_3 = BashOperator(
        task_id="dbt_dag_test_3",
        bash_command=f"pwd",
    )

    # $CHA_BEGIN
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --project-dir {DBT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --project-dir {DBT_DIR}",
    )

    dbt_dag_test >> dbt_dag_test_2 >> dbt_dag_test_3 >> dbt_run >> dbt_test
    # $CHA_END
