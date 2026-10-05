from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime


with DAG(
    dag_id="user_data_pipeline",
    start_date=datetime(2026, 10, 3),
    schedule=None,
    catchup=False,
) as dag:

    load_users = BashOperator(
        task_id="load_users",
        bash_command="python /opt/airflow/scripts/load_users.py",
    )