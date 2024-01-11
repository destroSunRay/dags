from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner':'Surya',
    'retries':1,
    'retry_delay':timedelta(minutes=1)
}

with DAG(
    dag_id='my_first_dag_v2',
    description="A simple tutorial DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    first_task=BashOperator(
        task_id='first_task',
        bash_command="echo 'Hello World! This is my first dag.'"
    )

    second_task=BashOperator(
        task_id="second_task",
        bash_command="echo 'The second task is executed after the completion of ditrst task'"
    )

    first_task >> second_task