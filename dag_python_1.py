from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

def greet(ti):
    first_name=ti.xcom_pull(task_id='get_name', key='first_name')
    last_name=ti.xcom_pull(task_id='get_name', key='last_name')
    age=ti.xcom_pull(task_id="get_age", key="age")
    return f"Hi! My name is {first_name} {last_name} and I'm {age} years old."

def get_name(ti):
    ti.xcom_push(key='first_name', value="Surya")
    ti.xcom_push(key='last_name', value='Arikatla')

def get_age(ti):
    ti.xcom_push(key="age", value=25)

default_args={
    'owner':'surya',
    'retries':1,
    'retry_delay':timedelta(minutes=1)
}

with DAG(
    dag_id='dag_python',
    description="This id th epython dag",
    default_args=default_args,
    start_date=datetime.today(),
    schedule_interval=timedelta(days=1),
    catchup=False
) as dag:
    task1=PythonOperator(
        task_id='greet',
        python_callable=greet,
        # op_kwargs={'name':'Surya','age':25}
    )

    task2=PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )

    task3=PythonOperator(
        task_id='get_age',
        python_callable=get_age
    )

    task2 >> task1
    task3 >> task1
