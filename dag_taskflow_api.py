from datetime import datetime, timedelta

from airflow.decorators import dag,task

default_args={
    'owner':'Surya',
    'retries':1,
    'retry_delay':timedelta(minutes=1)
}

@dag(
    description="Dag created using taskflow api",
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime.today(),
    catchup=False
)
def hello_world():

    @task(multiple_outputs=True)
    def get_name():
        return {'first_name':'Surya',
                'last_name':'Arikatla'}

    @task()
    def get_age():
        return 25

    @task()
    def greet(name, age):
        print(f"Hi! My name is {name['first_name']} {name['last_name']} and I'm {age} years old.")

    name = get_name()
    age = get_age()
    greet(name=name,age=age)

greet_dag = hello_world()