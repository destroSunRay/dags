from datetime import datetime, timedelta

from airflow.decorators import dag,task
import time

default_args={
    'owner':'surya',
    'retries':1,
    'retry_delay':timedelta(seconds=30)
}

@dag(
    description = "dag to read the recent anime form anime-watch website",
    default_args = default_args,
    catchup = False,
    start_date = datetime.strptime('2023-10-25','%Y-%m-%d'),
    schedule_interval = "*/30 * * * *"
)
def test():

    @task()
    def sleep():
        time.sleep(15)

    @task()
    def run():
        sleep()


    for i in range(15):
        run()

test()
