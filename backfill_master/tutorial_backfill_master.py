import datetime
import logging
import requests
from time import sleep
import pendulum
import random

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models.variable import Variable
from airflow.exceptions import AirflowTaskTimeout


@dag(
    dag_id="backfill_master",
    catchup=True,
    # dagrun_timeout=datetime.timedelta(seconds=5),
    schedule_interval="0 0/5 * * *",  # 5시간마다 실행 0시, 5시, 10시, 15시, 20시
    start_date=pendulum.datetime(2022, 9, 29, tz="UTC"),
)
def BackfillMaster():
    # 1~10초 사이 랜덤으로 텀을 두게 만들고
    # 5초 이상이면 task 종료
    # 현재 날짜 시간 얻기 : PythonOperator
    @task.python(
        task_id="get_datetime",
        execution_timeout=datetime.timedelta(seconds=5),
    )
    def get_datetime():
        try:
            sleep_time = random.randrange(1, 11)
            logging.info("sleep_time is ... {}".format(sleep_time))

            sleep(sleep_time)

            datetime_now = datetime.datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
            logging.info("datetime_now is ... {}".format(datetime_now))

            return {"datetime_now": datetime_now}

        except Exception as e:
            logging.info(e)
            raise AirflowTaskTimeout("time out!!! {} seconds".format(sleep_time))

    # url에서 파일 받아오고, 파일이름을 받아온 날짜로 설정하고 저장함 : BashOperator -> PythonOperator
    @task
    def get_file(datetime_now: dict):
        try:
            logging.info("datetime_now is ... {}".format(datetime_now["datetime_now"]))
            url = Variable.get("url")
            file_path = "{}/{}.csv".format(Variable.get("base_file_path"), datetime_now["datetime_now"])
            logging.info("file_path is ... {}".format(file_path))

            response = requests.request("GET", url)
            with open(file_path, "w") as file:
                file.write(response.text)

            return 0

        except Exception as e:
            logging.info(e)
            return 1

    # dag실행 제한 시간은 5초로 설정해서 랜덤으로 5초 이상 대기시간이 소요되면 자동으로 종료됨
    # catchup 설정해놓아서 다음 스케줄에 이전에 실패한 작업을 한 번 더 시도하도록 설정함

    # get_random = get_random()
    get_datetime = get_datetime()
    get_file = get_file(get_datetime)

    # get_random >> get_datetime >> get_file
    get_datetime >> get_file


dag = BackfillMaster()
