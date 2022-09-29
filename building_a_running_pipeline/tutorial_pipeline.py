import datetime
import pendulum
import os
import logging
import requests

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable


@dag(
    dag_id="process-employees",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def ProcessEmployees():
    def get_date(**context):
        try:
            logging.info("author is : {}".format(context['params']['author']))
            logging.info("email is : {}".format(context['params']['e-mail']))
            date_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            return {"date_now": date_now}

        except Exception as e:
            logging.error(e)
            return 1

    get_date = PythonOperator(
        task_id="get_date",
        python_callable=get_date,
        params={
            'author': Variable.get("author"),
            'e-mail': Variable.get("e-mail")
        },
        provide_context=True,
        # dag="process-employees"  # dag를 명시하면 에러 발생, 이미 dag내에서 선언하는 task라서 그런건지
    )

    create_tutorial_schema = PostgresOperator(
        task_id="create_tutorial_schema",
        postgres_conn_id="tutorial_pg_conn",
        sql="sql/create_tutorial_schema.sql",
    )

    create_employees_table = PostgresOperator(
        task_id="create_employees_table",
        postgres_conn_id="tutorial_pg_conn",
        sql="sql/create_employees.sql",
    )

    create_employees_temp_table = PostgresOperator(
        task_id="create_employees_temp_table",
        postgres_conn_id="tutorial_pg_conn",
        sql="sql/create_employees_temp.sql",
    )

    @task
    def get_data(**context):
        try:
            xcom = context['task_instance'].xcom_pull(key="return_value", task_ids="get_date")  # xcom을 불러오는 방식
            logging.info("now is ... {}".format(xcom))
            logging.info(xcom['date_now'])

            # data_path = "/opt/airflow/dags/files/employees.csv"
            data_path = Variable.get("data_path")
            logging.info("data_path is {}".format(data_path))
            os.makedirs(os.path.dirname(data_path), exist_ok=True)

            # url = "https://raw.githubusercontent.com/apache/airflow/main/docs/apache-airflow/tutorial/pipeline_example.csv"
            url = Variable.get("url")
            logging.info("url is {}".format(url))

            response = requests.request("GET", url)

            with open(data_path, "w") as file:
                file.write(response.text)

            postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()

            with open(data_path, "r") as file:
                cur.copy_expert(
                    "COPY tutorial.employees_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                    file,
                )
            conn.commit()

            return 0

        except Exception as e:
            logging.error(e)
            return 1

    @task
    def merge_data(get_data_res: int, **context):
        try:
            logging.info("get_data_res is ... {}".format(get_data_res))  # 이전 task의 결과값을 param으로 받는 방식

            xcom = context['task_instance'].xcom_pull(key="return_value", task_ids="get_data")  # xcom으로 불러오는 방식
            logging.info("get_data result is ... {}".format(xcom))

            xcom = context['task_instance'].xcom_pull(key="return_value", task_ids="get_date")  # xcom으로 불러오는 방식
            logging.info("now is ... {}".format(xcom))
            logging.info(xcom['date_now'])

            logging.info("current directory ...{}".format(os.path.dirname(os.path.realpath(__file__))))
            with open(os.path.dirname(os.path.realpath(__file__)) + "/sql/insert_employees.sql", "r") as f:
                query = f.read()
            logging.info("query is ...{}".format(query))

            postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
            return 0

        except Exception as e:
            logging.error(e)
            return 1

    # taskflow간에는 xcom으로 데이터를 넘겨줄 필요가 없는 것 같음, 물론 xcom_pull을 사용해서 가져올 수는 있음
    get_data = get_data()  # taskflow
    merge_data = merge_data(get_data)  # taskflow
    # taskflow만으로 이루어져있다면, 아래처럼 flow순서를 잡아줄 필요도 없이 위에서 아래로 순차적으로 task들이 실행된다
    get_date >> create_tutorial_schema >> [create_employees_table, create_employees_temp_table] >> get_data >> merge_data


dag = ProcessEmployees()
