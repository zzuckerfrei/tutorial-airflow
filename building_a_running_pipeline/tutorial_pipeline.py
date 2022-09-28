import datetime
import pendulum
import os
import logging
import requests

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models.variable import Variable


@dag(
    dag_id="process-employees",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def ProcessEmployees():
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
    def get_data():
        # NOTE: configure this as appropriate for your airflow environment
        # data_path = "/opt/airflow/dags/files/employees.csv"
        data_path = Variable.get("data_path")
        os.makedirs(os.path.dirname(data_path), exist_ok=True)

        # url = "https://raw.githubusercontent.com/apache/airflow/main/docs/apache-airflow/tutorial/pipeline_example.csv"
        url = Variable.get("url")

        logging.info("data_path is {}".format(data_path))
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

    @task
    def merge_data():
        # query = """
        #     INSERT INTO tutorial.employees
        #     SELECT *
        #     FROM (
        #         SELECT DISTINCT *
        #         FROM tutorial.employees_temp
        #     ) AS et
        #     ON CONFLICT ("Serial Number") DO UPDATE
        #     SET "Serial Number" = excluded."Serial Number";
        # """
        try:
            logging.info("now ...{}".format(os.path.dirname(os.path.realpath(__file__))))
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

    create_tutorial_schema >> [create_employees_table, create_employees_temp_table] >> get_data() >> merge_data()


dag = ProcessEmployees()
