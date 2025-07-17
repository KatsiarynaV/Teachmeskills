from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import os
from airflow.models import Variable
import requests

TELEGRAM_CHAT_ID = Variable.get("telegram_chat_id")
TELEGRAM_TOKEN = Variable.get("telegram_token")


@dag(
    dag_id='project_dds_to_marts',
    start_date=datetime(2025, 7, 10),
    schedule_interval='@daily',
    catchup=False,
    tags=['data_marts'],
    description='Даг, который создаёт витрины на основе dds'
)
def dds_to_marts():
    
    SQL_DIR = os.path.join(os.path.dirname(__file__), 'sql', 'project_dds')

    with TaskGroup('dm_product_popularity_group') as dm_product_popularity_group:

        create_table = PostgresOperator(
            task_id='create_dm_product_popularity',
            postgres_conn_id='my_postgres_conn',
            sql='sql/project_dds/create_dm_product_popularity.sql'
        )

        insert_data = PostgresOperator(
            task_id='insert_dm_product_popularity',
            postgres_conn_id='my_postgres_conn',
            sql='sql/project_dds/insert_dm_product_popularity.sql'
        )

        create_table >> insert_data

    @task
    def send_telegram_message():
        text = "Загрузка данных в data_mart завершёна успешно"
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        response = requests.post(url, data={'chat_id': TELEGRAM_CHAT_ID, 'text': text})
        response.raise_for_status()

    telegram_task = send_telegram_message()

    dm_product_popularity_group >> telegram_task

dds_to_marts()