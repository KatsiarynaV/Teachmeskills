from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import pandas as pd
import logging
import os
import glob
import requests


logger = logging.getLogger()
logger.setLevel('INFO')

DATA_DIR = '/opt/airflow/dags/input/'
PROCESSED_DIR = '/opt/airflow/dags/processed/'

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)


class FileSensorWithXCom(FileSensor):
    def poke(self, context):
        files = glob.glob(self.filepath)
        if files:
            context['ti'].xcom_push(key='file_path', value=files[0])
            return True
        return False


def load_data_to_postgres(**kwargs):
    table_name = kwargs['table_name']
    ti = kwargs['ti']
    
    file_path = ti.xcom_pull(task_ids=f"wait_for_{table_name}", key='file_path')

    try:
        df = pd.read_csv(file_path)

        pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        engine = pg_hook.get_sqlalchemy_engine()

        df.to_sql(
            table_name,
            engine,
            schema='raw',
            if_exists='append',
            index=False
        )

        logger.info(f'Файл {file_path} загружен')

        processed_path = os.path.join(PROCESSED_DIR, os.path.basename(file_path))
        os.rename(file_path, processed_path)
        logger.info(f"Файл перемещён в {processed_path}")

        ti.xcom_push(key='load_report', value=f'Таблица {table_name}: Загружено {len(df)} строк (файл {os.path.basename(file_path)})\n')

    except:
        logger.error(f'Ошибка при загрузке {file_path}')
        raise

def generate_telegram_report(**kwargs):
    ti = kwargs['ti']
    message = "Отчёт о загрузке данных в PostgreSQL:\n\n"
    has_content = False

    for table in ['users', 'orders', 'deliveries']:
        report = ti.xcom_pull(task_ids=f'load_{table}_data', key='load_report')
        if report:
            message += report + "\n"
            has_content = True

    if not has_content:
        message = "No data"

    logger.info(f"[DEBUG] Telegram message is generated {message}")
    return message


def send_telegram_message(**kwargs):
    ti = kwargs['ti']
    message = ti.xcom_pull(task_ids='generate_telegram_report')

    if not message:
        message = 'No message generated.'

    url = "https://api.telegram.org/bot7747274326:AAFU9ysq7CibYX2eCfJnEoF3DCgzJS0smno/sendMessage"
    payload = {
        "chat_id": "-4674431231",
        "text": message,
        "parse_mode": "HTML"
    }

    response = requests.post(url, json=payload)
    response.raise_for_status()

    logging.info(f"[TELEGRAM] Status: {response.status_code}, response: {response.text}")

with DAG(
    'load_data_to_postgres',
    description = 'Вставка данных в постгрес из файлов',
    schedule_interval = '* * * * *',
    start_date=datetime(2025, 6, 26),
    catchup=False,
    max_active_runs=1
) as dag:
    
    wait_for_users = FileSensorWithXCom(
        task_id = 'wait_for_users',
        fs_conn_id = 'fs_default',
        filepath = f'{DATA_DIR}users_*.csv',
        poke_interval = 30,
        timeout = 30 * 5
    )

    create_users_table = PostgresOperator(
        task_id = 'create_users_table',
        postgres_conn_id = 'my_postgres_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS raw.users (
                user_id TEXT,   
                name TEXT,      
                surname TEXT,   
                age INTEGER,       
                email TEXT,    
                phone TEXT ,     
                card_number TEXT
            );
        """
    )

    load_users_task = PythonOperator(
        task_id='load_users_data',
        python_callable=load_data_to_postgres,
        op_kwargs={"table_name": "users"},
        provide_context=True
    )

    wait_for_orders = FileSensorWithXCom(
        task_id = 'wait_for_orders',
        fs_conn_id = 'fs_default',
        filepath = f'{DATA_DIR}orders_*.csv',
        poke_interval = 30,
        timeout = 30 * 5
    )

    create_orders_table = PostgresOperator(
        task_id = 'create_orders_table',
        postgres_conn_id = 'my_postgres_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS raw.orders (
                order_id TEXT,   
                product TEXT,      
                quantity INTEGER,   
                price_per_unit INTEGER,       
                total_price INTEGER,    
                card_number TEXT,
                user_id TEXT
            );
        """
    )

    load_orders_task = PythonOperator(
        task_id='load_orders_data',
        python_callable=load_data_to_postgres,
        op_kwargs={"table_name": "orders"},
        provide_context=True
    )

    wait_for_deliveries = FileSensorWithXCom(
        task_id = 'wait_for_deliveries',
        fs_conn_id = 'fs_default',
        filepath = f'{DATA_DIR}deliveries_*.csv',
        poke_interval = 30,
        timeout = 30 * 5
    )

    create_deliveries_table = PostgresOperator(
        task_id = 'create_deliveries_table',
        postgres_conn_id = 'my_postgres_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS raw.deliveries (
                delivery_id TEXT,
                order_id TEXT,   
                product TEXT,   
                company TEXT,   
                cost INTEGER, 
                courier_name TEXT,
                courier_phone TEXT,
                start_time TIMESTAMP,       
                end_time TIMESTAMP,    
                city TEXT,
                warehouse TEXT,
                address TEXT
            );
        """
    )

    load_deliveries_task = PythonOperator(
        task_id='load_deliveries_data',
        python_callable=load_data_to_postgres,
        op_kwargs={"table_name": "deliveries"},
        provide_context=True
    )

    generate_telegram_message = PythonOperator(
        task_id = 'generate_telegram_report',
        python_callable = generate_telegram_report,
        provide_context=True
    )

    send_report = PythonOperator(
        task_id='send_tg_report',
        python_callable=send_telegram_message,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE
    )

wait_for_users >> create_users_table >> load_users_task

wait_for_orders >> create_orders_table >> load_orders_task

wait_for_deliveries >> create_deliveries_table >> load_deliveries_task

[load_users_task, load_orders_task, load_deliveries_task] >> generate_telegram_message >> send_report