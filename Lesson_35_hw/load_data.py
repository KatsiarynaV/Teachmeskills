from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from etl_functions import read_xlsx_and_load_to_postgres, read_orders_and_load_to_postgres, read_deliveries_and_load_to_postgres, get_users_total_orders_price

default_args = {
    'owner': "kate",
}


with DAG(
    'import_data_from_excel',
    default_args = default_args,
    description = 'Интеграция данных из xlsx',
    schedule_interval = None,
    start_date=datetime(2025, 6, 1),
) as dag:
    d = DummyOperator(task_id="dummy")

    read_and_load_task = PythonOperator(
        task_id = 'read_and_load_users',
        python_callable=read_xlsx_and_load_to_postgres
    )

    read_and_load_orders_task = PythonOperator(
        task_id = 'read_and_load_orders',
        python_callable=read_orders_and_load_to_postgres
    )

    read_and_load_deliveries_task = PythonOperator(
    task_id = 'read_and_load_deliveries',
    python_callable=read_deliveries_and_load_to_postgres
    )

    get_data_from_sql_task = PythonOperator(
        task_id = 'get_data',
        python_callable=get_users_total_orders_price
    )

    d >> [read_and_load_task, read_and_load_orders_task, read_and_load_deliveries_task] >> get_data_from_sql_task


