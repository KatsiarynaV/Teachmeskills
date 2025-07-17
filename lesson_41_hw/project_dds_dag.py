from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow.utils.helpers import chain
import requests
from datetime import datetime
import pandas as pd
import logging
import os
from project_models import Product, Order, OrderItem

logger = logging.getLogger(__name__)

TELEGRAM_CHAT_ID = Variable.get("telegram_chat_id")
TELEGRAM_TOKEN = Variable.get("telegram_token")

@dag(
        dag_id='project_raw_to_dds',
        start_date=datetime(2025, 7, 10),
        schedule_interval='*/10 * * * *',
        tags=['dds'],
        description='Даг, который складывает данные из raw в dds слой',
        catchup=False
)

def raw_to_dds():
    @task()
    def extract_raw_data(table_name: str):
        
        pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        conn = pg_hook.get_conn()

        df = pd.read_sql(f"select * from raw.{table_name}", conn)

        return df
    
    @task
    def load_products(data_df):
        
        products_df = data_df[['product_id', 'product_name']].drop_duplicates().copy()

        products_list = products_df.to_dict(orient='records')

        valid_products = []
        for record in products_list:
            try:
                product = Product(**record)
                valid_products.append(product.model_dump())
            except Exception as e:
                logger.error(f"Невалидная запись товара {record}", e)

        products_valid_df = pd.DataFrame(valid_products)

        pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        engine = pg_hook.get_sqlalchemy_engine()

        if not products_valid_df.empty:
            products_valid_df['product_id'] = products_valid_df['product_id'].astype(str)
            existing_products = pd.read_sql('select product_id from dds.products', engine)
            existing_products['product_id'] = existing_products['product_id'].astype(str)
            existing_products_ids = set(existing_products['product_id'])
            products_valid_df = products_valid_df[~products_valid_df['product_id'].isin(existing_products_ids)]

            products_valid_df.to_sql('products', engine, schema='dds', if_exists='append', index=False)

    @task
    def load_orders(data_df):
        
        orders_df = data_df[['order_id', 'order_date']].drop_duplicates().copy()

        orders_list = orders_df.to_dict(orient='records')

        valid_orders = []
        for record in orders_list:
            try:
                order = Order(**record)
                valid_orders.append(order.model_dump())
            except Exception as e:
                logger.error(f"Невалидная запись заказа {record}", e)

        orders_valid_df = pd.DataFrame(valid_orders)

        pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        engine = pg_hook.get_sqlalchemy_engine()

        if not orders_valid_df.empty:
            orders_valid_df['order_id'] = orders_valid_df['order_id'].astype(str)
            existing_orders = pd.read_sql('select order_id from dds.orders', engine)
            existing_orders['order_id'] = existing_orders['order_id'].astype(str)
            existing_orders_ids = set(existing_orders['order_id'])
            orders_valid_df = orders_valid_df[~orders_valid_df['order_id'].isin(existing_orders_ids)]

            orders_valid_df.to_sql('orders', engine, schema='dds', if_exists='append', index=False)
        
    @task
    def load_order_items(data_df):
        
        order_items_df = data_df[['item_id', 'order_id', 'product_id', 'quantity', 'price', 'total_price']].drop_duplicates().copy()

        order_items_list = order_items_df.to_dict(orient='records')

        valid_order_items = []
        for record in order_items_list:
            try:
                order_item = OrderItem(**record)
                valid_order_items.append(order_item.model_dump())
            except Exception as e:
                logger.error(f"Невалидная запись товара в заказе {record}", e)

        order_items_valid_df = pd.DataFrame(valid_order_items)

        pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        engine = pg_hook.get_sqlalchemy_engine()

        if not order_items_valid_df.empty:
            order_items_valid_df['item_id'] = order_items_valid_df['item_id'].astype(str)
            existing_order_items = pd.read_sql('select item_id from dds.order_items', engine)
            existing_order_items['item_id'] = existing_order_items['item_id'].astype(str)
            existing_order_items_ids = set(existing_order_items['item_id'])
            order_items_valid_df = order_items_valid_df[~order_items_valid_df['item_id'].isin(existing_order_items_ids)]

            order_items_valid_df.to_sql('order_items', engine, schema='dds', if_exists='append', index=False)

    SQL_DIR = os.path.join(os.path.dirname(__file__), 'sql', 'project_dds')
    table_creation_order = [
        'products',
        'orders',
        'order_items'
    ]

    create_tasks = {}
    for table_name in table_creation_order:
        
        sql_file_path = os.path.join(SQL_DIR, f'create_{table_name}.sql')
        relative_sql_path = os.path.relpath(sql_file_path, os.path.dirname(__file__))

        create_table_task = PostgresOperator(
            task_id=f'create_table_{table_name}',
            postgres_conn_id='my_postgres_conn',
            sql=relative_sql_path
        )

        create_tasks[table_name] = create_table_task

    for i in range(1, len(table_creation_order)):
        create_tasks[table_creation_order[i-1]] >> create_tasks[table_creation_order[i]]

    @task
    def send_telegram_message():
        text = "Загрузка данных в dds завершёна успешно"
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        response = requests.post(url, data={'chat_id': TELEGRAM_CHAT_ID, 'text': text})
        response.raise_for_status()

    products_df = extract_raw_data("products")
    orders_df = extract_raw_data("orders")
    order_items_df = extract_raw_data("order_items")

    load_products_task = load_products(products_df)
    load_orders_task = load_orders(orders_df)
    load_order_items_task = load_order_items(order_items_df)

    telegram_task = send_telegram_message()

    chain(
        create_tasks['products'], load_products_task,
        create_tasks['orders'], load_orders_task,
        create_tasks['order_items'], load_order_items_task,
        telegram_task
    )

raw_to_dds()
