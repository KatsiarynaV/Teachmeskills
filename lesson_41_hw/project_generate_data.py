from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from faker import Faker
from airflow.models import Variable
import requests
import random
import uuid

fake = Faker()

TELEGRAM_CHAT_ID = Variable.get("telegram_chat_id")
TELEGRAM_TOKEN = Variable.get("telegram_token")

PRODUCTS = ['мобильный телефон', 'компьютер', 'монитор', 'системный блок', 'планшет', 'умные часы', 'электронная книга', 'клавиатура', 'мышь', 'наушники']

def insert_data_to_postgres():

    hook = PostgresHook(postgres_conn_id='my_postgres_conn')
    conn = hook.get_conn()
    cursor = conn.cursor()

    products = []
    orders = []
    order_items = []

    num_products = random.randint(5, min(10, len(PRODUCTS))) 
    unique_product_names = random.sample(PRODUCTS, num_products)

    for product_name in unique_product_names:
        product_id = str(uuid.uuid4())
        products.append((product_id, product_name))


        for _ in range(random.randint(5, 20)):
            order_id = str(uuid.uuid4())
            order_date = fake.date_between(start_date='-7d', end_date='today')

            orders.append((order_id, order_date))

            for _ in range(random.randint(5, 20)):
                item_id = str(uuid.uuid4())
                quantity = random.randint(1, 5)
                price = round(random.uniform(10, 300), 2)
                total_price = round(quantity * price, 2)

                order_items.append((item_id, order_id, product_id, quantity, price, total_price))

    cursor.executemany(
        "INSERT INTO raw.products (product_id, product_name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
        products
    )
    cursor.executemany(
        "INSERT INTO raw.orders (order_id, order_date) VALUES (%s, %s) ON CONFLICT DO NOTHING",
        orders
    )
    cursor.executemany(
        """INSERT INTO raw.order_items (item_id, order_id, product_id, quantity, price, total_price) VALUES (%s, %s, %s, %s, %s, %s)""",
        order_items
    )

    conn.commit()
    cursor.close()
    conn.close()


def send_telegram_message():
    text = "Вставка данных завершёна успешно"
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    response = requests.post(url, data={'chat_id': TELEGRAM_CHAT_ID, 'text': text})
    response.raise_for_status()

with DAG(
    'project_insert_raw_data',
    description = "Генерация и вставка данных о продуктак и заказах",
    schedule_interval = '*/5 * * * *',
    start_date = datetime(2025, 6, 9),
    catchup = False,
    tags=['raw']
) as dag:
    
    insert_raw_data_task = PythonOperator(
        task_id = 'insert_raw_data',
        python_callable = insert_data_to_postgres
    ) 

    send_telegram_report = PythonOperator(
        task_id='send_tg_report',
        python_callable=send_telegram_message,
        provide_context=True,
    )

    insert_raw_data_task >> send_telegram_report