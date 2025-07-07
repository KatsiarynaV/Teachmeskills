from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import pandas as pd
import logging
import os
from models import User, Card, Product, Order


logger = logging.getLogger(__name__)

@dag(
        dag_id='build_dds_layer_potgres',
        start_date=datetime(2025, 6, 28),
        schedule_interval='0 19 * * *',
        tags=['dds'],
        description='Даг, который складывает данные из raw в dds слой',
        catchup=True
)
def build_dds_layer():
    @task()
    def extract_raw_data(table_name: str):
        """ Чтение данных из raw-слоя """
        pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        conn = pg_hook.get_conn()

        df = pd.read_sql(f"select * from raw.{table_name}", conn)

        return df
    
    @task
    def transform_and_load_users(data_df):
        # users
        users_df = data_df[['user_id', 'name', 'surname', 'age', 'email', 'phone']].drop_duplicates().copy()

        users_list = users_df.to_dict(orient='records')
        logger.info(users_list)

        # Провалидировали
        valid_users = []
        for record in users_list:
            try:
                user = User(**record)
                valid_users.append(user.model_dump())
            except Exception as e:
                logger.error(f"Невалидная запись пользователя {record}", e)

        users_valid_df = pd.DataFrame(valid_users)

        # Создаем соединение
        pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        engine = pg_hook.get_sqlalchemy_engine()

        # убираем те, что уже были
        if not users_valid_df.empty:
            existing_users = pd.read_sql('select user_id from dds.users', engine)
            existing_users_ids = set(existing_users['user_id'])
            users_valid_df = users_valid_df[~users_valid_df['user_id'].isin(existing_users_ids)]

        # Записываем результаты
        users_valid_df.to_sql('users', engine, schema='dds', if_exists='append', index=False)

        logger.info(f'кол-во строчек вставлено - {users_valid_df.shape[0]}')


        # === cards ===

        cards_df = data_df[['card_number', 'user_id']].drop_duplicates().copy()
        cards_list = cards_df.to_dict(orient='records')
        logger.info(cards_list)

        # Провалидировали
        valid_cards = []
        for record in cards_list:
            try:
                card = Card(**record)
                valid_cards.append(card.model_dump())
            except Exception as e:
                logger.error(f"Невалидная запись карты {record}", e)

        cards_valid_df = pd.DataFrame(valid_cards)

        # убираем те, что уже были
        if not cards_valid_df.empty:
            existing_cards = pd.read_sql('select card_number from dds.cards', engine)
            existing_cards_numbers = set(existing_cards['card_number'])
            cards_valid_df = cards_valid_df[~cards_valid_df['card_number'].isin(existing_cards_numbers)]

        # Записываем результаты
        cards_valid_df.to_sql('cards', engine, schema='dds', if_exists='append', index=False)

        logger.info(f'кол-во строчек вставлено - {cards_valid_df.shape[0]}')

    @task
    def transform_and_load_orders(data_df):
        # products
        products_df = data_df[['product']].drop_duplicates().copy()
        products_df.rename(columns={'product': 'product_name'}, inplace=True)
        products_df['product_id'] = range(1, len(products_df) + 1)

        products_list = products_df.to_dict(orient='records')
        logger.info(products_list)

        # Провалидировали
        valid_products = []
        for record in products_list:
            try:
                product = Product(**record)
                valid_products.append(product.model_dump())
            except Exception as e:
                logger.error(f"Невалидная запись ппродукта {record}", e)

        products_valid_df = pd.DataFrame(valid_products)

        # Создаем соединение
        pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        engine = pg_hook.get_sqlalchemy_engine()

        # убираем те, что уже были
        if not products_valid_df.empty:
            existing_products = pd.read_sql('select product_name from dds.products', engine)
            existing_products_ids = set(existing_products['product_name'])
            products_valid_df = products_valid_df[~products_valid_df['product_name'].isin(existing_products_ids)]

        # Записываем результаты
        products_valid_df.to_sql('products', engine, schema='dds', if_exists='append', index=False)

        logger.info(f'кол-во строчек вставлено - {products_valid_df.shape[0]}')

        # orders
        product_map = dict(zip(products_df['product_name'], products_df['product_id']))
        orders_df = data_df[['order_id', 'quantity', 'price_per_unit', 'total_price', 'card_number', 'user_id', 'product']].drop_duplicates().copy()
        orders_df['product_id'] = orders_df['product'].map(product_map)
        orders_df.drop(columns=['product'], inplace=True)

        orders_list = orders_df.to_dict(orient='records')
        logger.info(orders_list)

        # Провалидировали
        valid_orders = []
        for record in orders_list:
            try:
                order = Order(**record)
                valid_orders.append(order.model_dump())
            except Exception as e:
                logger.error(f"Невалидная запись заказа {record}", e)

        orders_valid_df = pd.DataFrame(valid_orders)

        # убираем те, что уже были
        if not orders_valid_df.empty:
            existing_orders = pd.read_sql('select order_id from dds.orders', engine)
            existing_orders_ids = set(existing_orders['order_id'])
            orders_valid_df = orders_valid_df[~orders_valid_df['order_id'].isin(existing_orders_ids)]

        # Записываем результаты
        orders_valid_df.to_sql('orders', engine, schema='dds', if_exists='append', index=False)

        logger.info(f'кол-во строчек вставлено - {orders_valid_df.shape[0]}')       

    # Создание таблиц в dds слое
    SQL_DIR = os.path.join(os.path.dirname(__file__), 'sql', 'dds')
    table_creation_order = [
        'users',
        'products',
        'delivery_companies',
        'couriers',
        'warehouses',
        'orders',
        'deliveries',
        'cards'
    ]

    prev_task = None
    for table_name in table_creation_order:
        # Найти путь к файлику
        sql_file_path = os.path.join(SQL_DIR, f'create_{table_name}.sql')
        relative_sql_path = os.path.relpath(sql_file_path, os.path.dirname(__file__))

        # Созданиие оператора создания таблички
        create_table_task = PostgresOperator(
            task_id=f'create_table_{table_name}',
            postgres_conn_id='my_postgres_conn',
            sql=relative_sql_path
        )

        # Логика запуска
        if prev_task: 
            prev_task >> create_table_task

        prev_task = create_table_task 

    df_users = extract_raw_data('users')
    transform_and_load_users(df_users)

    df_orders = extract_raw_data('orders')
    transform_and_load_orders(df_orders)

build_dds_layer()