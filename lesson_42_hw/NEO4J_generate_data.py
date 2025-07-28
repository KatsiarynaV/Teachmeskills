from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from neo4j import GraphDatabase
from faker import Faker
import random
from datetime import datetime


default_args = {
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 13),
}


def generate_bank_data():
    fake = Faker()
    data = []    

    for _ in range(100):
        record = {
            'transaction_id': fake.uuid4(),
            'account_number': fake.bban(),
            'customer_name': fake.name(),
            'transaction_type': random.choice(['deposit', 'transfer', 'payment',]),
            'amount': round(random.uniform(10, 10000), 2),
            'currency': random.choice(['USD', 'EUR', 'RUB',]),
            'transaction_date': fake.date_time_this_month(),
            'bank_name': fake.company(),
            'branch_code': fake.swift11(),
            'status': random.choice(['completed', 'pending', 'failed',]),
            'created_at': datetime.now(),
        }

        data.append(record)

    return data


def insert_to_neo4j(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='generate_data')

    uri = 'neo4j://neo4j:7687'
    user = Variable.get("NEO4J_USER")
    password = Variable.get("NEO4J_PASSWORD")

    driver = GraphDatabase.driver(uri, auth=(user, password))

    def insert_tx(tx, record):
        tx.run("""
            MERGE (c:Customer {account_number: $account_number})
              ON CREATE SET c.name = $customer_name

            MERGE (b:Bank {branch_code: $branch_code})
              ON CREATE SET b.name = $bank_name

            CREATE (t:Transaction {
              transaction_id: $transaction_id,
              type: $transaction_type,
              amount: $amount,
              currency: $currency,
              date: $transaction_date,
              status: $status,
              created_at: $created_at
            })

            MERGE (c)-[:MADE]->(t)
            MERGE (t)-[:PROCESSED_BY]->(b)
        """, record)

    with driver.session() as session:
        for record in data:
            session.execute_write(insert_tx, record)

    driver.close()


with DAG(
    'NEO4J_generate_bank_data',
    default_args=default_args,
    schedule_interval='*/5 * * * *',
    catchup=False
) as dag:
    generate_data = PythonOperator(
        task_id='generate_data',
        python_callable=generate_bank_data
    )

    load_to_neo4j = PythonOperator(
        task_id='load_to_neo4j',
        python_callable=insert_to_neo4j
    )

    generate_data >> load_to_neo4j