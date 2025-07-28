from airflow import DAG
from neo4j import GraphDatabase
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta



default_args = {
    'depands_on_past': False,
    'start_date': datetime(2025, 7, 13),
}


def transfer_from_neo4j_to_postgres():
    uri = 'neo4j://neo4j:7687'
    user = Variable.get("NEO4J_USER")
    password = Variable.get("NEO4J_PASSWORD")

    driver = GraphDatabase.driver(uri, auth=(user, password))

    query = """
        MATCH (c:Customer)-[:MADE]->(t:Transaction)-[:PROCESSED_BY]->(b:Bank)
    RETURN
        t.transaction_id AS transaction_id,
        c.account_number AS account_number,
        c.name AS customer_name,
        t.type AS transaction_type,
        t.amount AS amount,
        t.currency AS currency,
        t.date AS transaction_date,
        b.name AS bank_name,
        b.branch_code AS branch_code,
        t.status AS status,
        t.created_at AS created_at
    """

    with driver.session() as session:
        results = session.run(query)
        data = [dict(row) for row in results]

    driver.close()

    if data:
        pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        for record in data:
            created_at = str(record['created_at'])
            transaction_date = str(record['transaction_date'])
            cursor.execute("""
                INSERT INTO raw.bank_transactions_neo4j VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (transaction_id) DO NOTHING
            """, (
                record['transaction_id'],
                record['account_number'],
                record['customer_name'],
                record['transaction_type'],
                record['amount'],
                record['currency'],
                transaction_date,
                record['bank_name'],
                record['branch_code'],
                record['status'],
                created_at
            ))

        conn.commit()
        cursor.close()
        conn.close()


with DAG(
    'NEO4J_integrate_date',
    default_args=default_args,
    schedule_interval='*/5 * * * *',
    catchup=False
) as dag:
    transfer_from_neo4j = PythonOperator(
        task_id='transfer_from_neo4j',
        python_callable=transfer_from_neo4j_to_postgres
    )


    transfer_from_neo4j