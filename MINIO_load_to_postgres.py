from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import random
import os

def get_and_load():
    hook = S3Hook(aws_conn_id='minio_default')
    bucket_name = 'data-bucket'
    minio_client=hook.get_conn()

    pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT filekey FROM raw.minio_processed_files")
    processed = set(row[0] for row in cursor.fetchall())

    response=minio_client.list_objects_v2(Bucket=bucket_name, Prefix='raw/')

    for obj in response['Contents']:
        filekey=obj['Key']
        if filekey in processed:
            continue 
        obj_data=minio_client.get_object(Bucket=bucket_name, Key=filekey)
        df = pd.read_csv(obj_data['Body'])
        engine = pg_hook.get_sqlalchemy_engine()
        df.to_sql('minio_data', engine, schema = 'raw', if_exists='append', index=False)

        cursor.execute("""
            INSERT INTO raw.minio_processed_files (filekey)
            VALUES (%s)
            ON CONFLICT DO NOTHING
        """, (filekey,))
        conn.commit()

    cursor.close()
    conn.close()

with DAG(
    'MINIO_load_to_postgres',
    start_date = datetime(2025, 7, 15),
    schedule_interval='@hourly',
    catchup=False
) as dag:
    transfer_task = PythonOperator(
        task_id='transfer_task',
        python_callable=get_and_load
    )

    transfer_task