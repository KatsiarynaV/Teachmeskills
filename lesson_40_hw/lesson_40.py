from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import os
import pandas as pd
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


def extract_session_data(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
    engine = pg_hook.get_sqlalchemy_engine()

    data = pd.read_sql('select * from raw.sessions', engine)
    print(data)
    kwargs['ti'].xcom_push(key='sessions_data', value=data) 

def extract_events_data(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
    engine = pg_hook.get_sqlalchemy_engine()

    data = pd.read_sql('select * from raw.events', engine)
    print(data)
    kwargs['ti'].xcom_push(key='events_data', value=data)

def merge_data(**kwargs):
    sessions = kwargs['ti'].xcom_pull(key='sessions_data', task_ids='data_extraction.extract_sessions_data')
    events = kwargs['ti'].xcom_pull(key='events_data', task_ids='data_extraction.extract_events_data')

    print(sessions)
    print(events)

    sessions = sessions.values.tolist()
    events = events.values.tolist()

    session_dict = {}
    for sess in sessions:
        session_id = sess[0]
        session_dict[session_id] = {
            'duration_sec': sess[5],
            'event_count': 0,
            'session_date': sess[2]
        }

    for event in events:
        session_id = event[1]
        if session_id in session_dict:
            session_dict[session_id]['event_count'] += 1

    result = [
        (sess_id, data['duration_sec'], data['event_count'])
        for sess_id, data in session_dict.items()
    ]

    print(result)
    kwargs['ti'].xcom_push(key='joined_data', value=result)


def check_nonzero_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key='joined_data', task_ids='merge_sessions_data')

    filtered = [row for row in data if row[1] > 0 and row[2] > 0]
    ti.xcom_push(key='filtered_data', value=filtered)


def remove_duplicates(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key='filtered_data', task_ids='data_validation.check_nonzero_task')

    seen = set()
    unique_data = []
    for row in data:
        if row[0] not in seen:
            unique_data.append(row)
            seen.add(row[0])

    ti.xcom_push(key='clean_data', value=unique_data)


def load_to_temp_table(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key='clean_data', task_ids='data_validation.remove_duplicates_task')

    hook = PostgresHook(postgres_conn_id='my_postgres_conn')
    hook.run("DELETE FROM temp.session_event_stats", autocommit=True)

    for row in data:
        hook.run(f"""
            INSERT INTO temp.session_event_stats VALUES (
                '{row[0]}', '{row[1]}', '{row[2]}'
            )
        """, autocommit=True)


with DAG(
    'extract_and_prepare_data',
    description = 'Извлечение данных',
    schedule_interval = '@daily',
    start_date = datetime(2025, 7, 7),
    catchup=False,
    tags = ['raw'],
) as dag:
    
    with TaskGroup('data_extraction') as extraction_group:
    
        extract_data_sessions_task = PythonOperator(
            task_id = 'extract_sessions_data',
            python_callable = extract_session_data
        )

        extract_data_events_task = PythonOperator(
        task_id = 'extract_events_data',
        python_callable = extract_events_data
        )

    extract_data_sessions_task >> extract_data_events_task

    merge_data_task = PythonOperator(
        task_id = 'merge_sessions_data',
        python_callable = merge_data
    )

    with TaskGroup('data_validation') as validation_group:
    
        check_nonzero_task = PythonOperator(
            task_id = 'check_nonzero_task',
            python_callable = check_nonzero_data
        )

        remove_duplicates_task = PythonOperator(
            task_id = 'remove_duplicates_task',
            python_callable = remove_duplicates
        )

    check_nonzero_task >> remove_duplicates_task

    joined_data_task = PythonOperator(
    task_id = 'joined_data_task',
    python_callable = load_to_temp_table,
    provide_context = True
)

    trigger_dag = TriggerDagRunOperator(
        task_id = 'trigger_DAG2',
        trigger_dag_id = 'agg_sessions',
        execution_date = '{{execution_date}}',
        wait_for_completion = False,
        reset_dag_run = True,
        trigger_rule = 'all_success'
    )

    extraction_group >> merge_data_task >> validation_group >> joined_data_task >> trigger_dag

