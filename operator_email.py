from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from datetime import datetime


with DAG(
    dag_id='send_email',
    schedule_interval=None,
    start_date=datetime(2025, 6, 26),
    catchup=False,
) as dag:

    send_email = EmailOperator(
        task_id='send_email_task',
        to='ioksha.kate@gmail.com',
        subject='Письмо из Airflow',
        html_content='<h3>Привет! Это письмо отправлено через airflow.</h3>'
    )