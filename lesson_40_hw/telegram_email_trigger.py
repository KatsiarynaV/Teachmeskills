from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.email import EmailOperator
import requests
from airflow.models import Variable

TELEGRAM_CHAT_ID = Variable.get("telegram_chat_id")
TELEGRAM_TOKEN = Variable.get("telegram_token")

def send_telegram_message():
    text = "Расчёт статистики завершён успешно"
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    response = requests.post(url, data={'chat_id': TELEGRAM_CHAT_ID, 'text': text})
    response.raise_for_status()

with DAG(
    'send_reports',
    description = 'Отправка уведомлений на почту и в телеграмм',
    schedule_interval = '@daily',
    start_date = datetime(2025, 7, 7),
    catchup=False,
) as dag:
    
    wait_for_aggregate_task = ExternalTaskSensor(
        task_id = 'wait_for_aggregate',
        external_dag_id = 'agg_sessions',
        external_task_id = 'aggregate_task',
        execution_date_fn = lambda dt: dt,
        mode = 'reschedule',
        poke_interval = 30,
        timeout = 3600,
        allowed_states = ['success'],
        failed_states = ['failed', 'skipped']
    )


    send_telegram_report = PythonOperator(
        task_id='send_tg_report',
        python_callable=send_telegram_message,
        provide_context=True,
    )

    send_email_report = EmailOperator(
        task_id='send_email',
        to='ioksha.kate@gmail.com',
        subject='Airflow DAG Успешно выполнен',
        html_content='<h3>Расчёт статистики завершён успешно</h3>'
    )

    wait_for_aggregate_task >> send_telegram_report >> send_email_report


    