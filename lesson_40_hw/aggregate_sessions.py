from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator



with DAG(
    'agg_sessions',
    description = 'Расчет статистики',
    schedule_interval = '@daily',
    start_date = datetime(2025, 7, 7),
    catchup=False,
    tags = ['raw'],
) as dag:
    
    wait_for_load_task = ExternalTaskSensor(
        task_id = 'wait_for_load',
        external_dag_id = 'extract_and_prepare_data',
        external_task_id = 'joined_data_task',
        execution_date_fn = lambda dt: dt,
        mode = 'reschedule',
        poke_interval = 30,
        timeout = 3600,
        allowed_states = ['success'],
        failed_states = ['failed', 'skipped']
    )


    aggregate_task = PostgresOperator(
        task_id = 'aggregate_task',
        postgres_conn_id = 'my_postgres_conn',
        sql = """
            insert into data_mart.session_summary(avg_duration_sec, avg_event_count)
            select 
            avg(duration_sec) as avg_duration,
            avg(event_count) as avg_count
            from temp.session_event_stats;
        """
    )

    trigger_dag = TriggerDagRunOperator(
        task_id = 'trigger_DAG3',
        trigger_dag_id = 'send_reports',
        execution_date = '{{execution_date}}',
        wait_for_completion = False,
        reset_dag_run = True,
        trigger_rule = 'all_success'
    )

    wait_for_load_task >> aggregate_task >> trigger_dag