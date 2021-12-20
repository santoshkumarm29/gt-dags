"""daily casino jobs run by vaggs ported to airflow

"""
from airflow import DAG
from datetime import datetime, timedelta
from common.operators.vertica_operator import VerticaOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 11, 21),
    'email': ['etl@gsngames.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='etl_ww_events_web_sessions',
    default_args=default_args,
    schedule_interval='10 9 * * *',
    concurrency=1,
    max_active_runs=1,
    catchup=False,
)

t_update_ww_events_web_sessions = VerticaOperator(
    task_id='t_update_ww_events_web_sessions',
    vertica_conn_id='vertica_conn',
    sql='sql/t_update_ww_events_web_sessions.sql',
    resourcepool='default',
    dag=dag
)
