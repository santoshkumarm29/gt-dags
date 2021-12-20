import datetime

from airflow import DAG

from common.operators.vertica_operator import VerticaOperator


default_args = {
    'owner': 'UA',
    'depends_on_past': False,
    'email': ['etl@gsngames.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=10),
}

dag = DAG(
    dag_id='kochava_device_summary_backfill_000',
    schedule_interval='0 8 * * *',
    start_date=datetime.datetime(2021, 11, 22),
    default_args=default_args,
    catchup=True,
)

t_update_kochava_device_summary = VerticaOperator(
    task_id='t_update_kochava_device_summary',
    vertica_conn_id='vertica_conn',
    resourcepool='default',
    sql='sql/ua_attribution_tasks/t_update_kochava_device_summary.sql',
    dag=dag
)
