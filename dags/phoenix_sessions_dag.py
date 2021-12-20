"""DAG that aggregates appstore revenue data.

"""
from airflow import DAG

from common.operators.gsn_sensor import GsnSqlSensor
from datetime import datetime, timedelta
from common.operators.vertica_operator import VerticaOperator
from common.dag_utils import is_airflow


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 11, 21) if is_airflow() else datetime(2021, 11, 21),
    'email': ['etl@gsngames.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    dag_id='etl_phoenix_sessions',
    default_args=default_args,
    schedule_interval='10 * * * *', # 4:00am EST, 8:00am UTC , 1:00 am PST
    catchup=True,
)


t_update_device_sessions_phoenix = VerticaOperator(
    task_id='t_update_device_sessions_phoenix',
    vertica_conn_id='vertica_conn',
    resourcepool='default',
    sql='sql/t_update_device_sessions_phoenix.sql',
    dag=dag
)

