"""DAG that aggregates appstore revenue data.

"""
from airflow import DAG

from common.operators.gsn_sensor import GsnSqlSensor
from datetime import datetime, timedelta
from common.operators.vertica_operator import VerticaOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 7, 27),
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
    dag_id='etl_phoenix_frequent_jobs',
    default_args=default_args,
    schedule_interval='10 * * * *',
    max_active_runs=1,
    catchup=False,
)

t_validate_client_events_phoenix = GsnSqlSensor(
    task_id='t_validate_client_events_phoenix',
    conn_id='vertica_conn',
    sql='sql/t_validate_client_events_phoenix.sql',
    resourcepool='AIRFLOW_SKILL_STD',
    sla=timedelta(hours=2),
    dag=dag
)

t_update_deposits_withdrawals_phoenix = VerticaOperator(
    task_id='t_update_deposits_withdrawals_phoenix',
    vertica_conn_id='vertica_conn',
    resourcepool='AIRFLOW_SKILL_STD',
    sql='sql/t_update_deposits_withdrawals_phoenix.sql',
    dag=dag
)

t_update_dim_device_mapping = VerticaOperator(
    task_id='t_update_dim_device_mapping',
    vertica_conn_id='vertica_conn',
    resourcepool='AIRFLOW_SKILL_STD',
    sql='sql/t_update_dim_device_mapping.sql',
    dag=dag
)

t_update_deposits_withdrawals_phoenix.set_upstream(t_validate_client_events_phoenix)
t_update_dim_device_mapping.set_upstream(t_validate_client_events_phoenix)
