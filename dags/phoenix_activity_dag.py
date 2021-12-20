"""DAG that aggregates appstore revenue data.

"""
from airflow import DAG

from datetime import datetime, timedelta
from common.operators.vertica_operator import VerticaOperator
from common.dag_utils import is_airflow
from common.operators.vertica_data_sensor import VerticaDataSensor


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
    dag_id='etl_phoenix_activity',
    default_args=default_args,
    schedule_interval='30 8 * * *', # 4:00am EST, 8:00am UTC , 1:00 am PST
    catchup=True,
)


t_validate_client_events_phoenix = VerticaDataSensor(
    task_id='t_validate_client_events_phoenix',
    vertica_conn_id='vertica_conn',
    resourcepool='default',
    table_name='phoenix.events_client',
    time_dimension='event_time',
    metrics='count(1)',
    granularity='hour',
    days_offset=0,
    poke_interval=600,
    timeout=600,
    email=['airflow-sla-miss@gsngames.pagerduty.com', ],
    email_on_retry=False,
    email_on_failure=True,
    retries=0,
    dag=dag,
)


t_validate_device_sessions_phoenix = VerticaDataSensor(
    task_id='t_validate_device_sessions_phoenix',
    vertica_conn_id='vertica_conn',
    resourcepool='default',
    table_name='phoenix.device_sessions',
    time_dimension='start_ts',
    metrics='count(1)',
    granularity='hour',
    days_offset=0,
    poke_interval=600,
    timeout=600,
    email=['airflow-sla-miss@gsngames.pagerduty.com', ],
    email_on_retry=False,
    email_on_failure=True,
    retries=0,
    dag=dag,
)


t_update_device_day_phoenix = VerticaOperator(
    task_id='t_update_device_day_phoenix',
    vertica_conn_id='vertica_conn',
    resourcepool='default',
    sql='sql/t_update_device_day_phoenix.sql',
    dag=dag
)


t_update_devices_phoenix = VerticaOperator(
    task_id='t_update_devices_phoenix',
    vertica_conn_id='vertica_conn',
    resourcepool='default',
    sql='sql/t_update_devices_phoenix.sql',
    dag=dag
)

t_update_user_day = VerticaOperator(
    task_id='t_update_user_day',
    vertica_conn_id='vertica_conn',
    resourcepool='default',
    sql='sql/t_update_user_day.sql',
    dag=dag
)

t_validate_client_events_phoenix >> t_update_device_day_phoenix
t_validate_device_sessions_phoenix >> t_update_device_day_phoenix
t_update_device_day_phoenix >> t_update_devices_phoenix

t_validate_client_events_phoenix >> t_update_user_day
