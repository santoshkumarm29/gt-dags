"""Airflow DAG for the GSN daily KPI email.

"""

from airflow import DAG
from datetime import datetime, timedelta, time
from common.dag_utils import is_airflow
from airflow.operators.python import PythonOperator
from common.operators.gsn_sensor import GsnSqlSensor

from common.operators.vertica_operator import VerticaOperator

# how many days to backfill on catchup run
PARAMS = dict(
    days_back=7,
    update_job='scheduled backfill'
)

default_args = {
    'owner': 'airflow',
    # 'depends_on_past': True,
    # 'wait_for_downstream': True,
    'start_date': datetime(2020, 9, 21) if is_airflow() else datetime(2020, 1, 24),
    'email': ['etl@gsngames.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
}

dag = DAG(
    dag_id='etl_gsn_daily_kpi_email_backfill',
    default_args=default_args,
    schedule_interval='0 23 * * *',  # 12:00 (noon) UTC is 5:00 AM PT, 8:00 AM ET
    concurrency=3,
    max_active_runs=1,
    catchup=True,
)

t_update_wwdesktop_kpi_dim_app_daily_metrics = VerticaOperator(
    task_id='t_update_wwdesktop_kpi_dim_app_daily_metrics',
    vertica_conn_id='vertica_conn',
    sql='sql/t_update_wwdesktop_kpi_dim_app_daily_metrics.sql',
    resourcepool='AIRFLOW_ALL_PRIO',
    params=PARAMS,
    email=['airflow-sla-miss@gsngames.pagerduty.com', ],
    email_on_retry=False,
    email_on_failure=True,
    dag=dag
)


t_update_wwphoenix_kpi_dim_app_daily_metrics = VerticaOperator(
    task_id='t_update_wwphoenix_kpi_dim_app_daily_metrics',
    vertica_conn_id='vertica_conn',
    sql='sql/t_update_wwphoenix_kpi_dim_app_daily_metrics.sql',
    resourcepool='AIRFLOW_ALL_PRIO',
    params=PARAMS,
    email=['airflow-sla-miss@gsngames.pagerduty.com', ],
    email_on_retry=False,
    email_on_failure=True,
    dag=dag
)

t_validate_kpi_email_forecasts = GsnSqlSensor(
    task_id='t_validate_kpi_email_forecasts',
    conn_id='vertica_conn',
    sql='sql/t_validate_forecasts.sql',
    resourcepool='AIRFLOW_ALL_PRIO',
    poke_interval=600,
    timeout=600,
    email=['data-team-standard@gsngames.pagerduty.com', 'gkutty@gsngames.com'],
    email_on_retry=False,
    email_on_failure=True,
    retries=0,
    dag=dag
)