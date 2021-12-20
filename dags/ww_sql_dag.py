from airflow import DAG
from datetime import datetime, timedelta
from common.operators.vertica_data_sensor import VerticaDataSensor

from common.operators.vertica_operator import VerticaOperator
from common.dag_utils import is_airflow

default_args = {
    'owner': 'skill',
    'start_date': datetime(2021, 11, 21),
    'email': ['etl@gsngames.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    dag_id='ww_aggs_000',
    default_args=default_args,
    schedule_interval='45 7 * * *',
    catchup=True,
    max_active_runs=1,
)

t_update_daily_cash_game_stats = VerticaOperator(
    task_id="t_update_daily_cash_game_stats",
    vertica_conn_id="vertica_conn",
    sql='sql/daily_cash_game_stats.sql',
    resourcepool="default",
    dag=dag
)

t_update_ww_agg_kpi_daily = VerticaOperator(
    task_id="t_update_ww_agg_kpi_daily",
    vertica_conn_id="vertica_conn",
    sql='sql/ww_agg_kpi_daily.sql',
    resourcepool="default",
    dag=dag
)


t_validate_played_games = VerticaDataSensor(
    task_id='t_validate_played_games',
    vertica_conn_id='vertica_conn',
    resourcepool='default',
    table_name='ww.played_games',
    time_dimension='start_time',
    metrics='count(1)',
    granularity='hour',
    days_offset=0,
    poke_interval=600,
    timeout=600,
    email_on_retry=False,
    email_on_failure=True,
    retries=0,
    dag=dag,
)

t_validate_login_activity = VerticaDataSensor(
    task_id='t_validate_login_activity',
    vertica_conn_id='vertica_conn',
    resourcepool='default',
    table_name='ww.login_activity',
    time_dimension='activity_date',
    metrics='count(1)',
    granularity='hour',
    days_offset=0,
    poke_interval=600,
    timeout=600,
    email_on_retry=False,
    email_on_failure=True,
    retries=0,
    dag=dag,
)

t_validate_ww_dim_users = VerticaDataSensor(
    task_id='t_validate_ww_dim_users',
    vertica_conn_id='vertica_conn',
    resourcepool='default',
    table_name='ww.dim_users',
    time_dimension='createdate',
    metrics='count(1)',
    granularity='hour',
    days_offset=0,
    poke_interval=600,
    timeout=600,
    email_on_retry=False,
    email_on_failure=True,
    retries=0,
    dag=dag,
)

t_validate_ww_internal_transactions = VerticaDataSensor(
    task_id='t_validate_ww_internal_transactions',
    vertica_conn_id='vertica_conn',
    resourcepool='default',
    table_name='ww.internal_transactions',
    time_dimension='trans_date',
    metrics='sum(amount)',
    granularity='hour',
    days_offset=0,
    poke_interval=600,
    timeout=600,
    email_on_retry=False,
    email_on_failure=True,
    retries=0,
    dag=dag,
)


t_validate_ww_internal_transactions >> t_update_ww_agg_kpi_daily
t_validate_ww_dim_users >> t_update_ww_agg_kpi_daily
t_validate_login_activity >> t_update_ww_agg_kpi_daily
t_validate_played_games >> t_update_ww_agg_kpi_daily

t_validate_ww_internal_transactions >> t_update_daily_cash_game_stats
t_validate_ww_dim_users >> t_update_daily_cash_game_stats
