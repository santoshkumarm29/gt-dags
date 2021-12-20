"""DAG that updates ww_user_day tables.

"""
from airflow import DAG

from common.operators.gsn_sensor import GsnSqlSensor
from datetime import datetime, timedelta
from common.operators.vertica_operator import VerticaOperator
from common.dag_utils import is_airflow

default_args = {
    'owner': 'skill',
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
    dag_id='etl_ww_user_day',
    default_args=default_args,
    schedule_interval='10 11 * * *',
    max_active_runs=1,
    catchup=True,
)

# dag.catchup = False

t_validate_ww_dim_users = GsnSqlSensor(
    task_id='t_validate_ww_dim_users',
    conn_id='vertica_conn',
    sql='sql/t_validate_ww_dim_users.sql',
    resourcepool='default',
    sla=timedelta(hours=2),
    dag=dag
)

t_validate_ww_internal_transactions = GsnSqlSensor(
    task_id='t_validate_ww_internal_transactions',
    conn_id='vertica_conn',
    sql='sql/t_validate_ww_internal_transactions.sql',
    resourcepool='default',
    sla=timedelta(hours=2),
    dag=dag
)

t_update_ww_user_day = VerticaOperator(
    task_id='t_update_ww_user_day',
    vertica_conn_id='vertica_conn',
    sql='sql/t_update_ww_user_day.sql',
    resourcepool='default',
    dag=dag
)

t_update_ww_user_day_game = VerticaOperator(
    task_id='t_update_ww_user_day_game',
    vertica_conn_id='vertica_conn',
    sql='sql/t_update_ww_user_day_game.sql',
    resourcepool='default',
    dag=dag
)


t_validate_ww_dim_users >> t_update_ww_user_day
t_validate_ww_internal_transactions >> t_update_ww_user_day

t_update_ww_user_day >> t_update_ww_user_day_game

