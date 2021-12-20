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
    dag_id='etl_phoenix_tournaments',
    default_args=default_args,
    schedule_interval='30 8 * * *',
    max_active_runs=1,
    catchup=True,
)


t_validate_client_events_phoenix = GsnSqlSensor(
    task_id='t_validate_client_events_phoenix',
    conn_id='vertica_conn',
    sql='sql/t_validate_client_events_phoenix.sql',
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

t_update_tournament_cef_phoenix_all = VerticaOperator(
    task_id='t_update_tournament_cef_phoenix_all',
    vertica_conn_id='vertica_conn',
    resourcepool='default',
    sql='sql/t_update_tournament_cef_phoenix_all.sql',
    dag=dag
)

t_update_tournament_winnings_phoenix = VerticaOperator(
    task_id='t_update_tournament_winnings_phoenix',
    vertica_conn_id='vertica_conn',
    resourcepool='default',
    sql='sql/t_update_tournament_winnings_phoenix.sql',
    dag=dag
)

t_validate_client_events_phoenix >> t_update_tournament_cef_phoenix_all
t_validate_ww_internal_transactions >> t_update_tournament_cef_phoenix_all
t_update_tournament_cef_phoenix_all >> t_update_tournament_winnings_phoenix
