from airflow import DAG
from datetime import datetime

from common.operators.vertica_operator import VerticaOperator


vertica_conn_id = 'vertica_conn'

default_args = {
    'owner': 'skill',
    'depends_on_past': False,
    'start_date': datetime(2021, 11, 21),
    'email': ['etl@gsngames.com',
              'jstix@gsngames.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}


dag = DAG(
    dag_id='ww_ltv_calculations_000',
    default_args=default_args,
    schedule_interval='0 9 * * *',
    catchup=False,
    max_active_runs=1,
)


import_from_sftp = VerticaOperator(
    task_id='t_truncate_ltv_table_and_insert',
    sql='sql/Truncate LTV Table and Insert.sql',
    resourcepool='default',
    vertica_conn_id=vertica_conn_id,
    dag=dag,
)
