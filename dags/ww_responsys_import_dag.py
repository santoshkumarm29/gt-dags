from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from common.operators.vertica_operator import VerticaOperator
from common.dag_utils import is_airflow

from ww_responsys_import.ww_responsys_import_driver import import_responsys, get_column_names
from ww_responsys_import.ww_responsys_import_settings import config

dag_id = 'ww_responsys_import_001'
st_date = datetime(2021, 10, 29)

if not is_airflow():
    dag_id = 'ww_responsys_import_001'
    st_date = datetime(2021, 11, 21)

vertica_conn_id = 'vertica_conn'

default_args = {
    'owner': 'skill',
    'depends_on_past': False,
    'start_date': st_date,
    'email': ['etl@gsngames.com','data-team-standard@gsngames.pagerduty.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}


dag = DAG(
    dag_id=dag_id,
    default_args=default_args,
    schedule_interval='@daily',
    user_defined_macros=dict(get_column_names=get_column_names),
    catchup=True,
    max_active_runs=1,
)


import_from_sftp = PythonOperator(
    task_id='t_import_from_sftp',
    python_callable=import_responsys,
    op_args=[config['table_mappings'], vertica_conn_id],
    dag=dag,
    retries=1,
)

for table in config['table_mappings'].values():
    task = VerticaOperator(
        task_id=f't_update_table_{table}',
        sql='sql/t_update_prod_table.sql',
        params=dict(studio=config['studio'], schema=config['schema'], table_name=table),
        vertica_conn_id=vertica_conn_id,
        resourcepool='default',
        retries=2,
        dag=dag,
    )

    import_from_sftp >> task

