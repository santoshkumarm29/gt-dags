from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from common.operators.vertica_operator import VerticaOperator
from s3_import_parameters_v2 import s3_import_params
from ww_app_data_import_driver import get_task_groups, WWImporter

default_args = {
    'owner': 'skill',
    'depends_on_past': False,
    'start_date': datetime(2021, 11, 9),
    'email': ['juan.luna@full360.com'],
    'retries': 2,
    'email_on_failure': True,
    'email_on_retry': False,
    'execution_timeout': timedelta(minutes=60),
    'sla': timedelta(hours=2),
}

once_an_hour_dag = DAG(
    dag_id='ww_app_data_import_hourly_dag_v2',  # Add run frequency
    default_args=default_args,
    schedule_interval='26 * * * *',
    max_active_runs=1,
    catchup=False
)

params = s3_import_params.copy()
params['run_freq'] = 'HOURLY'
kwargs = params

t_get_file_list = PythonOperator(
    task_id='t_get_file_list',
    python_callable=get_task_groups,
    op_kwargs=params,
    sla=timedelta(hours=2),
    dag=once_an_hour_dag
)

t_user_ids_to_purge = VerticaOperator(
    task_id='t_user_ids_to_purge',
    vertica_conn_id='vertica_conn',
    sql='sql/app_data_import/t_user_ids_to_purge.sql',
    resourcepool='default',
    dag=once_an_hour_dag
)

for num in range(0, params['number_of_tasks']):
    task_id = params['task_prefix'] + str(num)
    tasks_group = WWImporter(
        run_freq=params['run_freq'],
        s3_bucket_name=params['s3_bucket_name'],
        task_id=task_id,
        sla=timedelta(hours=2),
        dag=once_an_hour_dag,
        op_kwargs=kwargs
    )
    t_get_file_list >> tasks_group >> t_user_ids_to_purge

