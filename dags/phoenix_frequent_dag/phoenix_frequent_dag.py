"""DAG that aggregates appstore revenue data.

"""
from airflow import DAG

from common.operators.gsn_sensor import GsnSqlSensor
# from airflow.operators.subdag import SubDagOperator
from airflow.operators.python import PythonOperator
from common.dbt_utils import run_dbt
from datetime import datetime, timedelta
import pathlib
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['etl@gsngames.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

_scripts_bucket_name ="dev-gametaco-airflow"
_docs_s3_path = "etl_phoenix_frequent_jobs/docs"
_start_date=datetime(2021, 12, 16)
_dbt_path = os.path.join(pathlib.Path(__file__).parent.absolute(), "dbt")

main_dag = DAG(
    dag_id='etl_phoenix_frequent_jobs',
    default_args=default_args,
    start_date=_start_date,
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
    dag=main_dag
)

# dbt_task = SubDagOperator(
#     task_id = "dbt_task",
#     subdag=make_sub_dag(main_dag.dag_id,main_dag.start_date, None, _scripts_bucket_name,_docs_s3_path,_dbt_path),
#     dag=main_dag
# )
dbt_task = PythonOperator(
    task_id='dbt_task',
    dag=main_dag,
    python_callable=run_dbt,
    op_kwargs={'parent_dag_name': main_dag.dag_id,'scripts_bucket_name': _scripts_bucket_name, 'docs_s3_path':_docs_s3_path,'dbt_path':_dbt_path},
  )

t_validate_client_events_phoenix >> dbt_task