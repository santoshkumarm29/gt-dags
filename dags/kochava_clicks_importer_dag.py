from datetime import datetime, timedelta
from airflow import DAG
from kochava_clicks_importer_pkg.kochava_clicks_importer import KochavaClicksImporter
from common.operators.vertica_operator import VerticaOperator
from ua_master_dag_driver import check_blocker_email
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python_operator import ShortCircuitOperator


dag_id = 'ua_master_main_dag.kochava_click_importer_subdag_001'
start_date = datetime(2021, 11, 21)

default_args = {
    'owner': 'UA',
    'depends_on_past': False,
    'email': ['etl@gsngames.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15)
}
kochava_click_importer_subdag = DAG(dag_id,
                               schedule_interval='0 * * * *',
                               start_date=start_date,
                               default_args=default_args,
                               catchup=True,
                               max_active_runs=1
                               )

t_import_kochava_clicks = KochavaClicksImporter(task_id='t_import_kochava_clicks',
                                   range_days=1,
                                   vertica_conn_id='vertica_conn',
                                   kochava_api_key_gamesnetwork='kochava_api_key_gamesnetwork',
                                   sla=timedelta(minutes=120),
                                   dag=kochava_click_importer_subdag)

t_apply_synthetic_ids = VerticaOperator(
    task_id='t_apply_synthetic_ids',
    vertica_conn_id='vertica_conn',
    resourcepool='default',
    sql='kochava_clicks_importer_pkg/sql/t_apply_synthetic_ids.sql',
    sla=timedelta(minutes=45),
    dag=kochava_click_importer_subdag)


t_import_kochava_fail_checker = PythonOperator(
    task_id='t_import_kochava_fail_checker',
    python_callable=check_blocker_email,
    op_kwargs={
        'receivers': (['etl@gsngames.com' ]),
        'message': 'Kochava clicks Importer Dag Failed , Please check logs under ua_master_main_dag.sub_dag_kochava_clicks_importer schedule Task t_import_kochava_clicks',
        'subject': 'Kochava clicks Importer Dag Failed - {subject_formatted_date}'
    },
    provide_context=True,
    trigger_rule=TriggerRule.ALL_FAILED,
    email_on_failure=False,
    email_on_retry=False,
    retries=0,
    dag=kochava_click_importer_subdag
)

def check_trigger(next_execution_date, **kwargs):
    return next_execution_date.hour == 20

check_trigger_d = ShortCircuitOperator(
  task_id='check_trigger_d',
  python_callable=check_trigger,
  provide_context=True,
  dag=kochava_click_importer_subdag
)


t_import_kochava_clicks >> check_trigger_d >> t_apply_synthetic_ids
t_apply_synthetic_ids >> t_import_kochava_fail_checker