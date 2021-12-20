from datetime import datetime, timedelta
from airflow import DAG
from kochava_impressions_importer_pkg.kochava_impressions_importer import KochavaImpressionsImporter
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from ua_master_dag_driver import check_blocker_email

from common.dag_utils import is_airflow

dag_id = 'ua_master_main_dag.kochava_impressions_importer_subdag_000'
start_date = datetime(2021, 11, 21)
if not is_airflow():
    dag_id = 'ua_master_main_dag.kochava_impressions_importer_subdag_000'
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
kochava_impressions_importer_subdag = DAG(dag_id,
                               schedule_interval='0 9 * * *',
                               start_date=start_date,
                               default_args=default_args,
                               catchup=True,
                               )

t_import_kochava_impressions = KochavaImpressionsImporter(task_id='t_import_kochava_impressions',
                                   range_days=1,
                                   vertica_conn_id='vertica_conn',
                                   kochava_api_key_gamesnetwork='kochava_api_key_gamesnetwork',
                                   kochava_api_key_bitrhymes='kochava_api_key_bitrhymes',
                                   kochava_api_key_idlegames='kochava_api_key_idlegames',
                                   rejected_pctdiff_expected=0.75,
                                   sla=timedelta(minutes=120),
                                   dag=kochava_impressions_importer_subdag)



t_import_kochava_impressions_fail_checker = PythonOperator(
    task_id='t_import_kochava_fail_checker',
    python_callable=check_blocker_email,
    op_kwargs={
        'receivers': (['etl@gsngames.com' ]),
        'message': 'Kochava impressions Importer Dag Failed , Please check logs under ua_master_main_dag.kochava_impressions_importer_subdag schedule Task t_import_kochava_impressions',
        'subject': 'Kochava impressions Importer Dag Failed - {subject_formatted_date}'
    },
    provide_context=True,
    trigger_rule=TriggerRule.ALL_FAILED,
    email_on_failure=False,
    email_on_retry=False,
    retries=0,
    dag=kochava_impressions_importer_subdag
)

t_import_kochava_impressions >> t_import_kochava_impressions_fail_checker
