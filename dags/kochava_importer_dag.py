from datetime import datetime, timedelta
from airflow import DAG
from kochava_importer_pkg.kochava_importer import KochavaImporter
from kochava_importer_pkg.kochava_importer_config import config
from common.operators.vertica_operator import VerticaOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.operators.dummy_operator import DummyOperator
from common.operators.vertica_data_sensor import VerticaDataSensor
import re

dag_id = 'kochava_importer_ua_main_003'
start_date = datetime(2021, 11, 21)

# we do not have any data validation here , data validation for gsnmobile.ad_partner_installs is done by UA master dag /ua_master_dag/sql1/ua_attribution_tasks/t_validate_kochava_data.sql

default_args = {
    'owner': 'UA',
    'depends_on_past': False,
    'email': ['etl@gsngames.com',],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(dag_id=dag_id,
                               schedule_interval='30 0-8,10,12-23 * * *',
                               start_date=start_date,
                               default_args=default_args,
                               catchup=False,
                               max_active_runs=1
                               )

t_update_dim_device_source = VerticaOperator(
    task_id='t_update_dim_device_source',
    vertica_conn_id='vertica_conn',
    resourcepool='default',
    sql='kochava_importer/sql/t_update_dim_device_source.sql',
    dag=dag)

# t_update_bingo_network_mapping = VerticaOperator(
#     task_id='t_update_bingo_network_mapping',
#     vertica_conn_id='vertica_conn',
#     resourcepool='default',
#     sql='kochava_importer/sql/t_update_bingo_network_mapping.sql',
#     dag=dag)


t_apply_synthetic_ids = VerticaOperator(
    task_id='t_apply_synthetic_ids',
    vertica_conn_id='vertica_conn',
    resourcepool='default',
    sql='kochava_importer_pkg/sql/t_apply_synthetic_ids.sql',
    email=['airflow-sla-miss@gsngames.pagerduty.com'],
    dag=dag)


def check_trigger(execution_date, check_hour, **kwargs):
    if isinstance(check_hour, int):
        return execution_date.hour == check_hour
    elif isinstance(check_hour, list):
        return execution_date.hour in check_hour
    else:
        return RuntimeError(f"Unexpected argument for check_hour: {check_hour}")

# trigger some downstream daily jobs after the 7 day range has run
check_trigger_d = ShortCircuitOperator(
  task_id='check_trigger_d',
  python_callable=check_trigger,
  op_kwargs={'check_hour': [18, 19]},
  provide_context=True,
  dag=dag
)

# check for prior day holes past midnight PST
check_trigger_validations = ShortCircuitOperator(
  task_id='check_trigger_validations',
  python_callable=check_trigger,
  op_kwargs={'check_hour': 10},
  provide_context=True,
  dag=dag
)

apps = [
    'PHOENIX',
]

t_check_holes_installs = []
for app in apps:
    app_clean = re.sub(' |\.', '_', app.lower())
    t_check_holes_installs += [VerticaDataSensor(
        task_id=f't_check_holes_installs_{app_clean}',
        vertica_conn_id='vertica_conn',
        resourcepool='default',
        table_name='gsnmobile.ad_partner_installs',
        time_dimension='install_date',
        days_offset=-1,
        metrics='count(synthetic_id)',
        conditionals=[f"app_name = '{app}'"],
        granularity='hour',
        poke_interval=60,
        timeout=60,
        #email=['data-team-standard@gsngames.pagerduty.com'],
        email=['ahirve@gsngames.com'],
        email_on_retry=False,
        email_on_failure=True,
        retries=0,
        dag=dag
    )]

for gamename in list(config["Accounts"].keys()):
    t_import_kochava = KochavaImporter(task_id=f't_import_kochava_{ gamename }',
        vertica_conn_id = 'vertica_conn',
        kochava_gamename = gamename,
        kochava_api_key = config["Accounts"][gamename]["connection_name"],
        ua_config_type="master",
        retries=1,
        email=['airflow-sla-miss@gsngames.pagerduty.com'],
        dag = dag)


    t_import_kochava >>  t_apply_synthetic_ids >> [check_trigger_d, check_trigger_validations]

# check_trigger_d >> t_update_bingo_network_mapping
check_trigger_d >> t_update_dim_device_source

for task in t_check_holes_installs:
    check_trigger_validations >> task
