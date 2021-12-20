from datetime import datetime, timedelta
from airflow import DAG
from kochava_importer_test.kochava_importer import KochavaImporter #by skm
from kochava_importer_test.kochava_importer_config import config #by skm

from common.operators.vertica_operator import VerticaOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.operators.dummy_operator import DummyOperator
from common.operators.vertica_data_sensor import VerticaDataSensor
import re

dag_id = 'kochava_importer_test_ua_main_000'
start_date = datetime(2021, 10, 28)

# we do not have any data validation here , data validation for gsnmobile.ad_partner_installs is done by UA master dag /ua_master_dag/sql/ua_attribution_tasks/t_validate_kochava_data.sql

default_args = {
    'owner': 'UA',
    'depends_on_past': False,
    'email': ['test@test.com',],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(dag_id=dag_id,
                               schedule_interval='30 15 * * *',
                               start_date=start_date,
                               default_args=default_args,
                               catchup=False,
                               max_active_runs=1
                               )

t_apply_synthetic_ids = VerticaOperator(
    task_id='t_apply_synthetic_ids',
    vertica_conn_id='vertica_conn',
    resourcepool='default',
    sql='kochava_importer_test/sql/t_apply_synthetic_ids.sql',
    dag=dag)

for gamename in list(config["Accounts"].keys()):
    t_import_kochava = KochavaImporter(task_id=f't_import_kochava_{ gamename }',
        vertica_conn_id = 'vertica_conn',
        kochava_gamename = gamename,
        kochava_api_key = config["Accounts"][gamename]["connection_name"],
        ua_config_type="master",
        retries=1,
        dag = dag)

    t_import_kochava >> t_apply_synthetic_ids

