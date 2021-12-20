"""
This sub dag is only for singular importer , Singular data needs to be imported every 4 hours . this is sub dag will have its own schedule
"""

from airflow import DAG
from datetime import datetime, timedelta
from common.operators.vertica_operator import VerticaOperator
from singular_importer_driver import SingularImporterV2

from common.dag_utils import is_airflow

dag_id = 'ua_master_main_dag.singular_subdag_000'
start_date = datetime(2021, 11, 21)
if not is_airflow():
    dag_id = 'ua_master_main_dag.singular_subdag_000'
    start_date = datetime(2021, 11, 21)

default_args = {
    'owner': 'UA',
    'depends_on_past': False,
    'email': ['etl@gsngames.com',
              'data-team-standard@gsngames.pagerduty.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

sub_dag_singular = DAG(dag_id,
                       schedule_interval='30 8,10,14 * * *',  # 12 30 am , 2 30 AM , 3 30 AM , 5 30 AM , 8 30 AM  PST note:  DO NOT remove 10:30 AM PST / 15:30 UTC , See SingularImporter get_range for the reason
                       start_date=start_date,
                       default_args=default_args,
                       catchup=True,
                       )

dimensions = [
        'app',
        'site_public_id',
        'source',
        'os',
        'platform',
        'country_field',
        'adn_campaign_name',
        'adn_campaign_id',
        'singular_campaign_id',
        'adn_sub_campaign_name',
        'adn_sub_campaign_id',
        'adn_publisher_id',
        'adn_sub_adnetwork_name',
        'adn_original_currency',
        'adn_timezone',
        'adn_utc_offset',
        'adn_account_id',
        'adn_campaign_url',
        'tracker_name',
        'publisher_site_name',
        'publisher_site_id',
        'affiliate_name'
]

t_import_singular_data = SingularImporterV2(task_id='t_import_singular_data',
                                          dimensions=dimensions,
                                          singular_table='apis.singular_campaign_stats',
                                          range_days=7,
                                          singular_api_key='singular_api_key',
                                          vertica_conn_id='vertica_conn',
                                          rejected_pctdiff_expected=0.75,
                                          sla=timedelta(minutes=90),
                                          dag=sub_dag_singular)

# task to import data into tableau stagging tables is already located at tableau_staging_tasks/t_update_tableau_us_singular_data.sql , we will reuse the same

t_update_tableau_ua_singular_data = VerticaOperator(
    task_id='t_update_tableau_ua_singular_data',
    vertica_conn_id='vertica_conn',
    resourcepool='default',
    sql='sql/tableau_staging_tasks/t_update_tableau_us_singular_data.sql',
    sla=timedelta(minutes=45),
    dag=sub_dag_singular
)

t_import_singular_data >> t_update_tableau_ua_singular_data
