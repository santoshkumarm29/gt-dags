"""DAG that handles derived tables and reporting based on a daily load from gsncom and worldwinner

"""

from airflow import DAG

from common.operators.vertica_operator import VerticaOperator
from common.operators.gsn_sensor import GsnSqlSensor
from datetime import datetime, timedelta
from common.operators.email_sql_results_operator import EmailSQLResultsOperator
from common.operators.sftp_sql_results_operator import SFTPSQLResultsOperator
from common.dag_utils import is_airflow
from airflow.operators.python_operator import PythonOperator

from worldwinner_gsncom_daily_imports.responsys_ww_obfuscation import send_obfuscated_unsubscribe_data

# # =============================================================================
# # Default arguments
# # =============================================================================

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 11, 21) if is_airflow() else datetime(2021, 11, 21),
    'email': ['etl@gsngames.com','data-team-standard@gsngames.pagerduty.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'sla': timedelta(hours=2)
}

# # =============================================================================
# # DAG
# # =============================================================================

dag = DAG(
    dag_id='worldwinner_responsys_daily_imports',
    default_args=default_args,
    schedule_interval='30 12 * * *',  # UTC Equivalent of 6 AM EST/ 3 AM PST. WW wwont hit until around 7, but this gives gsncom a head start.
    catchup=True,
)

# # =============================================================================
# # Define the nodes in the DAG
# # =============================================================================

# # =============================================================================
# # Validate snapshot loader tables (WW and platform)
# # =============================================================================

t_validate_ww_dim_users = GsnSqlSensor(
    task_id='t_validate_ww_dim_users',
    conn_id='vertica_conn',
    sql='sql/t_validate_ww_dim_users.sql',
    resourcepool='default',
    poke_interval=600,
    timeout=7200,
    dag=dag
)


t_validate_ww_dim_user_aux = GsnSqlSensor(
    task_id='t_validate_ww_dim_user_aux',
    conn_id='vertica_conn',
    sql='sql/t_validate_ww_dim_user_aux.sql',
    resourcepool='default',
    poke_interval=600,
    timeout=7200,
    dag=dag
)


t_validate_ww_dim_profiles_private = GsnSqlSensor(
    task_id='t_validate_ww_dim_profiles_private',
    conn_id='vertica_conn',
    sql='sql/t_validate_ww_dim_profiles_private.sql',
    resourcepool='default',
    poke_interval=600,
    timeout=7200,
    dag=dag
)

t_validate_ww_dim_point_account = GsnSqlSensor(
    task_id='t_validate_ww_dim_point_account',
    conn_id='vertica_conn',
    sql='sql/t_validate_ww_dim_point_account.sql',
    resourcepool='default',
    poke_interval=600,
    timeout=7200,
    dag=dag
)



# # =============================================================================
# # Validate ww incremental tables (vloads_ww)
# # =============================================================================

# Every 24 hours
t_validate_ww_internal_transactions = GsnSqlSensor(
    task_id='t_validate_ww_internal_transactions',
    conn_id='vertica_conn',
    sql='sql/t_validate_ww_internal_transactions.sql',
    resourcepool='default',
    poke_interval=600,
    timeout=7200,
    dag=dag
)

t_validate_ww_played_games = GsnSqlSensor(
    task_id='t_validate_ww_played_games',
    conn_id='vertica_conn',
    sql='sql/t_validate_ww_played_games.sql',
    resourcepool='default',
    poke_interval=600,
    timeout=7200,
    dag=dag
)

t_validate_ww_tournaments = GsnSqlSensor(
    task_id='t_validate_ww_tournaments',
    conn_id='vertica_conn',
    sql='sql/t_validate_ww_tournaments.sql',
    resourcepool='default',
    poke_interval=600,
    timeout=7200,
    dag=dag
)

t_validate_ww_login_activity = GsnSqlSensor(
    task_id='t_validate_ww_login_activity',
    conn_id='vertica_conn',
    sql='sql/t_validate_ww_login_activity.sql',
    resourcepool='default',
    poke_interval=600,
    timeout=7200,
    dag=dag
)


t_validate_ltc_predictions = GsnSqlSensor(
    task_id='t_validate_ltc_predictions',
    conn_id='vertica_conn',
    sql='sql/t_validate_ltc_predictions.sql',
    resourcepool='default',
    poke_interval=600,
    timeout=7200,
    dag=dag
)

# # =============================================================================
# # Validate WW App events
# # =============================================================================

t_validate_client_events_phoenix = GsnSqlSensor(
    task_id='t_validate_client_events_phoenix',
    conn_id='vertica_conn',
    sql='sql/t_validate_client_events_phoenix.sql',
    resourcepool='default',
    poke_interval=600,
    timeout=7200,
    dag=dag
)


# # =============================================================================
# # Prep ww responsys tables where a diff is necessary
# # =============================================================================

t_prepare_responsys_ww_main = VerticaOperator(
    task_id='t_prepare_responsys_ww_main',
    vertica_conn_id='vertica_conn',
    resourcepool='default',
    sql='sql/t_prepare_responsys_ww_main.sql',
    dag=dag
)

t_prepare_responsys_ww_pet_summary = VerticaOperator(
    task_id='t_prepare_responsys_ww_pet_summary',
    vertica_conn_id='vertica_conn',
    resourcepool='default',
    sql='sql/t_prepare_responsys_ww_pet_summary.sql',
    dag=dag
)

t_prepare_responsys_ww_pet_tags = VerticaOperator(
    task_id='t_prepare_responsys_ww_pet_tags',
    vertica_conn_id='vertica_conn',
    resourcepool='default',
    sql='sql/t_prepare_responsys_ww_pet_tags.sql',
    dag=dag
)

t_prepare_responsys_ww_pet_unsub = VerticaOperator(
    task_id='t_prepare_responsys_ww_pet_unsub',
    vertica_conn_id='vertica_conn',
    resourcepool='default',
    sql='sql/t_prepare_responsys_ww_pet_unsub.sql',
    dag=dag
)

t_prepare_responsys_ww_pet_universe = VerticaOperator(
    task_id='t_prepare_responsys_ww_pet_universe',
    vertica_conn_id='vertica_conn',
    resourcepool='default',
    sql='sql/t_prepare_responsys_ww_pet_universe.sql',
    dag=dag
)

t_prepare_responsys_ww_pet_universe2 = VerticaOperator(
    task_id='t_prepare_responsys_ww_pet_universe2',
    vertica_conn_id='vertica_conn',
    resourcepool='default',
    sql='sql/t_prepare_responsys_ww_pet_universe2.sql',
    dag=dag
)


t_prepare_responsys_ww_pet_affinity = VerticaOperator(
    task_id='t_prepare_responsys_ww_pet_affinity',
    vertica_conn_id='vertica_conn',
    resourcepool='default',
    sql='sql/t_prepare_responsys_ww_pet_affinity.sql',
    dag=dag
)

t_prepare_responsys_ww_pet_ltc = VerticaOperator(
    task_id='t_prepare_responsys_ww_pet_ltc',
    vertica_conn_id='vertica_conn',
    resourcepool='default',
    sql='sql/t_prepare_responsys_ww_pet_ltc.sql',
    dag=dag
)

# # =============================================================================
# # Responsys exports for ww
# # =============================================================================

t_sftp_responsys_ww_main = SFTPSQLResultsOperator(
    task_id='t_sftp_responsys_ww_main',
    ssh_remote_host='files.dc2.responsys.net',
    ssh_username='gsncash_scp',
    ssh_rsa_key_str="RESPONSYS_SSH_KEY",
    vertica_conn_id='vertica_conn',
    sql='sql/t_sftp_responsys_ww_main.sql',
    resourcepool='default',
    destination_path='upload/{{ macros.ds_format(tomorrow_ds, "%Y-%m-%d", "%Y%m%d") }}.GSN_MASTERDATA_TABLE_CASHGAMES.gz',
    compression='gzip',
    dag=dag
)

t_sftp_responsys_ww_pet_summary = SFTPSQLResultsOperator(
    task_id='t_sftp_responsys_ww_pet_summary',
    ssh_remote_host='files.dc2.responsys.net',
    ssh_username='gsncash_scp',
    ssh_rsa_key_str="RESPONSYS_SSH_KEY",
    vertica_conn_id='vertica_conn',
    sql='sql/t_sftp_responsys_ww_pet_summary.sql',
    resourcepool='default',
    destination_path='upload/{{ macros.ds_format(tomorrow_ds, "%Y-%m-%d", "%Y%m%d") }}.GSN_PET_SUMM_CASHGAMES.gz',
    compression='gzip',
    dag=dag
)

t_sftp_responsys_ww_pet_tags = SFTPSQLResultsOperator(
    task_id='t_sftp_responsys_ww_pet_tags',
    ssh_remote_host='files.dc2.responsys.net',
    ssh_username='gsncash_scp',
    ssh_rsa_key_str="RESPONSYS_SSH_KEY",
    vertica_conn_id='vertica_conn',
    sql='sql/t_sftp_responsys_ww_pet_tags.sql',
    resourcepool='default',
    destination_path='upload/{{ macros.ds_format(tomorrow_ds, "%Y-%m-%d", "%Y%m%d") }}.GSN_PET_TAGS_CASHGAMES.gz',
    compression='gzip',
    dag=dag
)

t_sftp_responsys_ww_pet_universe = SFTPSQLResultsOperator(
    task_id='t_sftp_responsys_ww_pet_universe',
    ssh_remote_host='files.dc2.responsys.net',
    ssh_username='gsncash_scp',
    ssh_rsa_key_str="RESPONSYS_SSH_KEY",
    vertica_conn_id='vertica_conn',
    sql='sql/t_sftp_responsys_ww_pet_universe.sql',
    resourcepool='default',
    destination_path='upload/{{ macros.ds_format(tomorrow_ds, "%Y-%m-%d", "%Y%m%d") }}.GSN_PET_UNIV_CASHGAMES.gz',
    compression='gzip',
    dag=dag
)

t_sftp_responsys_ww_pet_universe2 = SFTPSQLResultsOperator(
    task_id='t_sftp_responsys_ww_pet_universe2',
    ssh_remote_host='files.dc2.responsys.net',
    ssh_username='gsncash_scp',
    ssh_rsa_key_str="RESPONSYS_SSH_KEY",
    vertica_conn_id='vertica_conn',
    sql='sql/t_sftp_responsys_ww_pet_universe2.sql',
    resourcepool='default',
    destination_path='upload/{{ macros.ds_format(tomorrow_ds, "%Y-%m-%d", "%Y%m%d") }}.GSN_PET_UNIV2_CASHGAMES.gz',
    compression='gzip',
    dag=dag
)

t_sftp_responsys_ww_pet_unsub = PythonOperator(
    task_id='t_sftp_responsys_ww_pet_unsub',
    python_callable=send_obfuscated_unsubscribe_data,
    provide_context=True,
    op_kwargs={'vertica_conn_id': 'vertica_conn',
               'remote_host': 'files.dc2.responsys.net',
               'username': 'gsncash_scp',
               'rsa_key_str': "RESPONSYS_SSH_KEY"
                },
    templates_dict={'destination_path': 'upload/{{ macros.ds_format(tomorrow_ds, "%Y-%m-%d", "%Y%m%d") }}.GSN_PET_UNSUB_CASHGAMES.csv.gz'},
    dag=dag
)

t_sftp_responsys_ww_pet_affinity_1 = SFTPSQLResultsOperator(
    task_id='t_sftp_responsys_ww_pet_affinity_1',
    ssh_remote_host='files.dc2.responsys.net',
    ssh_username='gsncash_scp',
    ssh_rsa_key_str="RESPONSYS_SSH_KEY",
    vertica_conn_id='vertica_conn',
    sql='sql/t_sftp_responsys_ww_pet_affinity_1.sql',
    resourcepool='default',
    destination_path='upload/{{ macros.ds_format(tomorrow_ds, "%Y-%m-%d", "%Y%m%d") }}.GSN_PET_AFFINITY_1.gz',
    compression='gzip',
    dag=dag
)

t_sftp_responsys_ww_pet_affinity_2 = SFTPSQLResultsOperator(
    task_id='t_sftp_responsys_ww_pet_affinity_2',
    ssh_remote_host='files.dc2.responsys.net',
    ssh_username='gsncash_scp',
    ssh_rsa_key_str="RESPONSYS_SSH_KEY",
    vertica_conn_id='vertica_conn',
    sql='sql/t_sftp_responsys_ww_pet_affinity_2.sql',
    resourcepool='default',
    destination_path='upload/{{ macros.ds_format(tomorrow_ds, "%Y-%m-%d", "%Y%m%d") }}.GSN_PET_AFFINITY_2.gz',
    compression='gzip',
    dag=dag
)

t_sftp_responsys_ww_pet_ltc = SFTPSQLResultsOperator(
    task_id='t_sftp_responsys_ww_pet_ltc',
    ssh_remote_host='files.dc2.responsys.net',
    ssh_username='gsncash_scp',
    ssh_rsa_key_str="RESPONSYS_SSH_KEY",
    vertica_conn_id='vertica_conn',
    sql='sql/t_sftp_responsys_ww_pet_ltc.sql',
    resourcepool='default',
    destination_path='upload/{{ macros.ds_format(tomorrow_ds, "%Y-%m-%d", "%Y%m%d") }}.GSN_PET_LTC.gz',
    compression='gzip',
    dag=dag
)

# # =============================================================================
# # Clean up snapshot tables
# # =============================================================================


t_cleanup_ww_main = VerticaOperator(
    task_id='t_cleanup_ww_main',
    vertica_conn_id='vertica_conn',
    resourcepool='default',
    sql='sql/drop_old_partition.sql',
    params={'table': 'airflow.snapshot_responsys_ww_main'},
    dag=dag
)

t_cleanup_ww_pet_summary = VerticaOperator(
    task_id='t_cleanup_ww_pet_summary',
    vertica_conn_id='vertica_conn',
    resourcepool='default',
    sql='sql/drop_old_partition.sql',
    params={'table': 'airflow.snapshot_responsys_ww_pet_summary'},
    dag=dag
)

t_cleanup_ww_pet_tags = VerticaOperator(
    task_id='t_cleanup_ww_pet_tags',
    vertica_conn_id='vertica_conn',
    resourcepool='default',
    sql='sql/drop_old_partition.sql',
    params={'table': 'airflow.snapshot_responsys_ww_pet_tags'},
    dag=dag
)

t_cleanup_ww_pet_universe = VerticaOperator(
    task_id='t_cleanup_ww_pet_universe',
    vertica_conn_id='vertica_conn',
    resourcepool='default',
    sql='sql/drop_old_partition.sql',
    params={'table': 'airflow.snapshot_responsys_ww_pet_universe'},
    dag=dag
)

t_cleanup_ww_pet_universe2 = VerticaOperator(
    task_id='t_cleanup_ww_pet_universe2',
    vertica_conn_id='vertica_conn',
    resourcepool='default',
    sql='sql/drop_old_partition.sql',
    params={'table': 'airflow.snapshot_responsys_ww_pet_universe2'},
    dag=dag
)


t_cleanup_ww_pet_unsub = VerticaOperator(
    task_id='t_cleanup_ww_pet_unsub',
    vertica_conn_id='vertica_conn',
    resourcepool='default',
    sql='sql/drop_old_partition.sql',
    params={'table': 'airflow.snapshot_responsys_ww_pet_unsub'},
    dag=dag
)

t_cleanup_ww_pet_affinity = VerticaOperator(
    task_id='t_cleanup_ww_pet_affinity',
    vertica_conn_id='vertica_conn',
    resourcepool='default',
    sql='sql/drop_old_partition.sql',
    params={'table': 'airflow.snapshot_responsys_ww_pet_affinity'},
    dag=dag
)

t_cleanup_ww_pet_ltc = VerticaOperator(
    task_id='t_cleanup_ww_pet_ltc',
    vertica_conn_id='vertica_conn',
    resourcepool='default',
    sql='sql/drop_old_partition.sql',
    params={'table': 'airflow.snapshot_responsys_ww_pet_ltc'},
    dag=dag
)

# =============================================================================
# Define the directed edges in the DAG
# =============================================================================



# Dependencies for t_sftp_responsys_ww_main
t_validate_ww_dim_users >> t_prepare_responsys_ww_main
t_validate_ww_dim_user_aux >> t_prepare_responsys_ww_main
t_validate_ww_dim_profiles_private >> t_prepare_responsys_ww_main

t_prepare_responsys_ww_main >> t_sftp_responsys_ww_main
t_sftp_responsys_ww_main >> t_cleanup_ww_main

# Dependencies for t_sftp_responsys_ww_pet_summary
t_validate_ww_dim_users >> t_prepare_responsys_ww_pet_summary
t_validate_ww_dim_profiles_private >> t_prepare_responsys_ww_pet_summary
t_validate_ww_dim_user_aux >> t_prepare_responsys_ww_pet_summary
t_validate_ww_internal_transactions >> t_prepare_responsys_ww_pet_summary
t_validate_ww_dim_point_account >> t_prepare_responsys_ww_pet_summary
t_validate_ww_played_games >> t_prepare_responsys_ww_pet_summary

t_prepare_responsys_ww_pet_summary >> t_sftp_responsys_ww_pet_summary
t_sftp_responsys_ww_pet_summary >> t_cleanup_ww_pet_summary


# Dependencies for t_sftp_responsys_ww_pet_universe
t_validate_ww_dim_user_aux >> t_prepare_responsys_ww_pet_universe
t_validate_ww_dim_users >> t_prepare_responsys_ww_pet_universe

t_validate_ww_dim_user_aux >> t_prepare_responsys_ww_pet_universe2
t_validate_ww_dim_users >> t_prepare_responsys_ww_pet_universe2

t_prepare_responsys_ww_pet_universe >> t_sftp_responsys_ww_pet_universe
t_sftp_responsys_ww_pet_universe >> t_cleanup_ww_pet_universe

t_prepare_responsys_ww_pet_universe2 >> t_sftp_responsys_ww_pet_universe2
t_sftp_responsys_ww_pet_universe2 >> t_cleanup_ww_pet_universe2

# Dependencies for t_sftp_responsys_ww_pet_tags
t_validate_ww_dim_users >> t_prepare_responsys_ww_pet_tags
t_validate_ww_dim_user_aux >> t_prepare_responsys_ww_pet_tags

t_prepare_responsys_ww_pet_tags >> t_sftp_responsys_ww_pet_tags
t_sftp_responsys_ww_pet_tags >> t_cleanup_ww_pet_tags

# Dependencies for t_sftp_responsys_ww_pet_unsubscribe
t_validate_ww_dim_users >> t_prepare_responsys_ww_pet_unsub
t_validate_ww_dim_user_aux >> t_prepare_responsys_ww_pet_unsub

t_prepare_responsys_ww_pet_unsub >> t_sftp_responsys_ww_pet_unsub
t_sftp_responsys_ww_pet_unsub >> t_cleanup_ww_pet_unsub

# Dependencies for t_sftp_responsys_ww_pet_affinity
t_validate_ww_dim_users >> t_prepare_responsys_ww_pet_affinity
t_validate_ww_played_games >> t_prepare_responsys_ww_pet_affinity
t_validate_ww_tournaments >> t_prepare_responsys_ww_pet_affinity
t_validate_client_events_phoenix >> t_prepare_responsys_ww_pet_affinity
t_validate_ww_internal_transactions >> t_prepare_responsys_ww_pet_affinity
t_validate_ww_login_activity >> t_prepare_responsys_ww_pet_affinity

t_prepare_responsys_ww_pet_affinity >> t_sftp_responsys_ww_pet_affinity_1
t_sftp_responsys_ww_pet_affinity_1 >> t_cleanup_ww_pet_affinity
t_prepare_responsys_ww_pet_affinity >> t_sftp_responsys_ww_pet_affinity_2
t_sftp_responsys_ww_pet_affinity_2 >> t_cleanup_ww_pet_affinity

# Dependencies for LTC
t_validate_ltc_predictions >> t_prepare_responsys_ww_pet_ltc
t_validate_ww_dim_users >> t_prepare_responsys_ww_pet_ltc
t_prepare_responsys_ww_pet_ltc >> t_sftp_responsys_ww_pet_ltc
t_sftp_responsys_ww_pet_ltc >> t_cleanup_ww_pet_ltc

