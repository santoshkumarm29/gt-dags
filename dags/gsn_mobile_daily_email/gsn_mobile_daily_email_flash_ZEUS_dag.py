"""Airflow DAG for the GSN daily KPI email.

"""
from common.kpi_email_utils import send_daily_kpi_email
from os.path import join, dirname
from airflow.operators.dummy import DummyOperator
from airflow.sensors.time_sensor import TimeSensor
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, time
from common.operators.gsn_sensor import GsnSqlSensor
from common.operators.vertica_data_sensor import VerticaDataSensor
from common.operators.vertica_operator import VerticaOperator

PARAMS = dict(
    days_back=0,
    update_job='early edition',
    kpi_ovr_pct=-.05,
    kpi_wk_pct=.1,
    casino_mobile_ovr_ntile=5,
    casino_mobile_mult_ntile=5,
    tripeaks_ovr_ntile=1,
    nonblocking_annotation='KPIs incomplete, under investigation.'
)


default_args = {
    'owner': 'airflow',
    # 'depends_on_past': True,
    # 'wait_for_downstream': True,
    'start_date': datetime(2021, 1, 27),
    'email': ['etl@gsngames.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
     'priority_weight': 100,
}

dag = DAG(
    dag_id='etl_gsn_daily_kpi_email_flash_000',
    default_args=default_args,
    schedule_interval='30 9 * * *',
    catchup=True,
)

# =============================================================================
# App Dict List
# =============================================================================

app_dict_list = [
        {
            'studio': 'Skill Studio',
            'app_names': [
                'WorldWinner Web',
                'WorldWinner App',
            ],
            'superset': ['GSN Games']
        }
    ]



# =============================================================================
# Emailing tasks
# =============================================================================


t_final_send_email = PythonOperator(
    task_id='t_final_send_email',
    python_callable=send_daily_kpi_email,
    op_kwargs={'vertica_conn_id': 'vertica_conn',
               'bcc_addresses': ('bingobash-all@gsngames.com', 'vegas-studio-all@gsngames.com', 'casino-all@gsngames.com',
                             'ngs-all@gsngames.com', 'tripeaks-all@gsngames.com', 'skill-all@gsngames.com',
                             'data@gsngames.com', 'dsylvester@gameshownetwork.com', 'prandazzo@gsngames.com',
                             'ibuinyi@gsngames.com', 'wtaht@gsngames.com', 'ainferrera@gsngames.com',
                             'btruman@gsngames.com', 'cplant@gsngames.com', 'bsullivan@gsngames.com',
                             'axaubet@gsngames.com', 'mrios@gsngames.com', 'anissinboim@gsngames.com',
                             'kraju@gsngames.com', 'pkumar@gsngames.com', 'mturetzky@gsngames.com',),
               'subject': 'Flash Daily KPIs: {rev} ({delta_rev}) IAP Rev | {dau} ({delta_dau}) DAU | {installs} ({delta_installs}) Installs',
               'app_dict_list': app_dict_list,
               'subject_key_name': 'GSN Games',
               'template_path': join(dirname(__file__), 'templates/daily_email_zeus_flash.html'),
               'ads_data': True,
               # 'studio_level_fcst': ['Skill Studio',],
               },
    email=['etl@gsngames.com', 'airflow-sla-miss@gsngames.pagerduty.com'],
    sla=timedelta(hours=1, minutes=40),
    dag=dag
)


t_preview_send_email = PythonOperator(
    task_id='t_preview_send_email',
    python_callable=send_daily_kpi_email,
    op_kwargs={'vertica_conn_id': 'vertica_conn',
               'receivers': ('data@gsngames.com'),
               'subject': 'ZEUS: PREVIEW: Flash Daily KPIs: {rev} ({delta_rev}) IAP Rev | {dau} ({delta_dau}) DAU | {installs} ({delta_installs}) Installs',
               'app_dict_list': app_dict_list,
               'subject_key_name': 'GSN Games',
               'template_path': join(dirname(__file__), 'templates/daily_email_zeus_flash.html'),
               'ads_data': True,
               'kpi_qa_update': True
               #'studio_level_fcst': ['Skill Studio',],
               },
    email_on_failure=True,
    email_on_retry=True,
    email=['etl@gsngames.com','airflow-sla-miss@gsngames.pagerduty.com'],
    dag=dag
)

t_validate_qa_checks = GsnSqlSensor(
    task_id='t_validate_qa_check',
    conn_id='vertica_conn',
    sql='sql/t_validate_qa_checks.sql',
    resourcepool='AIRFLOW_ALL_PRIO',
    poke_interval=600,
    timeout=600,
    email=['data-team-standard@gsngames.pagerduty.com'],
    email_on_retry=False,
    email_on_failure=True,
    retries=0,
    dag=dag
)

# =============================================================================
# Timers
# =============================================================================


t_time_limit_ads_data = TimeSensor(
    task_id='t_time_limit_ads_data',
    target_time=time(hour=9, minute=30),
    dag=dag
)

# t_time_limit_kochava_data = TimeSensor(
#     task_id='t_time_limit_kochava_data',
#     target_time=time(hour=9, minute=30),
#     dag=dag
# )

# =============================================================================
# Updates for critical apps
# =============================================================================

t_update_wwdesktop_kpi_dim_app_daily_metrics = VerticaOperator(
    task_id='t_update_wwdesktop_kpi_dim_app_daily_metrics',
    vertica_conn_id='vertica_conn',
    sql='sql/t_update_wwdesktop_kpi_dim_app_daily_metrics.sql',
    resourcepool='AIRFLOW_ALL_PRIO',
    params=PARAMS,
    email=['airflow-sla-miss@gsngames.pagerduty.com', ],
    email_on_retry=False,
    email_on_failure=True,
    dag=dag
)

# =============================================================================
# Updates for non-critical apps
# =============================================================================


t_update_wwphoenix_kpi_dim_app_daily_metrics = VerticaOperator(
    task_id='t_update_wwphoenix_kpi_dim_app_daily_metrics',
    vertica_conn_id='vertica_conn',
    sql='sql/t_update_wwphoenix_kpi_dim_app_daily_metrics.sql',
    resourcepool='AIRFLOW_ALL_PRIO',
    params=PARAMS,
    email=['airflow-sla-miss@gsngames.pagerduty.com', ],
    email_on_retry=False,
    email_on_failure=True,
    dag=dag
)

t_update_gsncom_ads_kpi_dim_app_daily_metrics = VerticaOperator(
    task_id='t_update_gsncom_ads_kpi_dim_app_daily_metrics',
    vertica_conn_id='vertica_conn',
    sql='sql/t_update_gsncom_ads_kpi_dim_app_daily_metrics.sql',
    params=PARAMS,
    email_on_retry=False,
    email_on_failure=True,
    dag=dag
)

# =============================================================================
# Validations
# =============================================================================


t_validate_ww_login_activity = VerticaDataSensor(
    task_id='t_validate_ww_login_activity',
    vertica_conn_id='vertica_conn',
    resourcepool='AIRFLOW_ALL_PRIO',
    table_name='ww.login_activity',
    time_dimension='activity_date',
    metrics='count(user_id)',
    granularity='hour',
    conditionals=["(platform_id = 1 or platform_id = 2)"],
    days_offset=0,
    poke_interval=600,
    timeout=3000,
    email=['airflow-sla-miss@gsngames.pagerduty.com', ],
    email_on_retry=False,
    email_on_failure=True,
    retries=0,
    dag=dag,
)

t_validate_phoenix_dim_device_mapping = VerticaDataSensor(
    task_id='t_validate_phoenix_dim_device_mapping',
    vertica_conn_id='vertica_conn',
    resourcepool='AIRFLOW_ALL_PRIO',
    table_name='phoenix.dim_device_mapping',
    time_dimension='event_time',
    metrics='count(1)',
    granularity='hour',
    days_offset=0,
    poke_interval=600,
    timeout=3000,
    email=['airflow-sla-miss@gsngames.pagerduty.com', ],
    email_on_retry=False,
    email_on_failure=True,
    retries=0,
    dag=dag,
)

t_validate_ww_dim_users = VerticaDataSensor(
    task_id='t_validate_ww_dim_users',
    vertica_conn_id='vertica_conn',
    resourcepool='AIRFLOW_ALL_PRIO',
    table_name='ww.dim_users',
    time_dimension='createdate',
    metrics='count(1)',
    granularity='hour',
    days_offset=0,
    poke_interval=600,
    timeout=3000,
    email=['airflow-sla-miss@gsngames.pagerduty.com', ],
    email_on_retry=False,
    email_on_failure=True,
    retries=0,
    dag=dag,
)

t_validate_ww_internal_transactions = VerticaDataSensor(
    task_id='t_validate_ww_internal_transactions',
    vertica_conn_id='vertica_conn',
    resourcepool='AIRFLOW_ALL_PRIO',
    table_name='ww.internal_transactions',
    time_dimension='trans_date',
    metrics='sum(amount)',
    granularity='hour',
    days_offset=0,
    poke_interval=600,
    timeout=3000,
    email=['airflow-sla-miss@gsngames.pagerduty.com', ],
    email_on_retry=False,
    email_on_failure=True,
    retries=0,
    dag=dag,
)

# work hours only
t_validate_phoenix_device_day = GsnSqlSensor(
    task_id='t_validate_phoenix_device_day',
    conn_id='vertica_conn',
    sql='sql/t_validate_phoenix_device_day.sql',
    resourcepool='AIRFLOW_ALL_PRIO',
    poke_interval=600,
    timeout=3000,
    email=['airflow-sla-miss@gsngames.pagerduty.com', ],
    email_on_retry=False,
    email_on_failure=True,
    retries=0,
    dag=dag
)

# work hours only
t_validate_phoenix_devices = GsnSqlSensor(
    task_id='t_validate_phoenix_devices',
    conn_id='vertica_conn',
    sql='sql/t_validate_phoenix_devices.sql',
    resourcepool='AIRFLOW_ALL_PRIO',
    poke_interval=600,
    timeout=3000,
    email=['airflow-sla-miss@gsngames.pagerduty.com', ],
    email_on_retry=False,
    email_on_failure=True,
    retries=0,
    dag=dag
)


d_ads_non_blocking = DummyOperator(
    task_id='d_ads_non_blocking',
    trigger_rule='one_success',
    dag=dag,
)


# =============================================================================
# Auto Annotations
# =============================================================================
t_update_auto_annotations = VerticaOperator(
    task_id='t_update_auto_annotations',
    vertica_conn_id='vertica_conn',
    sql='sql/t_update_auto_annotations.sql',
    resourcepool='AIRFLOW_ALL_PRIO',
    params=PARAMS,
    email=['airflow-sla-miss@gsngames.pagerduty.com',],
    email_on_retry=False,
    email_on_failure=True,
    dag=dag
)


#### for execs only email

t_update_wwdesktop_kpi_dim_app_daily_metrics >> t_update_auto_annotations
t_update_wwphoenix_kpi_dim_app_daily_metrics >> t_update_auto_annotations
t_update_gsncom_ads_kpi_dim_app_daily_metrics >> t_update_auto_annotations

t_update_auto_annotations >> t_preview_send_email


################################
t_time_limit_ads_data >> d_ads_non_blocking
# t_validate_ads_aggregation >> d_ads_non_blocking

d_ads_non_blocking >> t_update_gsncom_ads_kpi_dim_app_daily_metrics

t_validate_phoenix_dim_device_mapping >> t_update_wwdesktop_kpi_dim_app_daily_metrics
t_validate_ww_login_activity >> t_update_wwdesktop_kpi_dim_app_daily_metrics
t_validate_ww_internal_transactions >> t_update_wwdesktop_kpi_dim_app_daily_metrics
t_validate_ww_dim_users >> t_update_wwdesktop_kpi_dim_app_daily_metrics
d_ads_non_blocking >> t_update_wwdesktop_kpi_dim_app_daily_metrics

t_validate_phoenix_device_day >> t_update_wwphoenix_kpi_dim_app_daily_metrics
t_validate_phoenix_devices >> t_update_wwphoenix_kpi_dim_app_daily_metrics
t_validate_ww_internal_transactions >> t_update_wwphoenix_kpi_dim_app_daily_metrics

t_preview_send_email >> t_validate_qa_checks
t_preview_send_email >> t_final_send_email
