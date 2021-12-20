from airflow import DAG
from common.operators.email_sql_results_operator import EmailSQLResultsOperator
from datetime import datetime, timedelta
from common.operators.vertica_data_sensor import VerticaDataSensor

default_args = {
    'owner': 'skill',
    'depends_on_past': False,
    'start_date': datetime(2021, 11, 21),
    'email': ['ahirve@test.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='skill_partner_reports_001',
    default_args=default_args,
    schedule_interval='0 8 * * *',
    max_active_runs=1,
    catchup=False,
)

t_send_email_swagbucks = EmailSQLResultsOperator(
    task_id='t_send_email_swagbucks',
    sql='sql/swagbucks_report.sql',
    vertica_conn_id='vertica_conn',
    source='etl@test.com',
    bcc_addresses=('ahirve@test.com','shiggins@test.com', 'mdusenberry@test.com','skill-reports@test.com','daniel.r@prodege.com','dennis.p@prodege.com','GSNReports@prodege.com'),
    subject='Swagbucks Daily Report ({{ ds }})',
    style='attachment',
    filename='swagbucks_daily_report_{{ ds }}.sql.csv',
    sla=timedelta(hours=1),
    dag=dag
)

t_send_email_inbox_dollars = EmailSQLResultsOperator(
    task_id='t_send_email_inbox_dollars',
    sql='sql/inbox_dollars_report.sql',
    vertica_conn_id='vertica_conn',
    source='etl@test.com',
    bcc_addresses=('ahirve@test.com','shiggins@test.com', 'mdusenberry@test.com','rganesan@test.com'),
    subject='Inbox Dollars Daily Report ({{ ds }})',
    style='attachment',
    filename='inbox_dollars_daily_report_{{ ds }}.sql.csv',
    sla=timedelta(hours=1),
    dag=dag
)

t_send_email_rewards_partner = EmailSQLResultsOperator(
    task_id='t_send_email_rewards_partner',
    sql='sql/rewards_partner_report.sql',
    vertica_conn_id='vertica_conn',
    source='etl@test.com',
    bcc_addresses=('ahirve@test.com','shiggins@test.com', 'mdusenberry@test.com','rganesan@test.com'),
    subject='Rewards Partner Daily Report ({{ ds }})',
    style='attachment',
    filename='rewards_partner_daily_report_{{ ds }}.sql.csv',
    sla=timedelta(hours=1),
    dag=dag
)

t_validate_ww_dim_users = VerticaDataSensor(
    task_id='t_validate_ww_dim_users',
    vertica_conn_id='vertica_conn',
    resourcepool='default',
    table_name='ww.dim_users',
    time_dimension='createdate',
    metrics='count(1)',
    granularity='hour',
    days_offset=0,
    poke_interval=600,
    timeout=7200,
    email=['data-team-standard@gsngames.pagerduty.com', ],
    email_on_retry=False,
    email_on_failure=True,
    retries=0,
    dag=dag,
)

t_validate_ww_internal_transactions = VerticaDataSensor(
    task_id='t_validate_ww_internal_transactions',
    vertica_conn_id='vertica_conn',
    resourcepool='default',
    table_name='ww.internal_transactions',
    time_dimension='trans_date',
    metrics='sum(amount)',
    granularity='hour',
    days_offset=0,
    poke_interval=600,
    timeout=600,
    email=['data-team-standard@gsngames.pagerduty.com', ],
    email_on_retry=False,
    email_on_failure=True,
    retries=0,
    dag=dag,
)


t_validate_ww_dim_users >> t_send_email_swagbucks
t_validate_ww_internal_transactions >> t_send_email_swagbucks

t_validate_ww_dim_users >> t_send_email_inbox_dollars
t_validate_ww_internal_transactions >> t_send_email_inbox_dollars

t_validate_ww_dim_users >> t_send_email_rewards_partner
t_validate_ww_internal_transactions >> t_send_email_rewards_partner