"""
DAG for Fraud Emails
"""
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

import fraud_mailer_ww.fraud_mailer_driver as driver



default_args = {
    'owner': 'skill',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 11, 21),
    'email': ['etl@gametaco.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

fraud_address = ['fraud@gametaco.com']
from_address = 'vertica_fraud_mails@gametaco.com'


dag = DAG(
    dag_id='fraud_mailer_ww_001',
    default_args=default_args,
    schedule_interval='30 13 * * *',
    catchup=False
)

t_fpue_mail = PythonOperator(
        task_id='t_fpue_mail',
        python_callable=driver.mass_mail,
        op_kwargs={
            'email_sql_path': 'fpue.sql',
            'body_format': driver.fpue_format,
            'to_address': fraud_address,
            'from_address': from_address,
            'subject': 'Fraud FPUE Review Alert - {6}',
        },
        dag=dag
    )

t_progressive_mail = PythonOperator(
        task_id='t_progressive_mail',
        python_callable=driver.mass_mail,
        op_kwargs={
            'email_sql_path': 'progressive.sql',
            'body_format': driver.progressive_format,
            'to_address': fraud_address,
            'from_address': from_address,
            'subject': 'Fraud Progressive Review Alert - {7}',
        },
        dag=dag
    )

t_ww_new_players_mail = PythonOperator(
        task_id='t_ww_new_players_mail',
        python_callable=driver.mass_mail,
        op_kwargs={
            'email_sql_path': 'ww_new_players.sql',
            'body_format': driver.ww_new_players_format,
            'to_address': fraud_address,
            'from_address': from_address,
            'subject': 'Cash Games New User - {2}',
        },
        dag=dag
    )

t_takeover_mail = PythonOperator(
        task_id='t_takeover_mail',
        python_callable=driver.mass_mail,
        op_kwargs={
            'email_sql_path': 'ww_takeover.sql',
            'body_format': driver.takeover_format,
            'to_address': fraud_address,
            'from_address': from_address,
            'subject': 'Account Takeover, Overspending, Fraud on WW - {1}',
        },
        dag=dag
    )
