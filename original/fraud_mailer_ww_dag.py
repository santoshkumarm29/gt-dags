"""
DAG for Fraud Emails
"""
import os
import pathlib

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from fraud_mailer_ww import fraud_mailer_driver as driver


dag = DAG(
    dag_id=driver.dag_id,
    default_args=driver.default_args,
    schedule_interval=driver.schedule,
    catchup=False
)

# Dynamic Task Generation:
my_path = pathlib.Path(__file__).parent.absolute()
sql_path = os.path.join(my_path, 'fraud_mailer_ww/sql')
for email in driver.fraud_email_config:
    task_name = 't_' + email['email_name'].lower().replace(' ', '_')
    with open(os.path.join(sql_path, email['email_sql']), 'rt') as fin:
        email_sql = fin.read()
    body_format = email['body_format']
    to_address = email['to_address']
    from_address = email['from_address']
    subject = email['subject']
    t_fraud_mail = PythonOperator(
        task_id=task_name,
        python_callable=driver.mass_mail,
        op_kwargs={
            'email_sql': email_sql,
            'body_format': body_format,
            'to_address': to_address,
            'from_address': from_address,
            'subject': subject,
        },
        provide_context=True,
        dag=dag
    )
