from common.email_utils import send_email
from datetime import datetime
import logging
#from airflow.hooks.postgres_hook import PostgresHook --deprecated
from airflow.providers.postgres.hooks.postgres import PostgresHook
from ua_master_email_config import get_config
from common.hooks.vertica_hook import VerticaHook
from airflow.models import TaskInstance
from os.path import join, dirname

def delete_failed_task_xcom(task_id, dag_id, ts, postgres_conn_id='postgres_main'):
    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    postgres_conn = postgres_hook.get_conn()
    cursor = postgres_conn.cursor()

    sql = """DELETE FROM xcom WHERE dag_id='{dag_id}' AND task_id ='{task_id}' AND execution_date = '{ts}';""".format(
        dag_id=dag_id, task_id=task_id, ts=ts)
    logging.info(sql)
    cursor.execute(sql)
    postgres_conn.commit()
    postgres_conn.close()


def check_blocker_email(ds, receivers, subject, message, **kwargs):
    subject_formatted_date = datetime.strptime(ds, '%Y-%m-%d').strftime('%D')
    subject = subject.format(subject_formatted_date=subject_formatted_date)
    response = send_email(message=message,
                          to_addresses=receivers,
                          subject=subject,
                          source='etl@gsngames.com')

    raise ValueError(subject)


def check_non_blocker_email(ds, receivers, subject, message, **kwargs):
    subject_formatted_date = datetime.strptime(ds, '%Y-%m-%d').strftime('%D')
    subject = subject.format(subject_formatted_date=subject_formatted_date)
    response = send_email(message=message,
                          to_addresses=receivers,
                          subject=subject,
                          source='etl@gsngames.com')
    return True


def final_email_notification(ds, **kwargs):
    ti = kwargs['ti']
    receivers = ['data@gsngames.com', 'airflow_tableau_ua_failure@gsngames.pagerduty.com']
    subject_formatted_date = datetime.strptime(ds, '%Y-%m-%d').strftime('%D')
    dag_id = kwargs.get('dag').dag_id
    ts = kwargs['ts']

    # checker level  task ids
    attribution_task_id = 't_pre_staged_attribution_checker'
    tablaue_task_id = 't_post_tableau_checker'

    # for each checker task ids


    # if the xcom is set then status will be FAIL but if its not set then automatically it means the task was success

    try:
        attribution_status = 'SUCCESS' if ti.xcom_pull(key='ATTRIBUTION',
                                                       task_ids=attribution_task_id) == None else ti.xcom_pull(
            key='ATTRIBUTION', task_ids='t_pre_staged_attribution_checker')
    except:
        attribution_status = 'SUCCESS'
    try:
        tableau_status = 'SUCCESS' if ti.xcom_pull(key='TABLEAU', task_ids=tablaue_task_id) == None else ti.xcom_pull(
            key='TABLEAU', task_ids='t_post_tableau_checker')
    except:
        tableau_status = 'SUCCESS'


    if attribution_status == 'FAIL' or tableau_status == 'FAIL':
        subject = 'Tableau UA data delayed'
        message = """<br>Hello UA team,</br> 
                          <br></br>
                          <br>Today’s Tableau UA reports have not updated yet. The data team has been notified and is investigating. We will keep you updated.</br>
                          """

        response = send_email(message=message,
                              to_addresses=receivers,
                              subject=subject,
                              source='data@gsngames.com')
        if attribution_status == 'FAIL':
            delete_failed_task_xcom(dag_id=dag_id, task_id=attribution_task_id, ts=ts, postgres_conn_id='postgres_main')
        if tableau_status == 'FAIL':
            delete_failed_task_xcom(dag_id=dag_id, task_id=tablaue_task_id, ts=ts, postgres_conn_id='postgres_main')

        raise ValueError(subject)


def final_email_notification_source_check(ds, **kwargs):
    ti = kwargs['ti']
    receivers = ['data@gsngames.com', 'airflow_tableau_ua_failure@gsngames.pagerduty.com']
    subject_formatted_date = datetime.strptime(ds, '%Y-%m-%d').strftime('%D')
    dag_id = kwargs.get('dag').dag_id
    ts = kwargs['ts']

    # checker level  task ids
    attribution_task_id = 't_pre_staged_attribution_checker'
    tablaue_task_id = 't_post_tableau_checker'

    # for each checker task ids


    # if the xcom is set then status will be FAIL but if its not set then automatically it means the task was success
    try:
        attribution_status = 'SUCCESS' if ti.xcom_pull(key='ATTRIBUTION',
                                                       task_ids=attribution_task_id) == None else ti.xcom_pull(
            key='ATTRIBUTION', task_ids='t_pre_staged_attribution_checker')
    except:
        attribution_status = 'SUCCESS'
    try:
        tableau_status = 'SUCCESS' if ti.xcom_pull(key='TABLEAU', task_ids=tablaue_task_id) == None else ti.xcom_pull(
            key='TABLEAU', task_ids='t_post_tableau_checker')
    except:
        tableau_status = 'SUCCESS'

    if attribution_status == 'FAIL' or tableau_status == 'FAIL':
        logging.info('inside attribution , tableue')
        subject = 'Tableau UA data delayed'
        message = """<br>Hello UA team,</br> 
                  <br></br>
                  <br>Today’s Tableau UA reports have not updated yet. The data team has been notified and is investigating. We will keep you updated.</br>
                  """
        logging.info(message)
        response = send_email(message=message,
                              to_addresses=receivers,
                              subject=subject,
                              source='data@gsngames.com')
        if attribution_status == 'FAIL':
            delete_failed_task_xcom(dag_id=dag_id, task_id=attribution_task_id, ts=ts, postgres_conn_id='postgres_main')
        if tableau_status == 'FAIL':
            delete_failed_task_xcom(dag_id=dag_id, task_id=tablaue_task_id, ts=ts, postgres_conn_id='postgres_main')

        raise ValueError(subject)


    else:
        logging.info('inside rerun ')
        logging.info('trynumber')
        logging.info(kwargs['task_instance'].try_number)
        try_number=kwargs['task_instance'].try_number
        receivers=['data@gsngames.com']
        if try_number !=1:
            subject = 'Tableau UA data refreshed'
            message = """<br>Hello UA team,</br> 
                              <br></br>
                              <br>We have resolved the issues leading to any delays in Tableau UA data and it should be refreshed. Please reach out if you have any questions.</br>
                              """

            response = send_email(message=message,
                                  to_addresses=receivers,
                                  subject=subject,
                                  source='data@gsngames.com')
            logging.info(message)
