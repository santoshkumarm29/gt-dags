from airflow import DAG
#from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.subdag import SubDagOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from phoenix_dag import phoenix_dag
from tableau_dag import tableau_dag
from common.operators.gsn_sensor import GsnSqlSensor
from common.operators.jenkins_operator import JenkinsOperator

from common.operators.vertica_operator import VerticaOperator
from airflow.sensors.time_sensor import TimeSensor
import pytz
from datetime import datetime, timedelta, time as t
import time
import logging

from common.dag_utils import is_airflow


dag_id = 'ua_master_skill_dag_000'
start_date = datetime(2021, 11, 21)

default_args = {
    'owner': 'UA',
    'depends_on_past': False,
    'email': ['etl@gsngames.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

ua_master_skill_dag = DAG(
    dag_id=dag_id,
    default_args=default_args,
    schedule_interval='15 9,17 * * *',  # 1:00  am PST/3 00 AM ETC ,  10  AM PST / 1  PM EST
    start_date=start_date,
    max_active_runs=1,
    catchup=False,
)



# Phoenix tasks Start


phoenix_subdag = SubDagOperator(
    subdag=phoenix_dag(dag_id, 'phoenix_subdag', ua_master_skill_dag.start_date, ua_master_skill_dag.schedule_interval),
    task_id='phoenix_subdag',
    dag=ua_master_skill_dag,
    email_on_failure=True,
    email=['etl@gsngames.com']
)



# Phoenix tasks END
# UA attribution update phoenix data START

t_update_phoenix_ua = VerticaOperator(
        task_id='t_update_phoenix_ua',
        vertica_conn_id='vertica_conn',
        resourcepool='default',
        sql='sql/ua_attribution_tasks/t_update_phoenix_ua.sql',
        sla=timedelta(hours=1, minutes=15),
        dag=ua_master_skill_dag
    )

phoenix_subdag >> t_update_phoenix_ua

# UA attribution update phoenix data END

# General Tableau Table refreshes START

tableau_subdag = SubDagOperator(
    subdag=tableau_dag(dag_id, 'tableau_subdag', ua_master_skill_dag.start_date, None),
    task_id='tableau_subdag',
    dag=ua_master_skill_dag,
    email_on_failure=True,
    email=['etl@gsngames.com']
)

t_update_phoenix_ua>>tableau_subdag

# General Tableau Table refreshes END

# UA WW Start
t_validate_gsnmobile_kochava_device_summary_phoenix = GsnSqlSensor(
    task_id='t_validate_gsnmobile_kochava_device_summary_phoenix',
    conn_id='vertica_conn',
    sql='sql/phoenix_tasks/t_validate_gsnmobile_kochava_device_summary_phoenix.sql',
    resourcepool='default',
    sla=timedelta(hours=4),
    timeout=int(60 * 60 * 2),
    retries=0,
    dag=ua_master_skill_dag,
)

t_validate_gsnmobile_tableau_ua_singular_data = GsnSqlSensor(
    task_id='t_validate_gsnmobile_tableau_ua_singular_data',
    conn_id='vertica_conn',
    sql='sql/phoenix_tasks/t_validate_gsnmobile_tableau_ua_singular_data.sql',
    resourcepool='default',
    sla=timedelta(hours=4),
    timeout=int(60 * 60 * 2),
    retries=0,
    dag=ua_master_skill_dag,
)

# original author of this SQL script is Jessica Styx jstix@gsngames.com
t_insert_into_ww_app_acquisition = VerticaOperator(
    task_id='t_insert_into_ww_app_acquisition',
    vertica_conn_id='vertica_conn',
    sql='sql/phoenix_tasks/t_insert_into_ww_app_acquisition.sql',
    resourcepool='default',
    sla=timedelta(hours=4),
    email=['etl@gsngames.com', 'airflow-sla-miss@gsngames.pagerduty.com'],
    dag=ua_master_skill_dag,
)

# The task t_insert_into_ww_app_acquisition will not work if it runs before 0930 UTC (5:30 ET)
# It has something to do with the way the script is written so this task prevents the script from running too early

t_time_sensor = TimeSensor(
    task_id='t_time_sensor',
    target_time=t(10),
    retries=2,
    dag=ua_master_skill_dag,
)

tableau_subdag>>[t_validate_gsnmobile_tableau_ua_singular_data, t_validate_gsnmobile_kochava_device_summary_phoenix,t_time_sensor] >> t_insert_into_ww_app_acquisition

t_validate_ww_app_acquisition = GsnSqlSensor(
    task_id='t_validate_ww_app_acquisition',
    conn_id='vertica_conn',
    sql='sql/phoenix_tasks/t_validate_ww_app_acquisition.sql',
    resourcepool='default',
    timeout=int(60 * 5),  # 5 minutes
    sla=timedelta(hours=4, minutes=30),
    retries=0,
    dag=ua_master_skill_dag,
)

t_insert_into_ww_app_acquisition >> t_validate_ww_app_acquisition

# UA WW END

# Refresh Tableau Job Start

# Tableau-UpdateDataSources-UAMaster-AM
#https://data-jenkins.gsngames.com/view/Tableau/job/Tableau-UpdateDataSources-Skill-app-acquisiton/

# NEEDS FIXING TO CONNECT TO NEW BOX FOR JENKINS
# if is_airflow():
#
#     t_build_jenkins_ua_master = JenkinsOperator(
#                     task_id='t_build_jenkins_ua_master',
#                     job_name='Tableau-UpdateDataSources-UAMaster-AM',
#                     retries=0,
#                     dag=ua_master_skill_dag)
#
#     t_build_jenkins_ua_ww = JenkinsOperator(
#                     task_id='t_build_jenkins_ua_ww',
#                     job_name='Tableau-UpdateDataSources-Skill-app-acquisiton',
#                     retries=0,
#                     dag=ua_master_skill_dag)
#
#     t_insert_into_ww_app_acquisition >> [t_build_jenkins_ua_master, t_build_jenkins_ua_ww]

# Refresh Tableau Job End

