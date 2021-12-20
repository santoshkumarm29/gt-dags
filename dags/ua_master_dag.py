from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from ua_master_dag_driver import final_email_notification_source_check
from ua_attribution_dag import ua_attribution_dag
from tableau_dag import tableau_dag
from common.operators.gsn_sensor import GsnSqlSensor
from common.dag_utils import is_airflow


### DO NOT CHANGE THE EXISTING TASK IDS , these task ids are used in task t_final_email_notification to determine the state of emails to be sent out .
### if we HAVE to change the task ids , please make sure to makes corresponding changes to t_final_email_notification


def push_xcom(stage,status, **kwargs):
    # xcom of status of each stages , stage values can be PHOENIX , ATTRIBUTION , TABLEAU / status value can be FAIL for failure
    kwargs['ti'].xcom_push(key=stage, value=status)



default_args = {
    'owner': 'UA',
    'depends_on_past': False,
    'email': ['etl@gsngames.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

dag_id = 'ua_master_main_dag_001'
start_date = datetime(2021, 11, 21)

ua_main_dag = DAG(
    dag_id=dag_id,
    default_args=default_args,
    start_date=start_date,
    schedule_interval='15 9 * * *',  # 2  am PST
    max_active_runs=1,
    catchup=True,
)



# UA ATTRIBUTION SUB DAG CALL

ua_attribution_subdag = SubDagOperator(
    subdag=ua_attribution_dag(dag_id, 'ua_attribution_subdag', ua_main_dag.start_date, None),
    task_id='ua_attribution_subdag',
    dag=ua_main_dag,
    email_on_failure=True,
    email=['etl@gsngames.com']
)


# FINAL UA TABLEAU data creation  SUB DAG CALL

tableau_subdag = SubDagOperator(
    subdag=tableau_dag(dag_id, 'tableau_subdag', ua_main_dag.start_date, None),
    task_id='tableau_subdag',
    dag=ua_main_dag,
    email_on_failure=True,
    email=['etl@gsngames.com']
)


t_dummy_pass = DummyOperator(task_id='t_dummy_pass',
                             dag=ua_main_dag,
                             retries=0,
                             trigger_rule=TriggerRule.ALL_DONE
                             )

t_pre_staged_attribution_checker = PythonOperator(
    task_id='t_pre_staged_attribution_checker',
    python_callable=push_xcom,
    op_kwargs={
        'stage': 'ATTRIBUTION',
        'status': 'FAIL'
    },
    provide_context=True,
    trigger_rule=TriggerRule.ONE_FAILED,
    email_on_failure=False,
    email_on_retry=False,
    retries=0,
    dag=ua_main_dag
)

t_pre_staged_success_checker = DummyOperator(task_id='t_pre_staged_success_checker',
                                             dag=ua_main_dag,
                                             retries=0,
                                             trigger_rule=TriggerRule.ALL_SUCCESS
                                             )

t_post_tableau_checker = PythonOperator(
    task_id='t_post_tableau_checker',
    python_callable=push_xcom,
    op_kwargs={
        'stage': 'TABLEAU',
        'status': 'FAIL'
    },
    provide_context=True,
    trigger_rule=TriggerRule.ONE_FAILED,
    email_on_failure=False,
    email_on_retry=False,
    retries=0,
    dag=ua_main_dag
)

# Smash data checker, smash is loaded by another dag ua_main_dag.smash we have to use gsn sensor to check if the data is loaded already by that job
# t_validate_smash_kocahva_summary_ua_master = GsnSqlSensor(
#         task_id='t_validate_smash_kocahva_summary_ua_master',
#         conn_id='vertica_conn',
#         resourcepool='default',
#         sql='sql1/smash_tasks/t_validate_smash_kocahva_summary_ua_master.sql',
#         sla=timedelta(hours=1),
#         poke_interval=60,
#         timeout=60*60*1,
#         dag=ua_main_dag
#     )
#
# t_post_smash_checker = PythonOperator(
#     task_id='t_post_smash_checker',
#     python_callable=push_xcom,
#     op_kwargs={
#         'stage': 'SMASH',
#         'status': 'FAIL'
#     },
#     provide_context=True,
#     trigger_rule=TriggerRule.ONE_FAILED,
#     email_on_failure=False,
#     email_on_retry=False,
#     retries=0,
#     dag=ua_main_dag
# )


t_final_email_notification = PythonOperator(task_id='t_final_email_notification',
                                             dag=ua_main_dag,
                                             retries=0,
                                             python_callable=final_email_notification_source_check,
                                             provide_context=True,
                                             trigger_rule=TriggerRule.ALL_DONE
                                             )


# MAIN UA subdag
t_dummy_pass >> ua_attribution_subdag
ua_attribution_subdag >> t_pre_staged_attribution_checker
ua_attribution_subdag >> t_pre_staged_success_checker

t_pre_staged_success_checker >> tableau_subdag
tableau_subdag >> t_post_tableau_checker

t_pre_staged_attribution_checker >> t_final_email_notification
t_post_tableau_checker >> t_final_email_notification

