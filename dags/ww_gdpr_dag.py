from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from ww_gdpr_deletion.ww_pii_driver import update_phoenix_events_for_pii, update_phoenix_ds_tables_for_pii
from ww_gdpr_deletion.ww_pii_driver import update_ww_schema_for_pii, update_gsnmobile_schema_for_pii
from ww_gdpr_deletion.ww_pii_driver import update_dim_device_map_for_pii, update_ticket_status

default_args = {
    'owner': 'worldwinner',
    'depends_on_past': False,
    'start_date': datetime(2021, 11, 19),
    'email': ['etl@gsngames.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='ww_pii_gdpr',
    default_args=default_args,
    schedule_interval='00 18 16 1 *',
    max_active_runs=1,
    concurrency=3,
    catchup=False,
)

t_update_phoenix_events_for_pii = PythonOperator(
    task_id='t_update_phoenix_events_for_pii',
    python_callable=update_phoenix_events_for_pii,
    op_kwargs={
        'vertica_conn_id': 'vertica_conn'
    },
    sla=timedelta(hours=1),
    dag=dag
)

t_update_phoenix_ds_tables_for_pii = PythonOperator(
    task_id='t_update_phoenix_ds_tables_for_pii',
    python_callable=update_phoenix_ds_tables_for_pii,
    op_kwargs={
        'vertica_conn_id': 'vertica_conn'
    },
    sla=timedelta(hours=1),
    dag=dag
)

t_update_ww_schema_for_pii = PythonOperator(
    task_id='t_update_ww_schema_for_pii',
    python_callable=update_ww_schema_for_pii,
    op_kwargs={
        'vertica_conn_id': 'vertica_conn'
    },
    sla=timedelta(hours=2),
    dag=dag
)

t_update_gsnmobile_schema_for_pii = PythonOperator(
    task_id='t_update_gsnmobile_schema_for_pii',
    python_callable=update_gsnmobile_schema_for_pii,
    op_kwargs={
        'vertica_conn_id': 'vertica_conn'
    },
    sla=timedelta(hours=2),
    dag=dag
)

t_update_dim_device_map_for_pii = PythonOperator(
    task_id='t_update_dim_device_map_for_pii',
    python_callable=update_dim_device_map_for_pii,
    op_kwargs={
        'vertica_conn_id': 'vertica_conn'
    },
    sla=timedelta(hours=2),
    dag=dag
)

t_final_ticket_update = PythonOperator(
    task_id='t_final_ticket_update',
    python_callable=update_ticket_status,
    op_kwargs={
        'vertica_conn_id': 'vertica_conn'
    },
    sla=timedelta(hours=1),
    dag=dag
)

t_update_phoenix_events_for_pii >> t_update_dim_device_map_for_pii
t_update_phoenix_ds_tables_for_pii >> t_update_dim_device_map_for_pii
t_update_ww_schema_for_pii >> t_update_dim_device_map_for_pii
t_update_gsnmobile_schema_for_pii >> t_update_dim_device_map_for_pii
t_update_dim_device_map_for_pii >> t_final_ticket_update
