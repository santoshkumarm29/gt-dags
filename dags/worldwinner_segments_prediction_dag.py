"""Worldwinner Segments predictions.
"""
from airflow import DAG
from airflow.models import Variable

from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

from common.operators.indexed_multi_df_getter_to_s3_operator import IndexedMultiDFGetterToS3Operator
from common.segments_utils import get_model_s3_filepaths
from common.operators.ml_segments_operator import MLSegmentsOperator
from worldwinner_segments_prediction.worldwinner_segments_feature_getter import get_features_df
from worldwinner_segments_prediction.worldwinner_segments_email import send_report_email

from common.dag_utils import is_airflow

MODEL_VERSION = 1
DAG_VERSION = 3  # Reset to 0 when bumping up the model version.  Just another way to force a backfill.

prediction_table_name = 'ww.segments_predictions_historical'
features_s3_bucket_name = Variable.get("general_s3_bucket_name")
features_file_template = "s3://{{ features_s3_bucket_name }}/features/segments/worldwinner_segments_predictions/v{{ model_version }}/features_{{ ds }}.csv"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 11, 21) if is_airflow() else datetime(2021, 11, 21), # Ref: BI-8821 usual is_airflow() check Ref: BI-8821,
    'email': ['data-science-ml@gsngames.com', ],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}


# Schedule to run once a week, every Monday morning
dag = DAG(
    dag_id='worldwinner_segments_prediction_v{model_version}.{dag_version}'.format(model_version=MODEL_VERSION,
                                                                            dag_version=DAG_VERSION),
    default_args=default_args,
    schedule_interval='05 14 * * 1',
    max_active_runs=1,
    catchup=True,
    user_defined_macros=dict(model_version=MODEL_VERSION,
                             features_s3_bucket_name=features_s3_bucket_name),
)


model_path_base = f's3://{features_s3_bucket_name}/models/'
model_s3_filepaths_complete = [model_path_base + x for x in [
    'segments/worldwinner/v1/worldwinner_segments_model_v1_2019-07-07.pkl'
]]

t_all_predictions_complete = DummyOperator(
    task_id='t_all_predictions_complete',
    retries=2,
    dag=dag,
)

# Construct dummy operators to act as task aggregators
t_make_predictions_dummy_dct = {}

## PREDICTION TASKS
model_s3_filepaths = get_model_s3_filepaths(
    model_s3_filepaths_complete,
    model_version=MODEL_VERSION
)

# build feature dataframe
t_build_segments_features_file = IndexedMultiDFGetterToS3Operator(
    task_id='t_build_segments_features_file',
    features_file=features_file_template,
    features_index_name=['user_id'],
    vertica_conn_id='vertica_conn',
    resourcepool='default',
    s3_conn_id='airflow_log_s3_conn',
    getter_list=[get_features_df],
    getter_params={'model_s3_filepaths': model_s3_filepaths},
    params={
        'model_version': MODEL_VERSION
    },
    execution_timeout=timedelta(hours=2),
    dag=dag,
)

# make predictions
t_make_segments_predictions = MLSegmentsOperator(
    task_id='t_make_segments_predictions',
    features_s3_filepath=features_file_template,
    model_s3_filepaths=model_s3_filepaths,
    s3_conn_id='airflow_log_s3_conn',
    vertica_conn_id='vertica_conn',
    resourcepool='default',
    table_name=prediction_table_name,
    player_id_column_name='user_id',
    params={
        'model_version': MODEL_VERSION,
    },
    retries=2,
    dag=dag,
)

t_build_segments_features_file >> t_make_segments_predictions
t_make_segments_predictions >> t_all_predictions_complete

"""
Summary email
"""
t_final_send_email = PythonOperator(
    task_id='t_final_send_email',
    python_callable=send_report_email,
    provide_context=True,
    op_kwargs={
        'vertica_conn_id': 'vertica_conn',
        'receivers': ('data-science-ml@gsngames.com', 'etl@gsngames.com',),
        'subject': 'ZEUS: WorldWinner Segments - {ds}',
        'model_version': MODEL_VERSION,
        'table_name': prediction_table_name,
        'subject_id_type': 'user_id',
    },
    sla=timedelta(hours=3),
    retries=2,
    dag=dag,
)

t_all_predictions_complete >> t_final_send_email
