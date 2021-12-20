"""Worldwinner LTC predictions.
"""
from airflow import DAG
from airflow.models import Variable

from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os

from common.operators.indexed_multi_df_getter_to_s3_operator import IndexedMultiDFGetterToS3Operator
from common.operators.ml_ltc_operator import MLLTCOperator #copied
from worldwinner_ltc_prediction.worldwinner_ltc_feature_getter import get_features_df #copied
from worldwinner_ltc_prediction.worldwinner_ltc_email import send_report_email #copied

from common.dag_utils import is_airflow

MODEL_VERSION = 1
DAG_VERSION = 0  # Reset to 0 when bumping up the model version.  Just another way to force a backfill.


features_s3_bucket_name =  Variable.get("general_s3_bucket_name")
features_file_template = "s3://{{ features_s3_bucket_name }}/features/ltc/worldwinner_ltc_predictions/v{{ model_version }}/features_{{ ds }}.csv"

model_path_base = f's3://{features_s3_bucket_name}/models/'
model_s3_filepaths = [model_path_base + x for x in [
    'ltc/worldwinner/v1/worldwinner_ltc_veteran_model_v1_c30_a30_i90_2017-07-01.pkl',
    'ltc/worldwinner/v1/worldwinner_ltc_veteran_model_v1_c30_a30_i90_2017-10-01.pkl',
    'ltc/worldwinner/v1/worldwinner_ltc_veteran_model_v1_c30_a30_i90_2018-01-01.pkl',
    'ltc/worldwinner/v1/worldwinner_ltc_veteran_model_v1_c30_a30_i90_2018-04-01.pkl'
]]

default_args = {
    'owner': 'data-science',
    'depends_on_past': False,
    'start_date': datetime(2021, 11, 21) if is_airflow() else datetime(2021, 11, 21), # Ref: BI-8821 usual is_airflow() check Ref: BI-8821
    'email': ['data-science-ml@gsngames.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='worldwinner_ltc_prediction_v{model_version}_{dag_version}'.format(model_version=MODEL_VERSION,
                                                                           dag_version=DAG_VERSION),
    default_args=default_args,
    schedule_interval='15 13 * * *',
    max_active_runs=6,
    catchup=True,
    user_defined_macros=dict(model_version=MODEL_VERSION, features_s3_bucket_name=features_s3_bucket_name),
)

t_build_features_file = IndexedMultiDFGetterToS3Operator(
    task_id='t_build_features_file',
    # FIXME: can this filename be more unique so that subsequent runs do not clobber feature files?
    # Also this needs to account for model parameters like the churn length
    features_file=features_file_template,
    features_index_name=['user_id', 'start_date'],
    vertica_conn_id='vertica_conn',
    s3_conn_id='airflow_log_s3_conn',
    getter_list=[get_features_df],
    getter_params={'model_s3_filepaths': model_s3_filepaths},
    execution_timeout=timedelta(hours=2),
    resourcepool='default',
    dag=dag,
)

t_make_predictions = MLLTCOperator(
    task_id='t_make_predictions',
    features_s3_filepath=features_file_template,
    model_s3_filepaths=model_s3_filepaths,
    s3_conn_id='airflow_log_s3_conn',
    vertica_conn_id='vertica_conn',
    table_name='ww.ltc_predictions_historical',
    copy_columns=['user_id', 'p_churn', 'category', "predict_type", "predict_date",
                  'updated_at', 'model_version', 'model_filepath', 'uuid'],
    player_id_column_name='user_id',
    dag=dag
)

t_final_send_email = PythonOperator(
    task_id='t_final_send_email',
    python_callable=send_report_email,
    provide_context=True,
    op_kwargs={
        'vertica_conn_id': 'vertica_conn',
        'receivers': ('data-science-ml@gsngames.com', 'etl@gsngames.com',),
        'subject': 'ZEUS: WorldWinner LTC',
        'model_version': MODEL_VERSION,
    },
    sla=timedelta(hours=3),
    dag=dag
)

t_build_features_file >> t_make_predictions
t_make_predictions >> t_final_send_email
