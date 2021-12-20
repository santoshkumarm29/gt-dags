## This DAG has a downstream dependency with worldwinner_mobile_ltv_prediction_v ... which needs the features file in S3


from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
import os

from ww_mobile_fixed_observation_feature_getter.ww_mobile_fixed_observation_features_dag_factory import create_dag
from ww_mobile_fixed_observation_feature_getter.ww_mobile_fixed_observation_feature_getter_v2 import get_features_df

from common.dag_utils import is_airflow

FEATURE_VERSION = 2
DAG_VERSION = 1 # Used to force a backfill
features_s3_bucket_name = Variable.get("general_s3_bucket_name")
feature_file_template = "s3://{{ features_s3_bucket_name }}/features/fixed_observations/ww_mobile/v{{ params.feature_version }}/features_{{ params.n_days_observation }}_{{ macros.ds_add(ds, (-1)*params.n_days_observation) }}.csv"
install_date = "{{ macros.ds_add(ds, (-1)*params.n_days_observation) }}"

default_args = {
    'owner': 'data-science',
    'depends_on_past': False,
    'start_date': datetime(2021, 11, 21) if is_airflow() else datetime(2021, 11, 12),
    'email': ['data-science-ml@gsngames.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='ww_mobile_fixed_observation_features_v{feature_version}.{dag_version}'.format(feature_version=FEATURE_VERSION, dag_version=DAG_VERSION),
    default_args=default_args,
    schedule_interval='20 8 * * *',
    max_active_runs=1,
    catchup=True,
    user_defined_macros=dict(feature_version=FEATURE_VERSION, features_s3_bucket_name=features_s3_bucket_name),
)

dag = create_dag(
    dag,
    feature_version=FEATURE_VERSION,
    feature_file_template=feature_file_template,
    features_df_getter=get_features_df,
    install_date=install_date
)
