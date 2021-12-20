"""
DAG to score recent installs in Worldwinner Mobile for iap LTV
"""
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import date, datetime, timedelta
import os

from common.ltv_utils import get_model_s3_filepaths
from common.ltv_utils import get_days_tuples
from common.operators.ml_ltv_operator import MLLTVOperator
from common.operators.vertica_data_sensor import VerticaDataSensor
from worldwinner_mobile_ltv_prediction.worldwinner_mobile_ltv_email import send_report_email

from common.dag_utils import is_airflow

MODEL_VERSION = 2
FEATURE_VERSION = 2
DAG_VERSION = 1  # Reset to 0 when bumping up the model version.  Just another way to force a backfill.

prediction_table_name = 'gsnmobile.worldwinner_mobile_ltv_predictions_historical'
features_s3_bucket_name = Variable.get("general_s3_bucket_name")
feature_file_template = "s3://{{ features_s3_bucket_name }}/features/fixed_observations/ww_mobile/v{{ params.feature_version }}/features_{{ params.n_days_observation }}_{{ macros.ds_add(ds, (-1)*params.n_days_observation) }}.csv"

default_args = {
    'owner': 'data-science',
    'depends_on_past': False,
    'start_date': datetime(2020, 9, 30) if is_airflow() else datetime(2020, 1, 23), # Ref: BI-8821 usual is_airflow() check Ref: BI-8821,
    'email': ['data-science-ml@gsngames.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(hours=2),
}

dag = DAG(
    dag_id='worldwinner_mobile_ltv_prediction_v{model_version}.{dag_version}'.format(model_version=MODEL_VERSION,
                                                                            dag_version=DAG_VERSION),
    default_args=default_args,
    schedule_interval='35 9 * * *', # Needs to be after feature gathering @ 20 8 * * *
    max_active_runs=3,
    catchup=True,
    user_defined_macros=dict(model_version=MODEL_VERSION, features_s3_bucket_name=features_s3_bucket_name),
)


model_path_base = f's3://{features_s3_bucket_name}/models/'
model_s3_filepaths_complete = [model_path_base + x for x in [
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_3_90_2018-03-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_3_180_2018-03-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_7_90_2018-03-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_7_180_2018-03-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_14_90_2018-03-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_14_180_2018-03-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_30_90_2018-03-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_30_180_2018-03-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_60_180_2018-03-01.pkl',

    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_3_90_2018-06-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_3_180_2018-06-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_7_90_2018-06-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_7_180_2018-06-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_14_90_2018-06-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_14_180_2018-06-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_30_90_2018-06-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_30_180_2018-06-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_60_180_2018-06-01.pkl',

    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_3_90_2018-09-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_3_180_2018-09-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_7_90_2018-09-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_7_180_2018-09-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_14_90_2018-09-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_14_180_2018-09-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_30_90_2018-09-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_30_180_2018-09-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_60_180_2018-09-01.pkl',

    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_3_90_2018-12-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_3_180_2018-12-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_7_90_2018-12-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_7_180_2018-12-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_14_90_2018-12-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_14_180_2018-12-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_30_90_2018-12-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_30_180_2018-12-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_60_180_2018-12-01.pkl',

    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_3_90_2019-03-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_3_180_2019-03-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_7_90_2019-03-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_7_180_2019-03-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_14_90_2019-03-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_14_180_2019-03-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_30_90_2019-03-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_30_180_2019-03-01.pkl',
    'ltv/worldwinner_mobile/v2/ww_mobile_ltv_model_v2_60_180_2019-03-01.pkl',
]]


days_tuples = get_days_tuples(model_s3_filepaths_complete, model_version=MODEL_VERSION)
days_observation_values = sorted([t[0] for t in days_tuples])

# Construct dummy operators to act as task aggregators
t_validate_dummy_dct = {}
for days_observation in days_observation_values:
    t_validate_dummy_dct[days_observation] = DummyOperator(
        task_id='t_validate_{}d'.format(days_observation),
        dag=dag,
    )
    # validation tasks
    t_validate_worldwinner_mobile_app_installs = VerticaDataSensor(
        task_id='t_validate_worldwinner_mobile_app_installs_{}d'.format(days_observation),
        vertica_conn_id='vertica_conn',
        resourcepool='default',
        table_name='gsnmobile.kochava_device_summary_phoenix',
        time_dimension='install_timestamp',
        metrics='count(distinct idfv)',
        granularity='day',
        ignore_dates=(date(2018, 9, 24),),
        days_offset=-1 * (days_observation + 1),
        dag=dag,
    )
    t_validate_worldwinner_mobile_app_installs >> t_validate_dummy_dct[days_observation]

t_all_predictions_complete = DummyOperator(
    task_id='t_all_predictions_complete',
    dag=dag,
)

# Iterate through all (days_observation, days_horizon) tuples
for days_observation, days_horizon in days_tuples:
    model_s3_filepaths = get_model_s3_filepaths(
        model_s3_filepaths_complete,
        model_version=MODEL_VERSION,
        days_observation=days_observation,
        days_horizon=days_horizon,
    )

    t_make_ltv_predictions = MLLTVOperator(
        task_id='t_{}_{}_make_ltv_predictions'.format(days_observation, days_horizon),
        features_s3_filepath=feature_file_template,
        model_s3_filepaths=model_s3_filepaths,
        s3_conn_id='airflow_log_s3_conn',
        vertica_conn_id='vertica_conn',
        resourcepool='default',
        table_name=prediction_table_name,
        player_id_column_name='idfv',
        params={
            'model_version': MODEL_VERSION,
            'n_days_observation': days_observation,
            'n_days_horizon': days_horizon,
            'feature_version': FEATURE_VERSION
        },
        dag=dag,
    )

    # Build DAG
    t_validate_dummy_dct[days_observation] >> t_make_ltv_predictions
    t_make_ltv_predictions >> t_all_predictions_complete

"""
Summary email
"""
t_final_send_email = PythonOperator(
    task_id='t_final_send_email',
    python_callable=send_report_email,
    op_kwargs={
        'vertica_conn_id': 'vertica_conn',
        'receivers': ('data-science-ml@gsngames.com', 'etl@gsngames.com',),
        'app_nice_name': 'ZEUS: Worldwinner Mobile',
        'model_version': MODEL_VERSION,
        'days_tuples': days_tuples,
        'table_name': prediction_table_name,
        'subject_id_type': 'idfv',
    },
    sla=timedelta(hours=3),
    dag=dag,
)

t_all_predictions_complete >> t_final_send_email
