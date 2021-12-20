from airflow.operators.dummy import DummyOperator
from airflow.operators.python import ShortCircuitOperator
from datetime import timedelta

from common.operators.indexed_multi_df_getter_to_s3_operator import IndexedMultiDFGetterToS3Operator
from common.operators.vertica_data_sensor import VerticaDataSensor


def create_dag(dag, feature_version, feature_file_template, features_df_getter, install_date):
    """Return a DAG object with the topology determined by the models available.

    This takes a DAG that has been initialized and completes the graph layout.
    This can be re-used for different model version, separating the model configuration
    from the DAG topology logic.

    """

    ## Fixed lists for all WW mobile tasks
    if feature_version == 2:
        days_observation_values = sorted([3, 7, 14, 30, 60]) # LTV Needs 3, 7, 14, 30, 60
    else:
        days_observation_values = sorted([7])

    # Construct dummy operator to act as validation task aggregators
    t_validate_dummy_dct = {}
    for days_observation in days_observation_values:
        ## ShortCircuit Task.  Is this install date before the app was released?
        ## If not, proceed normally, else all downstream is skipped
        #def date_greater_than_or_equal():
        #    print(install_date)
        #    return(install_date >= "2017-07-11")
        #t_check_if_before_app_released = ShortCircuitOperator(
        #    task_id='t_check_if_before_app_launch_{}d'.format(days_observation),
        #    params={'n_days_observation': days_observation},
        #    python_callable=date_greater_than_or_equal,
        #    dag=dag
        #)

        t_validate_dummy_dct[days_observation] = DummyOperator(
            task_id='t_validate_{}d'.format(days_observation),
            dag=dag,
        )
        # validation tasks
        t_validate_ww_mobile_events_payments_mobile = VerticaDataSensor(
            task_id='t_validate_ww_mobile_events_payments_mobile_{}d'.format(days_observation),
            vertica_conn_id='vertica_conn',
            resourcepool='default',
            table_name='phoenix.tournament_cef',
            time_dimension='event_time',
            metrics='sum(cef)',
            granularity='hour',
            conditionals=["ww_platform in ('app', 'moweb')", "cef > 0"],
            days_offset=-1 * (days_observation + 1),
            dag=dag,
        )
        t_validate_ww_mobile_events_payments_desktop = VerticaDataSensor(
            task_id='t_validate_ww_mobile_events_payments_desktop_{}d'.format(days_observation),
            vertica_conn_id='vertica_conn',
            resourcepool='default',
            table_name='phoenix.tournament_cef',
            time_dimension='event_time',
            metrics='sum(cef)',
            granularity='hour',
            conditionals=["ww_platform = 'desktop'", "cef > 0"],
            days_offset=-1 * (days_observation + 1),
            dag=dag,
        )
        t_validate_ww_mobile_app_installs = VerticaDataSensor(
            task_id='t_validate_ww_mobile_app_installs_{}d'.format(days_observation),
            vertica_conn_id='vertica_conn',
            resourcepool='default',
            table_name='gsnmobile.kochava_device_summary_phoenix',
            time_dimension='install_day',
            metrics='count(distinct idfv)',
            granularity='day',
            days_offset=-1 * (days_observation + 1),
            dag=dag,
        )
        t_validate_ww_mobile_events_payments_mobile >> t_validate_dummy_dct[days_observation]
        t_validate_ww_mobile_events_payments_desktop >> t_validate_dummy_dct[days_observation]
        t_validate_ww_mobile_app_installs >> t_validate_dummy_dct[days_observation]

        #t_check_if_before_app_released >> t_validate_ww_mobile_events_payments_mobile
        #t_check_if_before_app_released >> t_validate_ww_mobile_events_payments_desktop
        #t_check_if_before_app_released >> t_validate_ww_mobile_app_installs

    t_all_feature_queries_complete = DummyOperator(
        task_id='t_all_feature_queries_complete',
        dag=dag,
    )
    
    # Iterate through all (days_observation, days_horizon) tuples
    for days_observation in days_observation_values:
        # build feature dataframe
        t_build_ltv_features_file = IndexedMultiDFGetterToS3Operator(
            task_id='t_{}_build_ltv_features_file'.format(days_observation),
            features_file=feature_file_template,
            features_index_name=['idfv', 'install_date', 'n_days_observation'],
            vertica_conn_id='vertica_conn',
            resourcepool='default',
            s3_conn_id='airflow_log_s3_conn',
            getter_list=[features_df_getter],
            getter_params={'n_days_observation': days_observation},
            params={
                'feature_version': feature_version,
                'n_days_observation': days_observation,
            },
            execution_timeout=timedelta(hours=2),
            dag=dag,
        )
    
        # Build DAG
        t_validate_dummy_dct[days_observation] >> t_build_ltv_features_file
        t_build_ltv_features_file >> t_all_feature_queries_complete

    return dag
