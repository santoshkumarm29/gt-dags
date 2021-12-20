from common.operators.ml_segments_operator import MLSegmentsOperator
from worldwinner_segments_prediction.worldwinner_segments_features_v1 import get_features_df as _get_features_df
from datetime import timedelta

def get_features_df(ds, conn, model_s3_filepaths):
    """Entry point to pass to `IndexedMultiDFGetterToS3Operator`.

    """
    predict_date = MLSegmentsOperator.get_predict_date_from_ds(ds)
    # determine the parameters to pass to the feature query getters based on the standard filename
    model_s3_path = MLSegmentsOperator.get_model_s3_path(
        max_date=predict_date,
        date_column_name='report_date', # FIXME: this should be an internal implementation detail, not part of the API
        model_s3_filepaths=model_s3_filepaths,
    )
    model_info = MLSegmentsOperator.get_model_info_from_filename(model_s3_path)
    app_name = model_info['app_name']
    assert app_name == 'worldwinner', 'Expected app_name "worldwinner", instead got "{}"'.format(app_name)

    # some features need to be preprocessed using the scalers that were serialized with the model
    model_dct_all = MLSegmentsOperator.get_model_dct_from_s3_static(model_s3_filepath=model_s3_path, s3_conn_id='airflow_log_s3_conn')

    df = _get_features_df(
        start_date = predict_date - timedelta(days=30),
        n_days_activity = 30,
        n_days_churn = 1,
        n_min_days_since_install = 0,
        connection=conn,
        model_dct_all=model_dct_all
    )
    df = df.reset_index()
    return df