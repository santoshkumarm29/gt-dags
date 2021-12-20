from common.operators.ml_ltr_operator import MLLTROperator
from worldwinner_reg_ltr_prediction.worldwinner_reg_ltr_features_v1 import get_features_df as _get_features_df

def get_features_df(ds, conn, model_s3_filepaths):
    """Entry point to pass to `IndexedMultiDFGetterToS3Operator`.

    """
    predict_date = MLLTROperator.get_predict_date_from_ds(ds)
    # determine the parameters to pass to the feature query getters based on the standard filename
    model_s3_path = MLLTROperator.get_model_s3_path(
        max_date=predict_date,
        date_column_name='report_date', # FIXME: this should be an internal implementation detail, not part of the API
        model_s3_filepaths=model_s3_filepaths,
    )
    model_info = MLLTROperator.get_model_info_from_filename(model_s3_path)
    app_name = model_info['app_name']
    assert app_name == 'worldwinner_reg', 'Expected app_name "worldwinner_reg", instead got "{}"'.format(app_name)
    n_days_observation = model_info['n_days_observation']
    n_days_horizon = model_info['n_days_horizon']

    install_date = MLLTROperator.get_install_date(predict_date, n_days_observation)
    df = _get_features_df(
        install_start_date=install_date,
        install_end_date=install_date,
        n_days_observation=n_days_observation,
        n_days_horizon=n_days_horizon,
        connection=conn,
    )
    df.reset_index()
    return df
