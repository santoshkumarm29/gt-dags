from common.operators.ml_ltv_operator import MLLTVOperator
from ww_mobile_fixed_observation_feature_getter.ww_mobile_fixed_observation_features_v2 import get_features_df as _get_features_df


def get_features_df(ds, conn, n_days_observation):
    """Entry point to pass to `IndexedMultiDFGetterToS3Operator`.

    """
    predict_date = MLLTVOperator.get_predict_date_from_ds(ds)
    install_date = MLLTVOperator.get_install_date(predict_date, n_days_observation)
    df = _get_features_df(
        install_start_date=install_date,
        install_end_date=install_date,
        n_days_observation=n_days_observation,
        connection=conn,
    )
    df = df.reset_index()
    return df
