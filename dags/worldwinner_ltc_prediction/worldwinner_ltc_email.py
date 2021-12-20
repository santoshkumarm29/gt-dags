"""Send an email with summary statistics.

Currently includes:
- The app_name and predict_date.
- The number of players classified in each category.
- A histogram of the distribution of scores.
- The model_version and updated_at.
- Trends of score distributions over the last 30 days.

To be added:
- Calibration of scores from 30 days ago.

"""
import os

from common.email_utils import send_email
from common.hooks.vertica_hook import VerticaHook
from common.ltc_utils import get_predictions_df, get_trend_df, get_ltc_email_html_content #copied
from common.operators.ml_ltc_operator import MLLTCOperator #copied


def send_report_email(ds, vertica_conn_id, receivers, subject, model_version=None, **kwargs):
    connection = VerticaHook(vertica_conn_id=vertica_conn_id, resourcepool='default').get_conn()
    predict_date = MLLTCOperator.get_predict_date_from_ds(ds)
    predictions_df = get_predictions_df(
        table_name='ww.ltc_predictions_historical',
        user_id_column_name='user_id',
        predict_date=predict_date,
        model_version=model_version,
        connection=connection
    )
    trend_df = get_trend_df(
        table_name='ww.ltc_predictions_historical',
        user_id_column_name='user_id',
        predict_date=predict_date,
        model_version=model_version,
        n_days=30,
        connection=connection
    )
    template_dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
    html = get_ltc_email_html_content(
        app_name_nice='WorldWinner',
        template_dir_path=template_dir_path,
        dir_name_prefix='worldwinner_ltc_prediction',
        predict_date=predict_date,
        predictions_df=predictions_df,
        trend_df=trend_df,
    )
    subject = '{}: {}'.format(subject, predict_date)
    response = send_email(source='etl@gsngames.com', to_addresses=receivers, subject=subject, message=html)
    msg = '{subject} sent to {receivers} response: {response}'
    msg = msg.format(subject=subject, receivers=receivers, response=response)
    return msg
