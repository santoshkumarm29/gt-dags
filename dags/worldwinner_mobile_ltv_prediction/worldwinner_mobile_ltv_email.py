"""Send an email with summary statistics.

Currently includes:
- The app_name and predict_date.
- The number of devices
- Summary statistics for the distributions.
- The model_version and updated_at.

To be added:
- Trends of LTV distributions over the last 30 days.

"""
import jinja2
import os
import premailer

from common.email_utils import send_email
from common.hooks.vertica_hook import VerticaHook
from common.ltv_utils import get_ltv_predictions_df, get_ltv_trend_df
from common.ltv_plot_utils import create_ltv_figures
from common.operators.ml_ltv_operator import MLLTVOperator


def send_report_email(ds, vertica_conn_id, model_version, days_tuples, table_name, subject_id_type, app_nice_name,
                      receivers=('data-science-ml@gsngames.com',), **kwargs):
    connection = VerticaHook(vertica_conn_id=vertica_conn_id).get_conn()
    predict_date = MLLTVOperator.get_predict_date_from_ds(ds)
    template = get_template()
    html = get_email_html(app_nice_name, predict_date, model_version, days_tuples,
                          table_name, subject_id_type, template, connection)
    subject = '{app_nice_name} LTV - {predict_date}'.format(app_nice_name=app_nice_name, predict_date=predict_date)
    response = send_email(source='etl@gsngames.com', to_addresses=receivers, subject=subject, message=html)
    msg = '{subject} sent to {receivers} response: {response}'
    msg = msg.format(subject=subject, receivers=receivers, response=response)
    return msg


def get_template():
    template_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(template_path))
    template = env.get_template('summary_email.j2.html')
    return template


def get_email_html(app_nice_name, predict_date, model_version, days_tuples, table_name,
                   subject_id_type, template, connection):
    predictions_df = get_ltv_predictions_df(
        table_name=table_name,
        subject_id_type=subject_id_type,
        predict_date=predict_date,
        model_version=model_version,
        days_tuples=days_tuples,
        connection=connection
    )
    trend_df = get_ltv_trend_df(
        table_name=table_name,
        subject_id_type=subject_id_type,
        predict_date=predict_date,
        model_version=model_version,
        days_tuples=days_tuples,
        n_days=30,
        connection=connection,
    )
    url_dct = create_ltv_figures(
        predict_date=predict_date,
        predictions_df=predictions_df,
        trend_df=trend_df,
        model_version=model_version,
        dir_name='worldwinnermobile_ltv_prediction',
    )
    updated_at = predictions_df['updated_at'].min()
    html = template.render(
        app_nice_name=app_nice_name,
        predict_date=predict_date,
        ltv_summary_urls=url_dct['ltv_summary'],
        model_version=model_version,
        updated_at=updated_at,
    )
    html = premailer.transform(html)
    return html
