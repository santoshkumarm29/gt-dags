"""Send an email with summary statistics.
Currently includes:
- The app_name and predict_date.
- Summary statistics for the distributions (low, medium, and high).
- The model_version and updated_at.
"""

import jinja2
import os
import premailer

from common.email_utils import send_email
from common.hooks.vertica_hook import VerticaHook
from common.ltr_utils import get_ltr_predictions_df, create_ltr_figures
from common.operators.ml_ltr_operator import MLLTROperator

def send_report_email(ds, vertica_conn_id, receivers, subject, table_name, subject_id_type, model_version, days_tuples, **kwargs):
    connection = VerticaHook(vertica_conn_id=vertica_conn_id, resourcepool='default').get_conn()
    predict_date = MLLTROperator.get_predict_date_from_ds(ds)
    predictions_df = get_ltr_predictions_df(
        table_name=table_name,
        subject_id_type=subject_id_type,
        predict_date=predict_date,
        days_tuples=days_tuples,
        model_version=model_version,
        connection=connection
    )
    url_dct = create_ltr_figures(
        predict_date=predict_date,
        predictions_df=predictions_df,
        model_version=model_version,
        dir_name='worldwinner_ftd_ltr_prediction',
    )

    updated_at = predictions_df['updated_at'].min()
    html = get_email_html(ds, model_version, updated_at, url_dct)
    html = premailer.transform(html)
    subject = subject.format(ds=ds)
    response = send_email(source='etl@gsngames.com', to_addresses=receivers, subject=subject, message=html)
    msg = '{subject} sent to {receivers} response: {response}'
    msg = msg.format(subject=subject, receivers=receivers, response=response)
    return msg

def get_email_html(report_date, model_version, updated_at, url_dct):
    template_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(template_path))
    template = env.get_template('summary_email.html')

    html = template.render(
        report_date=report_date,
        score_dist_desktop_urls=url_dct['score_dist_desktop'],
        score_dist_mobile_urls=url_dct['score_dist_mobile'],
        model_version=model_version,
        updated_at=updated_at,
    )
    return html
