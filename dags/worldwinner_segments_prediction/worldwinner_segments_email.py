"""Send an email with summary statistics.
Currently includes:
- The app_name and predict_date.
- The distribution of players in each group, by segment
- The model_version and updated_at.
"""

import jinja2
import os
import premailer

from common.email_utils import send_email
from common.hooks.vertica_hook import VerticaHook
from common.segments_utils import get_segments_aggregate_df, create_segments_figures
from common.operators.ml_segments_operator import MLSegmentsOperator

def send_report_email(ds, vertica_conn_id, receivers, subject, table_name, subject_id_type, model_version, **kwargs):
    connection = VerticaHook(vertica_conn_id=vertica_conn_id, resourcepool='default').get_conn()
    predict_date = MLSegmentsOperator.get_predict_date_from_ds(ds)
    aggregate_df = get_segments_aggregate_df(
        table_name=table_name,
        subject_id_type=subject_id_type,
        predict_date=predict_date,
        model_version=model_version,
        connection=connection
    )
    url_dct = create_segments_figures(
        predict_date=predict_date,
        aggregate_df=aggregate_df,
        model_version=model_version,
        dir_name='worldwinner_segments_prediction',
    )

    updated_at = aggregate_df['updated_at'].min()
    html = get_email_html(predict_date, model_version, updated_at, url_dct)
    html = premailer.transform(html)
    subject = subject.format(ds=predict_date)
    response = send_email(source='etl@gsngames.com', to_addresses=receivers, subject=subject, message=html)
    msg = '{subject} sent to {receivers} response: {response}'
    msg = msg.format(subject=subject, receivers=receivers, response=response)
    return msg

def get_email_html(predict_date, model_version, updated_at, url_dct):
    template_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(template_path))
    template = env.get_template('summary_email.html')

    html = template.render(
        app_name_nice='WorldWinner',
        predict_date=predict_date,
        cluster_dist_urls=url_dct['cluster_dist'],
        model_version=model_version,
        updated_at=updated_at,
    )
    return html
