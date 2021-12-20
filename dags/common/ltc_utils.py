"""

"""

from airflow.models import Variable

from collections import OrderedDict
import jinja2
import logging
import matplotlib.pyplot as plt #python module
import numpy as np #python module
import os
import premailer #python module

from common.db_utils import get_dataframe_from_query
from common.plot_utils import format_axis_as_int, rotate_xaxis_labels #copied
from common.s3_utils import upload_mpl_figure_to_s3 #copied


def plot_score_dist(predictions_df):
    fig, ax0 = plt.subplots(nrows=1, ncols=1, figsize=(6, 4))

    n_bins = 20
    label = 'scores'
    ax0.hist(predictions_df['p_churn'], range=(0, 1), bins=n_bins, label=label, histtype="bar", lw=2)
    limits_df = predictions_df[['category', 'p_churn']].groupby('category').agg([np.min, np.max])
    low_min, low_max = limits_df.loc['low', 'p_churn'].values
    med_min, med_max = limits_df.loc['medium', 'p_churn'].values
    high_min, high_max = limits_df.loc['high', 'p_churn'].values
    for i, rectangle in enumerate(ax0.patches):
        x_mid = rectangle.get_x() + (rectangle.get_width() * (1 / 2))
        if x_mid <= low_max:
            rectangle.set_color('g')
        elif x_mid <= med_max:
            rectangle.set_color('y')
        else:
            rectangle.set_color('r')
    ax0.axvline(x=low_max, color='b', alpha=0.5, linestyle=':')
    ax0.axvline(x=high_min, color='b', alpha=0.5, linestyle=':')
    format_axis_as_int(ax0.yaxis)
    ax0.set_title('Distribution of scores: {}'.format(predictions_df['predict_date'].min()), fontsize=18)
    ax0.set_ylabel('frequency')
    ax0.set_xlabel('probability of churn')
    fig.tight_layout()
    return fig


def plot_score_trend(trend_df):
    fig, (ax0, ax1) = plt.subplots(nrows=2, ncols=1, figsize=(6, 8))
    predict_date = trend_df['predict_date'].max()
    colors = OrderedDict([('low', 'g'), ('medium', 'y'), ('high', 'r')])
    for category in colors.keys():
        df = trend_df[trend_df['category'] == category]
        if df.empty:
            # guard against the beginning of the date range
            continue
        # plot scores by category over time
        df.plot.line(x='predict_date', y='user_count', label=category, color=colors[category], ax=ax0)
        ax0.plot_date(x=[predict_date], y=df[df['predict_date'] == predict_date]['user_count'], marker='o',
                      markersize=8.0, color=colors[category])
        # plot score ranges over time
        ax1.fill_between(df['predict_date'].values, df['min_p_churn'], df['max_p_churn'], facecolor=colors[category])
    # format ax0
    ax0.set_title('Count of players by LTC category', fontsize=16)
    ax0.set_ylabel('count of players')
    ax0.set_ylim(bottom=0)
    format_axis_as_int(ax0.yaxis)
    rotate_xaxis_labels(ax0, degrees=30)
    # format ax1
    ax1.set_title('Score ranges by LTC category', fontsize=16)
    ax1.set_title('probability of churn')
    ax1.set_ylim(bottom=0.0, top=1.0)
    rotate_xaxis_labels(ax1, degrees=30)

    fig.tight_layout()
    return fig


def get_predictions_df(table_name, user_id_column_name, predict_date, model_version, connection=None):
    """Retrieve a dataframe of the most recent scores for this `predict_date` and `model_version`.

    """
    query = """
    with ltc_at_model_version_with_row_numbers as (
      select
        *,
        row_number() over (partition by {user_id_column_name}, predict_date order by updated_at desc ) as rn
      from {table_name}
      where predict_type = 'veteran'
        and model_version = {model_version}
    )
    select
      {user_id_column_name} as user_id,
      p_churn,
      category,
      predict_date,
      model_version,
      updated_at
    from ltc_at_model_version_with_row_numbers
    where rn=1
    and predict_date = '{predict_date}'
    """.format(table_name=table_name, user_id_column_name=user_id_column_name,
               predict_date=predict_date, model_version=model_version)
    df = get_dataframe_from_query(query, connection=connection)
    df['category'] = df['category'].astype("category", categories=['low', 'medium', 'high'], ordered=True)
    return df


def get_trend_df(table_name, user_id_column_name, predict_date, model_version, n_days=30, connection=None):
    query = """
    /* ltc trends */
    with ltc_at_model_version_with_row_numbers as (
      /* most up-to-date prediction at a specific model version by predict_date */
      select
        *,
        row_number() over (partition by {user_id_column_name}, predict_date order by updated_at desc ) as rn
      from {table_name}
      where predict_type = 'veteran'
        and model_version = {model_version}
    ), ltc_with_row_numbers as (
      /* most up-to-date prediction at any model version by predict_date */
      select
        *,
        row_number() over (partition by {user_id_column_name}, predict_date order by model_version desc, updated_at desc ) as rn
      from {table_name}
      where predict_type = 'veteran'
    )
    select
      predict_date,
      predict_type,
      category,
      count(1) as record_count,
      count(distinct {user_id_column_name}) as user_count,
      min(p_churn) as min_p_churn,
      max(p_churn) as max_p_churn
    from ltc_with_row_numbers
    where rn=1
    and predict_date between date('{predict_date}') - interval '{n_days} days' and date('{predict_date}') - interval '1 days'
    group by 1, 2, 3
    
    UNION ALL
    
    select
      predict_date,
      predict_type,
      category,
      count(1) as record_count,
      count(distinct {user_id_column_name}) as user_count,
      min(p_churn) as min_p_churn,
      max(p_churn) as max_p_churn
    from ltc_at_model_version_with_row_numbers
    where rn=1
    and predict_date = '{predict_date}'
    group by 1, 2, 3
    
    order by 1, 2, 3
    """.format(table_name=table_name, user_id_column_name=user_id_column_name,
               predict_date=predict_date, model_version=model_version, n_days=n_days)
    df = get_dataframe_from_query(query, connection=connection)
    return df


def save_figure_to_s3(fig, dir_name, filename, predict_date, model_version):
    """
    TODO: Add `days_activity` and `days_churn` to the arguments in order to add them to the prefix template.
    """
    bucket_name = Variable.get("general_s3_bucket_name")
    if bucket_name is None:
        logging.warning('Unable to get bucket_name from environment variable GENERAL_S3_BUCKET_NAME, using AIRFLOW_S3_BUCKET')
        bucket_name = os.getenv('AIRFLOW_S3_BUCKET', 'datateam-airflow-reports')
    prefix = 'reports/ltc/{dir_name}/v{model_version}/ltc_v{model_version}_{predict_date}'
    prefix = prefix.format(
        predict_date=predict_date,
        model_version=model_version,
        dir_name=dir_name,
    )
    url = upload_mpl_figure_to_s3(
        fig,
        bucket_name=bucket_name,
        filename=filename,
        prefix=prefix,
    )
    logging.info('saved file to {}'.format(url))
    plt.close(fig)
    plt.close('all')
    return url


def get_ltc_email_html_content(app_name_nice, template_dir_path, dir_name_prefix, predict_date, 
                               predictions_df, trend_df, template_filename='summary_email.html'):
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(template_dir_path))
    template = env.get_template(template_filename)

    updated_at = predictions_df['updated_at'].min()
    model_version = predictions_df['model_version'].min()

    by_category_df = (
        predictions_df[['category', 'user_id']]
        .groupby('category', as_index=True)
        .count()
        .rename(columns={'user_id': 'count'})
    )

    score_dist_fig = plot_score_dist(predictions_df)
    score_dist_url = save_figure_to_s3(
        fig=score_dist_fig,
        dir_name=dir_name_prefix,
        filename='score_dist',
        predict_date=predict_date,
        model_version=model_version,
    )
    score_trend_fig = plot_score_trend(trend_df)
    score_trend_url = save_figure_to_s3(
        fig=score_trend_fig,
        dir_name=dir_name_prefix,
        filename='score_trend',
        predict_date=predict_date,
        model_version=model_version,
    )
    html = template.render(
        app_name_nice=app_name_nice,
        predict_date=predict_date,
        by_category_html=by_category_df.to_html(),
        score_dist_url=score_dist_url,
        score_trend_url=score_trend_url,
        model_version=model_version,
        updated_at=updated_at,
    )
    html = premailer.transform(html)
    return html
