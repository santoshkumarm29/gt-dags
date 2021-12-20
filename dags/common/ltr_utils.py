"""
LTR Utils - Visualize daily results from an LTR model.
"""
from airflow.models import Variable

import logging
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import pandas as pd
import numpy as np
import os
import decimal

from common.db_utils import get_dataframe_from_query
from common.operators.ml_ltr_operator import MLLTROperator
from common.s3_utils import upload_mpl_figure_to_s3


def plot_score_dist(predictions_df, n_bins=50):
    """
    predictions_df must be formatted like the Vertica predictions table`
    """
    fig, ax0 = plt.subplots(nrows=1, ncols=1, figsize=(6, 4))

    title_days_observation = ''
    title_days_horizon = ''
    if len(np.unique(predictions_df.days_observation)) == 1:
        title_days_observation = np.unique(predictions_df.days_observation)[0]
    if len(np.unique(predictions_df.days_horizon)) == 1:
        title_days_horizon = np.unique(predictions_df.days_horizon)[0]

    ax0.hist(predictions_df['predicted_probability'], range=(0, 1), bins=n_bins, label=['low', 'medium', 'high'],
             histtype="bar", lw=2, ec='k')
    limits_df = predictions_df[['category', 'predicted_probability']].groupby('category').agg([np.min, np.max])

    # Fix for any group of predictions missing one of the three categories (specifically 1 to 30 day being all high)
    if ['low'] in limits_df.index.values:
        low_min, low_max = limits_df.loc['low', 'predicted_probability'].values
    else:
        low_min, low_max = 0, 0
    if ['medium'] in limits_df.index.values:
        med_min, med_max = limits_df.loc['medium', 'predicted_probability'].values
    else:
        med_min, med_max = 0, 0
    if ['high'] in limits_df.index.values:
        high_min, high_max = limits_df.loc['high', 'predicted_probability'].values
    else:
        high_min, high_max = 0, 0

    for i, rectangle in enumerate(ax0.patches):#
        x_mid = rectangle.get_x() + (rectangle.get_width() * (1 / 2))
        if x_mid <= low_max:
            rectangle.set_color('g')
        elif x_mid <= med_max:
            rectangle.set_color('y')
        else:
            rectangle.set_color('r')
    ax0.axvline(x=low_max, color='b', alpha=0.5, linestyle=':')
    ax0.axvline(x=high_min, color='b', alpha=0.5, linestyle=':')
    tup = (
        '{:2f}%'.format(np.float64(100 * np.sum(predictions_df.category == 'low') / predictions_df.shape[0])),
        '{:2f}%'.format(np.float64(100 * np.sum(predictions_df.category == 'medium') / predictions_df.shape[0])),
        '{:2f}%'.format(np.float64(100 * np.sum(predictions_df.category == 'high') / predictions_df.shape[0])),
    )
    ax0.set_title('Distribution of scores - {}\n{} Days Observation , {} Days Horizon\n {}'.format(
        predictions_df['predict_date'].min(), title_days_observation, title_days_horizon, tup), fontsize=18)
    ax0.set_ylabel('frequency')
    ax0.set_xlabel('Predicted Probability')
    fig.tight_layout()

    # create legend
    handles = [Rectangle((0, 0), 1, 1, color=c, ec="k") for c in ['g', 'y', 'r']]
    labels = ["low", "medium", "high"]
    plt.legend(handles, labels)
    return fig

def get_model_s3_filepaths(model_s3_filepaths, model_version, days_observation, days_horizon):
    """Return a list of full s3 paths to model files that match the criteria.

    """
    df = pd.DataFrame(
        [MLLTROperator.get_model_info_from_filename(f) for f in model_s3_filepaths]
    )
    df = df[(df['model_version'] == model_version) &
            (df['n_days_observation'] == days_observation) &
            (df['n_days_horizon'] == days_horizon)]
    if len(model_s3_filepaths) == 0:
        raise ValueError('Empty list of models passed to get_model_s3_filepaths')
    if df.empty:
        logging.error('No models found for list: {}'.format(model_s3_filepaths))
        raise ValueError('No models found matching the criteria model_version={}, days_observation={}, days_horizon={}'
                         .format(model_version, days_observation, days_horizon))
    return df['model_full_filepath'].values

def get_days_tuples(model_s3_filepaths, model_version):
    """Return a list of unique tuples of (n_days_observation, n_days_observation)
    """
    df = pd.DataFrame(
        [MLLTROperator.get_model_info_from_filename(f) for f in model_s3_filepaths]
    )
    df = df[(df['model_version'] == model_version)]
    return sorted(list(df.groupby(['n_days_observation', 'n_days_horizon']).groups.keys()))

def get_ltr_predictions_df(table_name, subject_id_type, predict_date, days_tuples, model_version, connection=None):
    """Retrieve a dataframe of the most recent scores for this `report_date` and `model_version`.

    """
    days_expression = ', '.join(["'{},{}'".format(days_observation, days_horizon)
                                 for days_observation, days_horizon in days_tuples])
    query = """
    select *
    from (
      select
        *,
        row_number() over (partition by ltr.subject_id, days_observation, days_horizon, model_version
                           order by updated_at desc) as rn
      from {table_name} as ltr
      where predict_date = date('{predict_date}')
        and days_observation || ',' || days_horizon in ({days_expression})
        and model_version = {model_version}
        and subject_id_type = '{subject_id_type}'
    ) as ltr
    where ltr.rn = 1
    """.format(
        table_name=table_name,
        subject_id_type=subject_id_type,
        predict_date=predict_date,
        days_expression=days_expression,
        model_version=model_version,
    )
    df = get_dataframe_from_query(query, connection=connection)
    return df

def create_ltr_figures(predict_date, predictions_df, model_version, dir_name):
    """Return a dictionary with keys 'score_dist', each having a value of a
    dictionary with keys (days_observation, days_horizon) with values the urls of the figures in S3.
    """
    url_dct = {
        'score_dist_desktop': {},
        'score_dist_mobile': {}
    }
    ## Desktop Figures
    for (n_days_observation, n_days_horizon), _predictions_df in predictions_df[predictions_df.played_mobile == 0].groupby(['days_observation', 'days_horizon']):
        score_dist_fig_desktop = plot_score_dist(_predictions_df)
        score_dist_url_desktop = save_figure_to_s3(
            fig=score_dist_fig_desktop,
            dir_name=dir_name,
            filename='score_dist_desktop',
            predict_date=predict_date,
            n_days_observation=n_days_observation,
            n_days_horizon=n_days_horizon,
            model_version=model_version,
        )
        url_dct['score_dist_desktop'][(n_days_observation, n_days_horizon)] = score_dist_url_desktop

    ## Mobile Figures
    for (n_days_observation, n_days_horizon), _predictions_df in predictions_df[predictions_df.played_mobile == 1].groupby(['days_observation', 'days_horizon']):
        score_dist_fig_mobile = plot_score_dist(_predictions_df)
        score_dist_url_mobile = save_figure_to_s3(
            fig=score_dist_fig_mobile,
            dir_name=dir_name,
            filename='score_dist_mobile',
            predict_date=predict_date,
            n_days_observation=n_days_observation,
            n_days_horizon=n_days_horizon,
            model_version=model_version,
        )
        url_dct['score_dist_mobile'][(n_days_observation, n_days_horizon)] = score_dist_url_mobile
    return url_dct

def save_figure_to_s3(fig, dir_name, filename, predict_date, n_days_observation, n_days_horizon, model_version):
    # FIXME: refactor for use by LTV and LTC code???
    bucket_name = Variable.get("general_s3_bucket_name")
    if bucket_name is None:
        logging.warning('Unable to get bucket_name from environment variable GENERAL_S3_BUCKET_NAME, using AIRFLOW_S3_BUCKET')
        bucket_name = os.getenv('AIRFLOW_S3_BUCKET', 'datateam-airflow-reports')
    prefix = 'reports/ltr/{dir_name}/v{model_version}/ltr_{n_days_observation}_{n_days_horizon}_{model_version}_{predict_date}'
    prefix = prefix.format(
        predict_date=predict_date,
        n_days_observation=n_days_observation,
        n_days_horizon=n_days_horizon,
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
