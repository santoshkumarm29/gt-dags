"""

"""
from airflow.models import Variable

import logging
import matplotlib.pyplot as plt
import os
import pandas as pd
import seaborn as sns

from common.db_utils import get_dataframe_from_query
from common.deprecated import deprecated
from common.plot_utils import format_axis_as_int, format_axis_as_currency_big, rotate_xaxis_labels
from common.s3_utils import upload_mpl_figure_to_s3


@deprecated
def plot_prediction_dist(predictions_df):
    days_observation = predictions_df['days_observation'].min()
    days_horizon = predictions_df['days_horizon'].min()
    fig, (ax0, ax1) = plt.subplots(nrows=1, ncols=2, figsize=(12, 4))
    sns.distplot(predictions_df['predicted_iap_revenue'], ax=ax0)
    ax0.set_title(
        'Distribution of LTV Predictions\n(days_observation={}, days_horizon={})'.format(days_observation, days_horizon),
        fontsize=14,
    )
    (
        predictions_df
        .sort_values('predicted_iap_revenue', ascending=False)
        ['predicted_iap_revenue']
        .reset_index(drop=True)
        .cumsum()
        .div(predictions_df['predicted_iap_revenue'].sum())
        .plot(ax=ax1)
    )
    ax1.set_title(
        'Cumulative Distribution of LTV Predictions\n(days_observation={}, days_horizon={})'.format(days_observation, days_horizon),
        fontsize=14,
    )
    ax1.set_xlim(left=0)
    fig.tight_layout()
    return fig


@deprecated
def plot_prediction_trend(trend_df):
    days_observation = trend_df['days_observation'].min()
    days_horizon = trend_df['days_horizon'].min()
    fig, (ax0, ax1) = plt.subplots(nrows=1, ncols=2, figsize=(12, 4))
    # prediction ranges
    default_color = sns.color_palette()[0]
    trend_df.plot(
        x='install_date',
        y='predicted_iap_revenue_mean',
        label='mean ltv',
        color=default_color,
        ax=ax0
    )
    trend_df.plot(
        x='install_date',
        y='predicted_iap_revenue_p50',
        label='median ltv',
        color=default_color,
        style='--',
        ax=ax0
    )
    ax0.fill_between(
        trend_df['install_date'].values,
        trend_df['predicted_iap_revenue_p25'],
        trend_df['predicted_iap_revenue_p75'],
        color=default_color,
        alpha=0.25,
    )
    ax0.fill_between(
        trend_df['install_date'].values,
        trend_df['predicted_iap_revenue_p025'],
        trend_df['predicted_iap_revenue_p975'],
        color=default_color,
        alpha=0.25,
    )
    ax0.set_title(
        'LTV Prediction Trends\n(days_observation={}, days_horizon={})'.format(days_observation, days_horizon),
        fontsize=14,
    )
    ax0.set_ylim(bottom=0)
    format_axis_as_currency_big(ax0.yaxis)
    rotate_xaxis_labels(ax0, degrees=30)
    # installs
    trend_df.plot(x='install_date', y='install_count', label='installs', ax=ax1)
    ax1.set_title(
        'LTV Prediction Count\n(days_observation={}, days_horizon={})'.format(days_observation, days_horizon),
        fontsize=14,
    )
    ax1.set_ylim(bottom=0)
    format_axis_as_int(ax1.yaxis)
    rotate_xaxis_labels(ax1, degrees=30)
    fig.tight_layout()
    return fig


def get_model_s3_filepaths(model_s3_filepaths, model_version, days_observation, days_horizon):
    """Return a list of full s3 paths to model files that match the criteria.

    """
    # FIXME: move this method so it doesn't require the airflow module to be imported
    from common.operators.ml_ltv_operator import MLLTVOperator
    df = pd.DataFrame(
        [MLLTVOperator.get_model_info_from_filename(f) for f in model_s3_filepaths]
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
    from common.operators.ml_ltv_operator import MLLTVOperator
    df = pd.DataFrame(
        [MLLTVOperator.get_model_info_from_filename(f) for f in model_s3_filepaths]
    )
    df = df[(df['model_version'] == model_version)]
    return sorted(list(df.groupby(['n_days_observation', 'n_days_horizon']).groups.keys()))


def update_dataframe_index_with_days_observation_days_horizon(df, n_days_observation, n_days_horizon):
    """Include 'n_days_observation' and 'n_days_horizon' in the index to avoid later accounting mistakes.
    """
    index_columns = df.index.names
    df = (
        df
        .reset_index()
        .assign(n_days_observation=n_days_observation)
        .assign(n_days_horizon=n_days_horizon)
        .set_index(index_columns + ['n_days_observation', 'n_days_horizon'])
    )
    return df


def get_ltv_predictions_df(table_name, subject_id_type, predict_date, model_version, days_tuples, connection=None):
    """Retrieve a dataframe of the most recent scores for this `report_date` and `model_version`
    and all the (days_observation, days_horizon) pairs in `days_tuples`.

    """
    days_expression = ', '.join(["'{},{}'".format(days_observation, days_horizon)
                                 for days_observation, days_horizon in days_tuples])
    query = """
    select
      subject_id,
      days_observation,
      days_horizon,
      predicted_iap_revenue,
      model_version,
      updated_at
    from (
      select
        subject_id,
        days_observation,
        days_horizon,
        predicted_iap_revenue,
        model_version,
        updated_at,
        row_number() over (partition by ltv.subject_id, days_observation, days_horizon, model_version
                           order by updated_at desc) as rn
      from {table_name} as ltv
      where predict_date = date('{predict_date}')
        and days_observation || ',' || days_horizon in ({days_expression})
        and model_version = {model_version}
        and subject_id_type = '{subject_id_type}'
    ) as ltv
    where ltv.rn = 1
    """.format(
        table_name=table_name,
        subject_id_type=subject_id_type,
        predict_date=predict_date,
        days_expression=days_expression,
        model_version=model_version,
    )
    df = get_dataframe_from_query(query, connection=connection)
    return df


def get_ltv_trend_df(table_name, subject_id_type, predict_date, model_version, days_tuples, n_days=30, connection=None):
    days_expression = ', '.join(["'{},{}'".format(days_observation, days_horizon)
                                 for days_observation, days_horizon in days_tuples])
    query = """
    with ltv_stats_rn as (
      /* include row number for selecting the latest prediction */
      select
        predict_date,
        install_date,
        subject_id,
        days_observation,
        days_horizon,
        predicted_iap_revenue,
        ltv.updated_at,
        row_number() over (partition by ltv.subject_id, days_observation, days_horizon, model_version order by updated_at desc) as rn
      from {table_name} as ltv
      where days_observation || ',' || days_horizon in ({days_expression})
        and model_version = {model_version}
        and ltv.subject_id_type = '{subject_id_type}'
    ), ltv_stats as (
      /* latest prediction */
      select
        predict_date,
        install_date,
        subject_id,
        days_observation,
        days_horizon,
        predicted_iap_revenue,
        percentile_cont(0.01) within group (order by predicted_iap_revenue asc) over (partition by install_date, days_observation, days_horizon) as predicted_iap_revenue_p01,
        percentile_cont(0.025) within group (order by predicted_iap_revenue asc) over (partition by install_date, days_observation, days_horizon) as predicted_iap_revenue_p025,
        percentile_cont(0.05) within group (order by predicted_iap_revenue asc) over (partition by install_date, days_observation, days_horizon) as predicted_iap_revenue_p05,
        percentile_cont(0.25) within group (order by predicted_iap_revenue asc) over (partition by install_date, days_observation, days_horizon) as predicted_iap_revenue_p25,
        percentile_cont(0.50) within group (order by predicted_iap_revenue asc) over (partition by install_date, days_observation, days_horizon) as predicted_iap_revenue_p50,
        percentile_cont(0.75) within group (order by predicted_iap_revenue asc) over (partition by install_date, days_observation, days_horizon) as predicted_iap_revenue_p75,
        percentile_cont(0.95) within group (order by predicted_iap_revenue asc) over (partition by install_date, days_observation, days_horizon) as predicted_iap_revenue_p95,
        percentile_cont(0.975) within group (order by predicted_iap_revenue asc) over (partition by install_date, days_observation, days_horizon) as predicted_iap_revenue_p975,
        percentile_cont(0.99) within group (order by predicted_iap_revenue asc) over (partition by install_date, days_observation, days_horizon) as predicted_iap_revenue_p99
      from ltv_stats_rn
      where rn=1
    ), ltv_summary as (
      select
        predict_date,
        install_date,
        days_observation,
        days_horizon,
        count(distinct subject_id) as install_count,
        avg(predicted_iap_revenue) as predicted_iap_revenue_mean,
        min(predicted_iap_revenue_p01) as predicted_iap_revenue_p01,
        min(predicted_iap_revenue_p025) as predicted_iap_revenue_p025,
        min(predicted_iap_revenue_p05) as predicted_iap_revenue_p05,
        min(predicted_iap_revenue_p25) as predicted_iap_revenue_p25,
        min(predicted_iap_revenue_p50) as predicted_iap_revenue_p50,
        min(predicted_iap_revenue_p75) as predicted_iap_revenue_p75,
        min(predicted_iap_revenue_p95) as predicted_iap_revenue_p95,
        min(predicted_iap_revenue_p975) as predicted_iap_revenue_p975,
        min(predicted_iap_revenue_p99) as predicted_iap_revenue_p99
      from ltv_stats as l
      group by 1, 2, 3, 4
    )
    /* previous 30 days */
    select
      *
    from ltv_summary as l
    where predict_date between date('{predict_date}') - interval '{n_days} days'
                           and date('{predict_date}') - interval '1 days'

    UNION ALL

    /* current prediction */
    select
      *
    from ltv_summary as l
    where predict_date = date('{predict_date}')

    order by 2, 3, 1;

    """.format(
        table_name=table_name,
        subject_id_type=subject_id_type,
        predict_date=predict_date,
        days_expression=days_expression,
        model_version=model_version,
        n_days=n_days,
    )
    df = get_dataframe_from_query(query, connection=connection)
    return df


@deprecated
def create_ltv_figures(predict_date, predictions_df, trend_df, model_version, dir_name):
    """Return a dictionary with keys 'score_dist' and 'score_trend', each having a value of a
    dictionary with keys (days_observation, days_horizon) with values the urls of the figures in S3.

    """
    url_dct = {
        'score_dist': {},
        'score_trend': {},
    }
    for (days_observation, days_horizon), _predictions_df in predictions_df.groupby(['days_observation', 'days_horizon']):
        score_dist_fig = plot_prediction_dist(_predictions_df)
        score_dist_url = save_figure_to_s3(
            fig=score_dist_fig,
            dir_name=dir_name,
            filename='score_dist',
            predict_date=predict_date,
            days_observation=days_observation,
            days_horizon=days_horizon,
            model_version=model_version,
        )
        url_dct['score_dist'][(days_observation, days_horizon)] = score_dist_url
    for (days_observation, days_horizon), _trend_df in trend_df.groupby(['days_observation', 'days_horizon']):
        score_trend_fig = plot_prediction_trend(_trend_df)
        score_trend_url = save_figure_to_s3(
            fig=score_trend_fig,
            dir_name=dir_name,
            filename='score_trend',
            predict_date=predict_date,
            days_observation=days_observation,
            days_horizon=days_horizon,
            model_version=model_version,
        )
        url_dct['score_trend'][(days_observation, days_horizon)] = score_trend_url
    return url_dct


def save_figure_to_s3(fig, dir_name, filename, predict_date, days_observation, days_horizon, model_version):
    # FIXME: refactor for use by LTV and LTC code???
    bucket_name = Variable.get("general_s3_bucket_name")
    if bucket_name is None:
        logging.warning('Unable to get bucket_name from environment variable GENERAL_S3_BUCKET_NAME, using AIRFLOW_S3_BUCKET')
        bucket_name = os.getenv('AIRFLOW_S3_BUCKET', 'datateam-airflow-reports')
    prefix = 'reports/ltv/{dir_name}/v{model_version}/ltv_{days_observation}_{days_horizon}_{model_version}_{predict_date}'
    prefix = prefix.format(
        predict_date=predict_date,
        days_observation=days_observation,
        days_horizon=days_horizon,
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

