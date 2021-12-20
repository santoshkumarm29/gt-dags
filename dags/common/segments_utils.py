"""
Segments Utils - Visualize results from a segment assignment model.
"""
from airflow.models import Variable

import logging
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import pandas as pd
import numpy as np
import os

from common.db_utils import get_dataframe_from_query
from common.operators.ml_segments_operator import MLSegmentsOperator
from common.s3_utils import upload_mpl_figure_to_s3

def autolabel(ax, rects):
    """Attach a text label above each bar in *rects*, displaying its height."""
    for rect in rects:
        height = rect.get_height()
        ax.annotate('{}'.format(height),
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom')

def plot_cluster_dist(aggregate_df, n_bins=50):
    """
    aggregate_df will be an aggregate player_count df grouped by predict_date and cluster for a given cluster_type`
    """
    fig, ax0 = plt.subplots(nrows=1, ncols=1, figsize=(6, 4)) # FIXME: this isn't used/working

    labels = np.unique(aggregate_df['predict_date'])
    groups = np.unique(aggregate_df['cluster'])
    data_dct = {}
    for group in groups:
        data_dct[group] = aggregate_df[aggregate_df['cluster'] == group]['player_count']

    fig, ax = plt.subplots(figsize=(6, 4))
    _y = len(data_dct)
    _x = np.arange(-1, 1.0001, 2 / _y) / 2
    x = np.arange(len(labels)) * 1.25
    width = np.diff(_x)[0] * .9
    rect_list = []
    for i, (k, v) in enumerate(data_dct.items()):
        rect_list.append(ax.bar(x + _x[i], v, width, label=k))

    for rect in rect_list:
        autolabel(ax, rect)

    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    plt.xticks(rotation=45)
    plt.legend(loc=(1.05, 0))
    fig.tight_layout()

    title_segment = ''
    if len(np.unique(aggregate_df['cluster_type'])) == 1:
        title_segment = np.unique(aggregate_df['cluster_type'])[0]

    ax0.set_title('Distribution of {} - {}'.format(
        title_segment, aggregate_df['predict_date'].max()), fontsize=18)
    ax0.set_ylabel('Player Count')
    ax0.set_xlabel('Cluster Assignment Date')
    fig.tight_layout()

    return fig

def get_model_s3_filepaths(model_s3_filepaths, model_version):
    """Return a list of full s3 paths to model files that match the criteria.

    """
    df = pd.DataFrame(
        [MLSegmentsOperator.get_model_info_from_filename(f) for f in model_s3_filepaths]
    )
    df = df[(df['model_version'] == model_version)]
    if len(model_s3_filepaths) == 0:
        raise ValueError('Empty list of models passed to get_model_s3_filepaths')
    if df.empty:
        logging.error('No models found for list: {}'.format(model_s3_filepaths))
        raise ValueError('No models found matching the criteria model_version={}'
                         .format(model_version))
    return df['model_full_filepath'].values

def get_segments_aggregate_df(table_name, subject_id_type, predict_date, model_version, connection=None):
    """Retrieve a dataframe of the most recent cluster assigment counts for this `report_date` and `model_version`.

    """
    query = """
    with data as (
      select subject_id as {subject_id_type}, predict_date, cluster_type, cluster, updated_at
      from (
        select
        *,
        row_number() over (partition by subject_id, model_version, cluster_type
                           order by updated_at desc) as rn
        from {table_name}
        where predict_date = date('{predict_date}')
        and model_version = {model_version}
      ) as X
      where rn = 1
    )
        select cluster_type, predict_date, cluster, updated_at, count(*) as player_count
        from data
        group by 1,2,3,4
        order by 1,2,3,4
    """.format(
        table_name=table_name,
        subject_id_type=subject_id_type,
        predict_date=predict_date,
        model_version=model_version,
    )
    df = get_dataframe_from_query(query, connection=connection)
    return df

def create_segments_figures(predict_date, aggregate_df, model_version, dir_name):
    """Return a dictionary with keys 'cluster_dist', each having a value of a
    dictionary with keys (cluster_type) with values the urls of the figures in S3.
    """
    url_dct = {
        'cluster_dist': {}
    }
    ## All Cluster Figures
    for cluster_type, _predictions_df in aggregate_df.groupby(['cluster_type']):
        cluster_dist_fig = plot_cluster_dist(_predictions_df)
        cluster_dist_url = save_figure_to_s3(
            fig=cluster_dist_fig,
            dir_name=dir_name,
            filename='cluster_dist',
            predict_date=predict_date,
            cluster_type=cluster_type,
            model_version=model_version
        )
        url_dct['cluster_dist'][cluster_type] = cluster_dist_url
    return url_dct

def save_figure_to_s3(fig, dir_name, filename, predict_date, cluster_type, model_version):
    bucket_name = Variable.get("general_s3_bucket_name")
    if bucket_name is None:
        logging.warning('Unable to get bucket_name from environment variable GENERAL_S3_BUCKET_NAME, using AIRFLOW_S3_BUCKET')
        bucket_name = os.getenv('AIRFLOW_S3_BUCKET', 'datateam-airflow-reports')
    prefix = 'reports/segments/{dir_name}/v{model_version}/segments_{cluster_type}_{model_version}_{predict_date}'
    prefix = prefix.format(
        predict_date=predict_date,
        cluster_type=cluster_type,
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
