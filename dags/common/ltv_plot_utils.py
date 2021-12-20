import matplotlib.pyplot as plt
import numpy as np
from operator import itemgetter
import pandas as pd
import seaborn as sns

from common.ltv_utils import save_figure_to_s3
from common.plot_utils import (
    mpl_figure_to_base64,
    format_axis_as_int,
    format_axis_as_currency_big,
    rotate_xaxis_labels,
)
from common.stats_utils import bootstrap_sample_iterate


def create_ltv_figures(predict_date, predictions_df, trend_df, model_version, dir_name, image_method='s3'):
    """
    Return a dictionary with keys 'ltv_summary', each having a value of a
    dictionary with keys (days_observation, days_horizon) with values the urls of the figures in S3.

    """
    url_dct = {
        'ltv_summary': {},
    }
    # Sort by `days_horizon` descending, then `days_observation` ascending.
    groups = trend_df[['days_observation', 'days_horizon', 'install_count']].groupby(
        ['days_observation', 'days_horizon'])
    days_tuples = sorted(
        sorted(groups.groups.keys(), key=itemgetter(0), reverse=False),
        key=itemgetter(1),
        reverse=True
    )
    for (days_observation, days_horizon) in days_tuples:
        _predictions_df = predictions_df[
            (predictions_df['days_observation'] == days_observation) &
            (predictions_df['days_horizon'] == days_horizon)
        ]
        _trend_df = trend_df[
            (trend_df['days_observation'] == days_observation) &
            (trend_df['days_horizon'] == days_horizon)
        ]
        summary_fig = plot_ltv_summary(_predictions_df, _trend_df)
        if image_method == 's3':
            summary_fig_url = save_figure_to_s3(
                fig=summary_fig,
                dir_name=dir_name,
                filename='ltv_summary',
                predict_date=predict_date,
                days_observation=days_observation,
                days_horizon=days_horizon,
                model_version=model_version,
            )
        elif image_method == 'base64':
            summary_fig_url = mpl_figure_to_base64(summary_fig)
            plt.close(summary_fig)
            plt.close('all')
        else:
            raise ValueError('Unhandled save method: "{}"'.format(image_method))
        url_dct['ltv_summary'][(days_observation, days_horizon)] = summary_fig_url
    return url_dct


def plot_ltv_summary(predictions_df, trend_df):
    assert predictions_df['days_observation'].nunique() == 1, \
        'Expected a single value for days_observation'
    assert predictions_df['days_horizon'].nunique() == 1, \
        'Expected a single value for days_horizon'
    assert predictions_df['days_observation'].min() == trend_df['days_observation'].min(), \
        'days_observation does not match'
    assert predictions_df['days_horizon'].min() == trend_df['days_horizon'].min(), \
        'days_horizon does not match'
    days_observation = predictions_df['days_observation'].min()
    days_horizon = predictions_df['days_horizon'].min()
    fig, (ax0, ax1, ax2, ax3) = plt.subplots(nrows=1, ncols=4, figsize=(12.5, 2.75))
    plot_predictions_bootstrap(predictions_df, ax0)
    plot_predictions_distribution(predictions_df, ax1)
    plot_trend_timeseries(trend_df, ax2)
    plot_trend_installs(trend_df, ax3)
    fig.suptitle(
        '{}-day horizon, {}-days observation'.format(days_horizon, days_observation),
        # x=0.5,
        # y=1.03,
        fontsize=14,
    )
    fig.tight_layout(rect=[0, 0.03, 1, 0.95])
    return fig


def plot_predictions_bootstrap(predictions_df, ax):
    """
    Plot the predictions distribution using a bootstrap distribution.
    """
    bootstrap_stats = pd.Series(
        bootstrap_sample_iterate(
            predictions_df['predicted_iap_revenue'].values,
            statistic=np.mean,
            n_samples=4000,
            n_jobs=-1,
        )
    )
    p_05, p_95 = bootstrap_stats.quantile(0.05), bootstrap_stats.quantile(0.95)
    sns.distplot(bootstrap_stats, ax=ax)
    ax.set_xlim(left=0)
    ax.axvline(x=p_05, c='k', linestyle=':')
    ax.axvline(x=p_95, c='k', linestyle=':')
    ax.set_title('Bootstrapped LTV', fontsize=12)


def plot_predictions_distribution(predictions_df, ax):
    """
    Plot the predictions distribution below some percentile.
    """
    max_value = predictions_df['predicted_iap_revenue'].quantile(0.95)
    sns.distplot(
        predictions_df[predictions_df['predicted_iap_revenue'] <= max_value]['predicted_iap_revenue'],
        ax=ax
    )
    ax.set_title('LTV Distribution < p95', fontsize=12)


def plot_predictions_cumulative_distribution(predictions_df, ax):
    """
    Plot the predictions cumulative distribution.
    """
    (
        predictions_df
        .sort_values('predicted_iap_revenue', ascending=False)
        ['predicted_iap_revenue']
        .reset_index(drop=True)
        .cumsum()
        .div(predictions_df['predicted_iap_revenue'].sum())
        .plot(ax=ax)
    )
    ax.set_title('LTV Cumulative Distribution', fontsize=12)
    ax.set_xlim(left=0)


def plot_trend_timeseries(trend_df, ax):
    """
    Plot the prediction trend as a timeseries.
    """
    default_color = sns.color_palette()[0]
    trend_df.plot(
        x='install_date',
        y='predicted_iap_revenue_mean',
        label='mean ltv',
        color=default_color,
        ax=ax,
    )
    trend_df.plot(
        x='install_date',
        y='predicted_iap_revenue_p50',
        label='median ltv',
        color=default_color,
        style='--',
        ax=ax,
    )
    ax.fill_between(
        trend_df['install_date'].values,
        trend_df['predicted_iap_revenue_p25'],
        trend_df['predicted_iap_revenue_p75'],
        color=default_color,
        alpha=0.25,
    )
    ax.fill_between(
        trend_df['install_date'].values,
        trend_df['predicted_iap_revenue_p025'],
        trend_df['predicted_iap_revenue_p975'],
        color=default_color,
        alpha=0.25,
    )
    ax.set_title('LTV by Install Date', fontsize=12)
    ax.set_ylim(bottom=0)
    # ax.tick_params(axis='x', labelrotation=30, labelsize=8)
    ax.tick_params(axis='x', labelsize=8)
    rotate_xaxis_labels(ax, degrees=30)
    format_axis_as_currency_big(ax.yaxis)



def plot_trend_installs(trend_df, ax):
    """
    Plot the installs with predictions as a timeseries.
    """
    trend_df.plot(x='install_date', y='install_count', label='installs', ax=ax)
    ax.set_title('LTV Scores by Install Date', fontsize=12)
    ax.set_ylim(bottom=0)
    format_axis_as_int(ax.yaxis)
    # ax.tick_params(axis='x', labelrotation=30, labelsize=8)
    ax.tick_params(axis='x', labelsize=8)
    rotate_xaxis_labels(ax, degrees=30)

