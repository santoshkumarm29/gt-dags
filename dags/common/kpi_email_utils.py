#!/usr/bin/env python
#  -*- coding: utf-8 -*-
"""Construct and send the daily email.

"""
from __future__ import division

from airflow.models import Variable

import jinja2
import os
from functools import partial
from datetime import datetime, date, timedelta
import calendar
import matplotlib.pyplot as plt
import premailer
from collections import defaultdict

import pandas as pd
import numpy as np

from common.revenue_bar_chart.generate_graph import get_bar_chart
from common.db_utils import get_dataframe_from_query, get_db_connection, vertica_copy
from common.email_utils import send_email_ses, send_email
from common.plot_utils import mpl_figure_to_base64
from common.s3_utils import upload_mpl_figure_to_s3
from common.string_utils import to_currency, to_currency_round, to_integer, to_percent, to_percent_round
from common.hooks.vertica_hook import VerticaHook


def _get_datetime_from_datestr(datestr, format='%Y-%m-%d'):
    return datetime.strptime(datestr, format)


def add_months(sourcedate, months):
    month = sourcedate.month - 1 + months
    year = int(sourcedate.year + month / 12)
    month = month % 12 + 1
    day = min(sourcedate.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def _get_current_and_delta(_curr_df, _prev_df, metric):
    try:
        curr_value = _curr_df.loc[0, metric]
    except KeyError as e:
        curr_value = 0
    try:
        prev_value = _prev_df.loc[0, metric]
    except KeyError as e:
        prev_value = 0
    if prev_value == 0:
        delta_value = 0
    else:
        delta_value = (curr_value - prev_value) / prev_value

    return curr_value, delta_value


def _get_percent_comparison(_df, metric_compare, metric_base):
    try:
        compare_value = _df.loc[0, metric_compare]
    except KeyError as e:
        compare_value = 0
    try:
        base_value = _df.loc[0, metric_base]
    except KeyError as e:
        base_value = 0
    if base_value == 0:
        delta_value = 0
    else:
        delta_value = (compare_value - base_value) / base_value

    return delta_value

def _get_formatted_subject_value(raw_val, format_type, abbreviate=None):
### format_type can be percent, number, or currency
### abbreviate can optionally be set to 'Y' to round numbers to K or M
    if raw_val is None:
        formatted_val = 'N/A'
    else:
        try:
            if format_type == 'percent':
                if raw_val < 0.005 and raw_val > -0.005:
                    formatted_val = 'flat'
                elif raw_val >= 0.005:
                    formatted_val = '+{:.0f}%'.format(raw_val * 100)
                else:
                    formatted_val = '{:.0f}%'.format(raw_val * 100)
            elif abbreviate == 'Y':
                if raw_val >= 1000000:
                    formatted_val = '{:,.2f}M'.format(raw_val / 1000000)
                elif raw_val >= 1000 and raw_val < 1000000:
                    formatted_val = '{:,.0f}K'.format(raw_val / 1000)
                else:
                    formatted_val = '{:,.0f}'.format(raw_val)
            else:
                formatted_val = '{:,.0f}'.format(raw_val)

            if format_type == 'currency':
                formatted_val = '$' + formatted_val
        except:
            formatted_val = 'N/A'
    return formatted_val


def _process_division_metrics(_df, division_logic_list):
    for metric in division_logic_list:
        metric_name = metric['metric_name']
        try:
            numerator = _df.loc[0, metric['numerator']].astype(int)
        except KeyError as e:
            numerator = 0
        try:
            denominator = _df.loc[0, metric['denominator']].astype(int)
        except KeyError as e:
            denominator = 0
        if denominator == 0:
            result = 0
        else:
            result = numerator / denominator

        _df[metric_name] = result

    return _df


def _calculate_rolling_metrics(_append_df, _full_df, metric_list, start_date, end_date):
    rolling_df = (
        _full_df[(_full_df['event_day'] <= end_date) & (_full_df['event_day'] >= start_date)]
            .groupby('app_name', as_index=False)
            .sum()
            .reset_index()
    )
    for metric in metric_list:
        metric_name = "rolling_" + metric
        _append_df[metric_name] = rolling_df[metric]

    return _append_df


def get_studio_metrics_dict(df, studio_name, app_names, mtd=None, sparkline_method='s3'):
    df = df.copy()
    report_date = df['event_day'].max()
    df = df[df['app_name'].isin(app_names)]

    df['studio_name'] = studio_name

    dict = {}

    if mtd:
        month = report_date.month
        prev_month = add_months(report_date, -1).month

        curr_df = (
            df[df['event_day'].map(lambda x: x.month == month)]
                .groupby(['studio_name', 'event_day'], as_index=False)
                .sum()
                # now compute the mean of values across these days
                .groupby('studio_name', as_index=False)
                .mean()
                .reset_index()
        )

        prev_df = (
            df[df['event_day'].map(lambda x: x.month == prev_month)]
                .groupby(['studio_name', 'event_day'], as_index=False)
                .sum()
                # now compute the mean of values across these days
                .groupby('studio_name', as_index=False)
                .mean()
                .reset_index()
        )

        iap_percent_to_budget = _get_percent_comparison(curr_df, 'transactional_bookings', 'budget_iap_rev')
        ad_percent_to_budget = _get_percent_comparison(curr_df, 'advertising_revenue', 'budget_ad_rev')
        iap_rev_budget, delta_iap_rev_budget = _get_current_and_delta(curr_df, prev_df, 'budget_iap_rev')
        ad_rev_budget, delta_ad_rev_budget = _get_current_and_delta(curr_df, prev_df, 'budget_ad_rev')

        dict.update({
            'iap_percent_to_budget': iap_percent_to_budget,
            'ad_percent_to_budget': ad_percent_to_budget,
            'iap_rev_budget': iap_rev_budget,
            'ad_rev_budget': ad_rev_budget
        })

        trend_df = (
            df[df['event_day'] > report_date - timedelta(days=30)].groupby('event_day',
                                                                           as_index=False).sum().reset_index()
        )
    else:

        curr_df = (
            df[df['event_day'] == report_date]
                .groupby('event_day', as_index=False)
                .sum()
                .reset_index()
        )
        prev_df = (
            df[(df['event_day'].dt.dayofweek == report_date.dayofweek) &
               (df['event_day'] < report_date)]
                # for each day, compute the sum for this studio
                .groupby(['studio_name', 'event_day'], as_index=False)
                .sum()
                # now compute the mean of values across these days
                .groupby('studio_name', as_index=False)
                .mean()
                .reset_index()
        )

        trend_df = df.groupby('event_day', as_index=False).sum().reset_index()

    curr_bookings, delta_bookings = _get_current_and_delta(curr_df, prev_df, 'transactional_bookings')
    curr_dau, delta_dau = _get_current_and_delta(curr_df, prev_df, 'daily_active_users')
    curr_installs, delta_installs = _get_current_and_delta(curr_df, prev_df, 'installs')
    curr_ad_rev, delta_ad_rev = _get_current_and_delta(curr_df, prev_df, 'advertising_revenue')
    curr_payers, delta_payers = _get_current_and_delta(curr_df, prev_df, 'payers')
    url_sparkline_bookings = get_sparkline(trend_df, x='event_day', y='transactional_bookings', method=sparkline_method)

    dict.update({
        'curr_bookings': curr_bookings,
        'delta_bookings': delta_bookings,
        'url_sparkline_bookings': url_sparkline_bookings,
        'curr_dau': curr_dau,
        'delta_dau': delta_dau,
        'curr_installs': curr_installs,
        'delta_installs': delta_installs,
        'curr_ad_rev': curr_ad_rev,
        'delta_ad_rev': delta_ad_rev,
        'curr_payers': curr_payers,
        'delta_payers': delta_payers
    })
    return dict


def get_app_metrics_dict(df, app_name, division_logic_list, mtd=None, sparkline_method='s3'):
    report_date = df['event_day'].max()
    df = df[df['app_name'] == app_name]

    if df.empty:
        return None

    dict = {}
    if mtd:
        month = report_date.month
        prev_month = add_months(report_date, -1).month

        curr_df = (
            df[df['event_day'].map(lambda x: x.month == month)]
                .groupby(['app_name'], as_index=False)
                .mean()
                .reset_index()
        )

        prev_df = (
            df[df['event_day'].map(lambda x: x.month == prev_month)]
                .groupby(['app_name'], as_index=False)
                .mean()
                .reset_index()
        )


        iap_percent_to_budget = _get_percent_comparison(curr_df, 'transactional_bookings', 'budget_iap_rev')
        ad_percent_to_budget = _get_percent_comparison(curr_df, 'advertising_revenue', 'budget_ad_rev')

        iap_rev_budget, delta_iap_rev_budget = _get_current_and_delta(curr_df, prev_df, 'budget_iap_rev')
        ad_rev_budget, delta_ad_rev_budget = _get_current_and_delta(curr_df, prev_df, 'budget_ad_rev')

        dict.update({
            'iap_percent_to_budget': iap_percent_to_budget,
            'ad_percent_to_budget': ad_percent_to_budget,
            'ad_rev_budget': ad_rev_budget,
            'iap_rev_budget': iap_rev_budget
        })

        trend_df = (
            df[df['event_day'] > report_date - timedelta(days=30)].groupby('event_day',
                                                                           as_index=False).sum().reset_index()
        )
    else:
        curr_df = df[df['event_day'] == report_date].reset_index()

        prev_df = (
            df[(df['event_day'].dt.dayofweek == report_date.dayofweek) &
               (df['event_day'] < report_date)]
                .groupby('app_name', as_index=False)
                .mean()
                .reset_index()
        )

        # calculate the cumulative sum for rolling ARPDAU
        curr_df = _calculate_rolling_metrics(curr_df, df, ['transactional_bookings', 'daily_active_users'],
                                             report_date + timedelta(days=-6), report_date)
        prev_df = _calculate_rolling_metrics(prev_df, df, ['transactional_bookings', 'daily_active_users'],
                                             report_date + timedelta(days=-13), report_date + timedelta(days=-7))

        # calculate divided metrics
        curr_df = _process_division_metrics(curr_df, division_logic_list)
        prev_df = _process_division_metrics(prev_df, division_logic_list)

        curr_day1_rr, delta_day1_rr = _get_current_and_delta(curr_df, prev_df, 'day1_rr')
        curr_day7_rr, delta_day7_rr = _get_current_and_delta(curr_df, prev_df, 'day7_rr')
        curr_day30_rr, delta_day30_rr = _get_current_and_delta(curr_df, prev_df, 'day30_rr')
        curr_rolling_arpdau, delta_rolling_arpdau = _get_current_and_delta(curr_df, prev_df, 'rolling_arpdau')
        curr_payers, delta_payers = _get_current_and_delta(curr_df, prev_df, 'payers')
        curr_pct_payers, delta_pct_payers = _get_current_and_delta(curr_df, prev_df, 'pct_payers')
        curr_cpi, delta_cpi = _get_current_and_delta(curr_df, prev_df, 'cpi')
        curr_cpi = None if np.isnan(curr_cpi) else curr_cpi
        delta_cpi = None if np.isnan(delta_cpi) else delta_cpi

        dict.update({
            'curr_day1_rr': curr_day1_rr,
            'delta_day1_rr': delta_day1_rr,
            'curr_day7_rr': curr_day7_rr,
            'delta_day7_rr': delta_day7_rr,
            'curr_day30_rr': curr_day30_rr,
            'delta_day30_rr': delta_day30_rr,
            'curr_rolling_arpdau': curr_rolling_arpdau,
            'delta_rolling_arpdau': delta_rolling_arpdau,
            'curr_payers': curr_payers,
            'delta_payers': delta_payers,
            'curr_pct_payers': curr_pct_payers,
            'delta_pct_payers': delta_pct_payers,
            'cpi': curr_cpi,
            'cpi_rr': delta_cpi,
        })

        trend_df = df

    curr_bookings, delta_bookings = _get_current_and_delta(curr_df, prev_df, 'transactional_bookings')
    curr_dau, delta_dau = _get_current_and_delta(curr_df, prev_df, 'daily_active_users')
    curr_installs, delta_installs = _get_current_and_delta(curr_df, prev_df, 'installs')
    curr_ad_rev, delta_ad_rev = _get_current_and_delta(curr_df, prev_df, 'advertising_revenue')

    url_sparkline_bookings = get_sparkline(trend_df, x='event_day', y='transactional_bookings', method=sparkline_method)

    dict.update({
        'curr_bookings': curr_bookings,
        'delta_bookings': delta_bookings,
        'url_sparkline_bookings': url_sparkline_bookings,
        'curr_dau': curr_dau,
        'delta_dau': delta_dau,
        'curr_installs': curr_installs,
        'delta_installs': delta_installs,
        'curr_ad_rev': curr_ad_rev,
        'delta_ad_rev': delta_ad_rev
    })

    return dict

def get_division_logic_list():
    return [{
        'numerator': 'day1_retained',
        'denominator': 'day1_installs',
        'metric_name': 'day1_rr'
    },
        {
            'numerator': 'day7_retained',
            'denominator': 'day7_installs',
            'metric_name': 'day7_rr'
        },
        {
            'numerator': 'day30_retained',
            'denominator': 'day30_installs',
            'metric_name': 'day30_rr'
        },
        {
            'numerator': 'rolling_transactional_bookings',
            'denominator': 'rolling_daily_active_users',
            'metric_name': 'rolling_arpdau'
        },
        {
            'numerator': 'payers',
            'denominator': 'daily_active_users',
            'metric_name': 'pct_payers'
        },
        {
            'numerator': 'ua_cost',
            'denominator': 'ua_installs',
            'metric_name': 'cpi'
        }
    ]


def get_superset_data(df, app_dict_list, superset_name, studio_list, mtd=None, sparkline_method='s3', row_spacing=False):
    # this function will update the studio list with an aggregate record
    app_list = []
    substudio_list = []
    pos = 0
    for a in app_dict_list:
        if superset_name in a['superset']:
            substudio_list.append(a['studio'])
            for app in a['app_names']:
                app_list.append(app)
        elif len(app_list) == 0:
            pos += 1

    supserset_metrics = get_studio_metrics_dict(df, superset_name, app_list, mtd=mtd,
                                              sparkline_method=sparkline_method)
    insert_index = next((index for (index, d) in enumerate(studio_list) if d["name"] == app_dict_list[pos]['studio']), None)
    supserset_data_dict = {'name': superset_name, 'metrics': supserset_metrics, 'app_list': []}

    # add info to help with formatting row spaces in html
    if row_spacing is True:
        supserset_data_dict.update({'row_spacing': True})
        for studio_data_dict in studio_list:
            if studio_data_dict['name'] in substudio_list[0:len(substudio_list) - 1]:
                studio_data_dict.update({'row_spacing': True})


    studio_list.insert(insert_index, supserset_data_dict)
    return studio_list

def get_kpi_data_list(df, app_dict_list, mtd=None, sparkline_method='s3'):
    df = df.fillna(False)  # fill NaN's with False so I can check for them in the html template.
    division_logic_list = get_division_logic_list()
    studio_list = []
    all_app_names = []
    superset_names = []

    for app_dict in app_dict_list:
        app_list = []
        studio_metrics = get_studio_metrics_dict(df, app_dict['studio'], app_dict['app_names'], mtd=mtd,
                                                 sparkline_method=sparkline_method)
        print('studio metrics gotten {}'.format(app_dict['studio']))
        studio_data_dict = {'name': app_dict['studio'], 'metrics': studio_metrics, 'app_list': app_list}
        # tag studios that need sub-studio formatting in template
        if len([x for x in app_dict.get('superset',[])]) > 1:
            studio_data_dict.update({'substudio_flag': True})
        for i in app_dict.get('superset',[]):
            superset_names.append(i)
        studio_list.append(studio_data_dict)
        all_app_names += app_dict['app_names']

        for app_name in app_dict['app_names']:
            print('app metrics starting {}'.format(app_name))
            app_metrics = get_app_metrics_dict(df, app_name, division_logic_list, mtd=mtd,
                                               sparkline_method=sparkline_method)
            print('app metrics gotten {}'.format(app_name))
            app_list.append({'name': app_name, 'metrics': app_metrics})

    # The way get studio metrics works allows us to leverage it for a virtual studio encapsulating everything
    # studio_list = get_superset_data(df, app_dict_list, 'Casino Studio', studio_list, mtd=mtd,sparkline_method=sparkline_method, row_spacing=True)
    superset_names = set(superset_names)
    n = 1
    superset_n = len(superset_names)
    for name in superset_names:
        # last row should be the highest total and should have row_spacing=False for formatting
        if n < superset_n:
            row_spacing = True
        else:
            row_spacing = False

        studio_list = get_superset_data(df, app_dict_list, name, studio_list, mtd=mtd,
                                        sparkline_method=sparkline_method, row_spacing=row_spacing)
        n += 1
    return studio_list


def get_sparkline(df, x, y, method='s3'):
    """Create a sparkline from the dataframe `df` using `x` as the x-axis and `y` as the y-axis.
    If `method` is 's3', the file is uploaded to s3 and the url is returned.
    If `method` is 'base64', the file is base64 encoded and returned as a string.

    """
    dpi = 96
    w = 125 / dpi
    h = 25 / dpi
    date_string = df['event_day'].max()
    fig, ax = plt.subplots(figsize=(w, h), dpi=dpi)
    if df.empty:
        return mpl_figure_to_base64(fig)
    df.sort_values(x).plot.line(x=x, y=y, ax=ax)
    # remove all the chart junk
    ax.set_ylim(bottom=df[y].min() * 0.9, top=df[y].max() * 1.05)
    for k, v in ax.spines.items():
        v.set_visible(False)
    ax.get_xaxis().set_visible(False)
    ax.get_yaxis().set_visible(False)
    ax.legend().set_visible(False)
    if method == 's3':
        bucket_name = Variable.get("general_s3_bucket_name", default_var="datateam-airflow-reports")
        filename = '{}_{}'.format(y, x)
        prefix = 'reports/daily-email/{date_string}'.format(date_string=date_string)
        url = upload_mpl_figure_to_s3(
            fig,
            bucket_name=bucket_name,
            filename=filename,
            prefix=prefix,
        )
        plt.close(fig)
        plt.close('all')
        return url
    elif method == 'base64':
        fig_base64 = mpl_figure_to_base64(fig)
        plt.close(fig)
        plt.close('all')
        return fig_base64
    else:
        raise ValueError('Unhandled save method: "{}"'.format(method))


def get_revenue_graph(iap_rev_historical_df, report_date_str, ww_flag=None):
    date_string = datetime.now()
    fig = get_bar_chart(iap_rev_historical_df, report_date_str, ww_flag=ww_flag)
    bucket_name = Variable.get("general_s3_bucket_name", default_var="datateam-airflow-reports")
    filename = 'iap_revenue_4_week_historical.png'
    prefix = 'reports/daily-email/{date_string}'.format(date_string=date_string)
    print('about to upload figure to s3')
    url = upload_mpl_figure_to_s3(
        fig,
        bucket_name=bucket_name,
        filename=filename,
        prefix=prefix,
        bbox_inches='tight'
    )
    print(url)
    plt.close(fig)
    plt.close('all')
    return url


def get_kpi_df(report_date, connection=None):
    query = """
        select /*+ LABEL ('airflow-all-etl_gsn_daily_kpi_email-gsn_mobile_daily_email_driver')*/
            d.thedate as event_day,
            d.app_name,
            transactional_bookings,
            coalesce(advertising_revenue, 0) as advertising_revenue,
            cast(daily_active_users as int) as daily_active_users,
            cast(installs as int) as installs,
            cast(day1_retained as INT) as day1_retained,
            cast(day7_retained as INT) as day7_retained,
            cast(day30_retained as INT) as day30_retained,
            cast(day1_installs as INT) as day1_installs,
            cast(day7_installs as INT) as day7_installs,
            cast(day30_installs as INT) as day30_installs,
            cast(ua_installs AS INT) as ua_installs,
            cast(ua_cost AS DECIMAL) as ua_cost,
            payers
        from
        (
        select thedate, app_name from newapi.dim_date x
            cross join
        (
            select distinct app_name from kpi.dim_app_daily_metrics_latest
            where event_day = date('{report_date}') 
        ) y
        where thedate between date('{report_date}') - interval '28 days' and date('{report_date}')
        ) d
        left join
        kpi.dim_app_daily_metrics_latest as m
        on d.thedate = m.event_day and d.app_name = m.app_name
        left join kpi.ua_daily_cpi ua
          on m.app_name = ua.app_name
         and ua.event_day = m.event_day
        order by d.thedate desc, d.app_name asc;
        """.format(report_date=report_date)
    df = get_dataframe_from_query(query, connection=connection, parse_dates=['event_day'])
    #df.event_day = pd.to_datetime(df.event_day).dt.date
    df = df.fillna(0)

    ## TODO: parameterize?
    #df[['daily_active_users', 'installs', 'day1_retained', 'day7_retained', 'day30_retained', 'day1_installs',
    #    'day7_installs', 'day30_installs', 'payers', 'ua_installs']].apply(lambda x: x.astype(int))
    return df

def get_mtd_kpi_df(report_date, connection=None):
    # if you update the forecast logic in this query
    # update the validation task that checks for future week of forecast data:
    # https://github.com/gsnsocial/data-products-airflow-service-dags/blob/master/dags/gsn_mobile_daily_email/sql/t_validate_forecasts.sql

    query = """
    select /*+ LABEL ('airflow-all-etl_gsn_daily_kpi_email-gsn_mobile_daily_email_driver')*/
        d.thedate as event_day,
        d.app_name,
        transactional_bookings,
        coalesce(advertising_revenue, 0) as advertising_revenue,
        daily_active_users,
        installs,
        budget_ad_rev,
        budget_iap_rev
    from
    (
        select thedate, app_name from newapi.dim_date x
            cross join
        (
            select distinct app_name from kpi.dim_app_daily_metrics_latest
             where event_day between date('{report_date}') - interval '60 days' and date('{report_date}')
        ) y
        where thedate between date('{report_date}') - interval '60 days' and date('{report_date}')
    ) d
    left join
    kpi.dim_app_daily_metrics_latest a
    on d.thedate = a.event_day and d.app_name = a.app_name
    left join
    (
        select
        event_day,
        case
               when app_name = 'TriPeaks' then 'TriPeaks Solitaire'
               when app_name = 'WorldWinner' then 'GSN Cash Games'
               when app_name = 'Casino Mobile' then 'GSN Casino'
               when app_name = 'Casino Canvas' then 'GSN Canvas'
               when app_name = 'Wheel of Fortune Slots' then 'Wheel of Fortune Slots 2.0'
               else app_name
        end as app_name,
        forecast_iap as budget_iap_rev,
        forecast_ad as budget_ad_rev
        from kpi.dim_forecast_by_app
        where event_day between date('{report_date}') - interval '60 days' and date('{report_date}')) b
    on a.event_day = b.event_day and a.app_name = b.app_name
    """.format(report_date=report_date)
    df = get_dataframe_from_query(query, connection=connection, parse_dates=['event_day'])
    #df.event_day = pd.to_datetime(df.event_day).dt.date
    df = df.fillna(0)

    return df

def get_iap_revenue_historical_df(report_date, connection=None):
    report_datetime = datetime.strptime(report_date, '%Y-%m-%d')
    yesterday = datetime.strftime(report_datetime, '%Y-%m-%d')
    one_week_from_yesterday = datetime.strftime(report_datetime - timedelta(days=7), '%Y-%m-%d')
    two_weeks_from_yesterday = datetime.strftime(report_datetime - timedelta(days=14), '%Y-%m-%d')
    three_weeks_from_yesterday = datetime.strftime(report_datetime - timedelta(days=21), '%Y-%m-%d')
    four_weeks_from_yesterday = datetime.strftime(report_datetime - timedelta(days=28), '%Y-%m-%d')
    query = """
      select app_name, 
      SUM(case when event_day = '{yesterday}' then transactional_bookings else 0 end) as yesterdays_bookings,
      SUM(case when event_day = '{one_week_from_yesterday}' then transactional_bookings else 0 end) as last_7_bookings,
      SUM(case when event_day = '{two_weeks_from_yesterday}' then transactional_bookings else 0 end) as last_14_bookings,
      SUM(case when event_day = '{three_weeks_from_yesterday}' then transactional_bookings else 0 end) as last_21_bookings,
      SUM(case when event_day = '{four_weeks_from_yesterday}' then transactional_bookings else 0 end) as last_28_bookings
      from kpi.dim_app_daily_metrics_latest
      group by app_name
    """.format(yesterday=yesterday,
               one_week_from_yesterday=one_week_from_yesterday,
               two_weeks_from_yesterday=two_weeks_from_yesterday,
               three_weeks_from_yesterday=three_weeks_from_yesterday,
               four_weeks_from_yesterday=four_weeks_from_yesterday)
    return get_dataframe_from_query(query, connection=connection)

def get_custom_annotations(report_date, connection=None,auto_annotate=True):
    query = """
    SELECT  /*+ LABEL ('airflow-all-etl_gsn_daily_kpi_email-gsn_mobile_daily_email_driver')*/ app_name, annotation
    FROM (
        SELECT app_name, annotation, ROW_NUMBER() OVER(PARTITION BY app_name ORDER BY is_auto asc, created_time DESC) AS rn
        FROM kpi.daily_email_annotations
        WHERE report_date = '{report_date}'
        and ( case when {auto_annotate} then True else not is_auto end ) 
        ) zz
    WHERE rn = 1;""".format(report_date=report_date,auto_annotate=auto_annotate)
    cursor = connection.cursor('dict')
    cursor.execute(query)
    results = cursor.fetchall()
    ret = {}
    for result in results:
        ret[result['app_name']] = result['annotation']
    return ret


def get_4week_delta_date_range_str(report_date):
    report_datetime = datetime.strptime(report_date, '%Y-%m-%d')
    day_offset = timedelta(days=1)
    four_week_offset = timedelta(weeks=4)

    start_day = report_datetime - four_week_offset - day_offset
    end_day = report_datetime - day_offset
    weekday = report_datetime.weekday()
    weekday_display = report_datetime.strftime('%A')

    dr = pd.date_range(start_day, end_day, name="date")
    df = pd.DataFrame(dr)
    dates_df = (
        df[(df['date'].dt.dayofweek == weekday) &
           (df['date'] < report_date)]
    )

    dates = dates_df['date'].tolist()
    dates_display = ", ".join([date.strftime('%m/%d') for date in dates])

    return "% changes are compared to the average of the prior 4 {weekday}s ({dates})".format(weekday=weekday_display,
                                                                                              dates=dates_display)
def get_forecast_version(report_date, vertica_conn_id):
    query = """
        SELECT  /*+ LABEL ('airflow-all-etl_gsn_daily_kpi_email-gsn_mobile_daily_email_driver')*/ 
        max(forecast_version) as forecast_version
        FROM kpi.dim_forecast_by_app where event_day >= '{report_date}';""".format(report_date=report_date)
    connection = VerticaHook(vertica_conn_id=vertica_conn_id, resourcepool='default').get_conn()
    cursor = connection.cursor('dict')
    cursor.execute(query)
    results = cursor.fetchall()
    print('forecast version: {}'.format(results))
    return results[0]['forecast_version']

def get_formatted_subject(subject, report_date, daily_kpi_data_list, subject_key_name):
    subject_values = dict()

    for i in daily_kpi_data_list:
        if i['name'] == subject_key_name:
            rev = i['metrics'].get('curr_bookings', None)
            subject_values.update({'rev': _get_formatted_subject_value(rev, 'currency', abbreviate='Y')})

            delta_rev = i['metrics'].get('delta_bookings', None)
            subject_values.update({'delta_rev': _get_formatted_subject_value(delta_rev, 'percent')})

            installs = i['metrics'].get('curr_installs', None)
            subject_values.update({'installs': _get_formatted_subject_value(installs, 'number', abbreviate='Y')})

            delta_installs = i['metrics'].get('delta_installs', None)
            subject_values.update({'delta_installs': _get_formatted_subject_value(delta_installs, 'percent')})

            dau = i['metrics'].get('curr_dau', None)
            subject_values.update({'dau': _get_formatted_subject_value(dau, 'number', abbreviate='Y')})

            delta_dau = i['metrics'].get('delta_dau', None)
            subject_values.update({'delta_dau': _get_formatted_subject_value(delta_dau, 'percent')})

    # subject_values.update({'subject_formatted_date': _get_datetime_from_datestr(report_date).strftime('%D')})
    subject = subject.format( **subject_values )

    return subject

def get_email_html(report_date, app_dict_list, df, mtd_df, iap_rev_historical_df, subject, template_path, subject_key_name, studio_level_fcst,
                   vertica_conn_id, annotations=None, sparkline_method='s3', ads_data=False, kpi_qa_update=False, ww_flag=None):
    daily_kpi_data_list = get_kpi_data_list(df, app_dict_list, sparkline_method=sparkline_method)
    mtd_kpi_data_list = get_kpi_data_list(mtd_df, app_dict_list, mtd=True, sparkline_method=sparkline_method)
    iap_rev_historical_url = get_revenue_graph(iap_rev_historical_df, report_date, ww_flag=ww_flag)

    if ads_data is True:
        daily_kpi_data_list, mtd_kpi_data_list = mark_non_ads_games(daily_kpi_data_list, mtd_kpi_data_list)

    if len(studio_level_fcst) > 0:
        print('marking non forecasts')
        mtd_kpi_data_list = mark_non_forecast_games(mtd_kpi_data_list, studio_level_fcst)

    print(daily_kpi_data_list)
    print(mtd_kpi_data_list)

    if kpi_qa_update is True:
        print('updating KPI QA table in Vertica...')
        update_kpi_qa(daily_kpi_data_list, mtd_kpi_data_list, report_date, vertica_conn_id)

    template_dir = os.path.dirname(template_path)
    template_file = template_path.split('/')[-1]

    env = jinja2.Environment(loader=jinja2.FileSystemLoader(template_dir))
    env.filters['to_currency'] = to_currency
    env.filters['to_currency_round'] = to_currency_round
    env.filters['to_percent'] = partial(to_percent, precision=1)
    env.filters['to_percent_round'] = to_percent_round
    env.filters['to_integer'] = to_integer
    template = env.get_template(template_file)
    report_date_dt = _get_datetime_from_datestr(report_date)
    formatted_date = report_date_dt.strftime('%A, %D')
    formatted_month = report_date_dt.strftime('%B %Y')
    # This gets the last day of last month
    unformatted_last_month = report_date_dt - timedelta(days=report_date_dt.day)
    formatted_last_month = unformatted_last_month.strftime('%B %Y')
    delta_date_range = get_4week_delta_date_range_str(report_date)
    forecast_version = get_forecast_version(report_date, vertica_conn_id)

    html = template.render(
        formatted_date=formatted_date,
        formatted_month=formatted_month,
        formatted_last_month=formatted_last_month,
        daily_kpi_data_list=daily_kpi_data_list,
        mtd_kpi_data_list=mtd_kpi_data_list,
        annotations=annotations,
        delta_date_range=delta_date_range,
        iap_rev_historical_url=iap_rev_historical_url,
        forecast_version=forecast_version
    )

    formatted_subject = get_formatted_subject(subject, report_date, daily_kpi_data_list, subject_key_name)
    return html, formatted_subject


def mark_non_ads_games(daily_kpi_data_list, mtd_kpi_data_list):
    non_ads_games = []
    for studio_list in mtd_kpi_data_list:
        for app_list in studio_list['app_list']:
            app_name = app_list['name']
            if not app_list['metrics'].get('ad_rev_budget', 0):
                app_list['noads'] = True
                non_ads_games.append(app_name)
    for studio_list in daily_kpi_data_list:
        for app_list in studio_list['app_list']:
            if app_list['name'] in non_ads_games:
                app_list['noads'] = True
    return daily_kpi_data_list, mtd_kpi_data_list

def mark_non_forecast_games(mtd_kpi_data_list, studio_level_fcst):
    print('studio lvl fcst {}'.format(studio_level_fcst))
    for studio_list in mtd_kpi_data_list:
        print(studio_list['name'])
        if studio_list['name'] in studio_level_fcst:
            print('marking no forecast')
            for app_list in studio_list['app_list']:
                app_list['no_fcst'] = True
    return mtd_kpi_data_list

def update_kpi_qa(daily_kpi_data_list, mtd_kpi_data_list, report_date, vertica_conn_id):
    studio_daily_metrics = ['curr_ad_rev', 'curr_bookings', 'curr_dau', 'curr_installs',
                            'curr_payers', 'delta_ad_rev', 'delta_bookings', 'delta_dau',
                            'delta_installs', 'delta_payers']

    app_daily_metrics = ['cpi', 'cpi_rr', 'curr_ad_rev', 'curr_bookings',
                         'curr_dau', 'curr_day1_rr', 'curr_day30_rr', 'curr_day7_rr',
                         'curr_installs', 'curr_payers', 'curr_pct_payers',
                         'curr_rolling_arpdau', 'delta_ad_rev', 'delta_bookings', 'delta_dau',
                         'delta_day1_rr', 'delta_day30_rr', 'delta_day7_rr', 'delta_installs',
                         'delta_payers', 'delta_pct_payers', 'delta_rolling_arpdau', ]

    mtd_metrics = ['ad_percent_to_budget', 'ad_rev_budget', 'curr_ad_rev',
                   'curr_bookings', 'curr_dau', 'curr_installs',
                   'delta_ad_rev', 'delta_bookings', 'delta_dau', 'delta_installs',
                   'iap_percent_to_budget', 'iap_rev_budget', ]

    studio_daily_df = metric_json_to_df(daily_kpi_data_list, studio_daily_metrics, mtd=False, level='studio',
                                        report_date=report_date)
    app_daily_df = metric_json_to_df(daily_kpi_data_list, app_daily_metrics, mtd=False, level='app',
                                        report_date=report_date)
    studio_mtd_df = metric_json_to_df(mtd_kpi_data_list, mtd_metrics, mtd=True, level='studio',
                                        report_date=report_date)
    app_mtd_df = metric_json_to_df(mtd_kpi_data_list, mtd_metrics, mtd=True, level='app',
                                        report_date=report_date)

    final_df = pd.concat([studio_daily_df, app_daily_df, studio_mtd_df, app_mtd_df])

    connection = VerticaHook(vertica_conn_id=vertica_conn_id, resourcepool='default').get_conn()
    vertica_copy(connection, final_df, 'kpi.daily_email_output_historical')


def metric_json_to_df(json_list, metric_cols, mtd, level, report_date):
    if level == 'studio':
        df = pd.io.json.json_normalize(json_list)
        df.columns = df.columns.str.replace('metrics.', '')
        df['noads'] = None
    elif level == 'app':
        df = pd.concat([pd.io.json.json_normalize(json_list, 'app_list')[['name', 'noads']],
                        pd.DataFrame(list(pd.io.json.json_normalize(json_list, 'app_list')['metrics']))], axis=1)
    else:
        print('Error: JSON level must be either app or studio.')

    df['mtd'] = mtd
    df['level'] = level
    df['event_day'] = report_date
    df['aggregation_time'] = pd.Timestamp.now(tz='America/Los_Angeles')

    df_tidy = pd.melt(df.reset_index(),
                      id_vars=['name', 'noads', 'level', 'mtd', 'event_day', 'aggregation_time'],
                      value_vars=metric_cols,
                      value_name='value')

    return df_tidy

def send_daily_kpi_email(ds, vertica_conn_id, app_dict_list, template_path, subject,
                         subject_key_name, studio_level_fcst=[], ads_data=False,
                         receivers=[], cc_addresses=[], bcc_addresses=[],
                         auto_annotate=True, kpi_qa_update=False, ww_flag=None,
                         **kwargs):
    """Sends a standard formatted KPI email that's currently used for early reports and
    the company-wide KPI email.
        :param ds: passed from Airflow context - the DAG run and report date
        :type ds: string
        :param vertica_conn_id: Vertica connection ID
        :type vertica_conn_id: string
        :param app_dict_list: list of dicts containing studio and apps for this email
        :type app_dict_list: list
        :param template_path: filename (including path) of the html file used as the email template
        :type template_path: string
        :param subject: subject text to be formatted with variables (e.g. KPIs)
        :type subject: string
        :param subject_key_name: key name of the studio/superset (e.g. GSN Games) referenced for embedded subject KPIs
        :type subject_key_name: string
        :param studio_level_fcst: what studio names to ignore app-level vs. Forecast for, using studio-level forecast instead
        :type studio_level_fcst: list
        :param ads_data: whether to include ad revenue data
        :type ads_data: boolean
        :param receivers: email addresses to receive the email
        :type receivers: list
        :param cc_addresses: cc email addresses to receive the email
        :type cc_addresses: list
        :param bcc_addresses: bcc email addresses to receive the email
        :type bcc_addresses: list
        :param include_auto_annotate: indicates if auto annotate should be turned on or off
        :type include_auto_annotate: bool
        :param kpi_qa_update: indicates if this task should update a table for QA checks on KPI email output
        :type kpi_qa_update: bool
        """

    connection = VerticaHook(vertica_conn_id=vertica_conn_id, resourcepool='default').get_conn()
    kpi_df = get_kpi_df(report_date=ds, connection=connection)
    mtd_kpi_df = get_mtd_kpi_df(report_date=ds, connection=connection)
    iap_rev_historical_df = get_iap_revenue_historical_df(report_date=ds, connection=connection)

    annotations = get_custom_annotations(report_date=ds, connection=connection,auto_annotate=auto_annotate)
    if ads_data is True:
        ads_annotations = check_for_missing_ads_data(report_date=ds, connection=connection)
        annotations = combine_annotations(annotations, ads_annotations)

    html, subject = get_email_html(ds, app_dict_list, kpi_df, mtd_kpi_df, iap_rev_historical_df, subject, template_path,
                                   subject_key_name, studio_level_fcst, vertica_conn_id, annotations=annotations,
                                   sparkline_method='s3', ads_data=ads_data, kpi_qa_update=kpi_qa_update, ww_flag=ww_flag)
    html = premailer.transform(html)

    response = send_email(source='GSN Dashboards <gsndashboards@gsngames.com>', to_addresses=receivers, cc_addresses=cc_addresses, bcc_addresses=bcc_addresses, subject=subject, message=html)
    msg = '{subject} sent to {receivers} response: {response}'
    msg = msg.format(subject=subject, receivers=receivers, response=response)
    return msg


def check_for_missing_ads_data(report_date, connection):
    sources_names = {'adcolony': 'AdColony', 'applovin': 'AppLovin', 'fb_audience_network': 'Facebook Audience Network',
                     'fyber': 'Fyber', 'supersonic': 'Supersonic', 'tapjoy': 'Tapjoy',
                     'truex': 'TrueX', 'unityads': 'UnityAds', 'vungle': 'Vungle', 'ads.earnings(dfp)': 'DoubleClick',
                     'ads.earnings(trialpay)': 'TrialPay',
                     'mopub':'mopub',
                     'chartboost':'chartboost'}
    nice_app_names = {
                      #('bingo bash', 't'): 'Bingo Bash Mobile',
                      ('bingo bash', 'f'): 'Bingo Bash Canvas',
                      ('fresh deck', 't'): 'Fresh Deck Poker',
                      ('gsn casino', 't'): 'GSN Casino',
                      ('gsn casino', 'f'): 'GSN Canvas',
                      ('gsn.com', 'f'): 'GSN.com',
                      ('slots bash', 't'): 'Slots Bash',
                      #('tripeaks solitaire', 't'): 'TriPeaks Solitaire',
                      ('wof slots', 't'): 'Wheel of Fortune Slots 2.0',
                      ('worldwinner.com', 'f'): 'WorldWinner Web'}
    cursor = connection.cursor('dict')
    cursor.execute("SELECT DISTINCT src FROM ads.daily_aggregation WHERE event_date = '{}'".format(report_date))
    present_sources = set((x['src'] for x in cursor.fetchall()))
    absent_sources = set(sources_names.keys()) - present_sources
    appwise_annotations = defaultdict(str)
    if absent_sources:
        # This gets the approximate amount of ad revenue the missing source accounted for last month, by app
        # Vertica python is doing something weird with boolean return values, so converting to string
        query = """
        SELECT  /*+ LABEL ('airflow-all-etl_gsn_daily_kpi_email-gsn_mobile_daily_email_driver')*/ app, CASE WHEN mobile THEN 't' ELSE 'f' END as mobile, src_rev/total_rev AS missing_ratio
        FROM (
            SELECT app,
                   mobile,
                   SUM(CASE WHEN src = '{absent_source}' THEN revenue ELSE 0 END) AS src_rev,
                   SUM(revenue) AS total_rev
            FROM ads.daily_aggregation
            WHERE event_date >= ADD_MONTHS(DATE(DATE_TRUNC('month', DATE('{report_date}'))), - 1)
            AND event_date <= DATE(DATE_TRUNC('month', DATE('{report_date}'))) - 1
            AND app not in ('n/a', 'unknown', 'slots of fun')
            GROUP BY 1, 2
            HAVING SUM(revenue) > 0
        ) rev_by_app
        WHERE src_rev/total_rev > 0.01;
        """
        base_annotation = 'Excludes {source_name} AdRev (~{ratio:.0f}%). '
        for absent_source in absent_sources:
            cursor.execute(query.format(absent_source=absent_source, report_date=report_date))
            for result in cursor.fetchall():
                if (result['app'], result['mobile']) in nice_app_names:
                    appwise_annotations[nice_app_names[(result['app'], result['mobile'])]] += base_annotation.format(
                        source_name=sources_names[absent_source], ratio=result['missing_ratio'] * 100)
    return appwise_annotations


def combine_annotations(custom_annotations, ads_annotations):
    output = custom_annotations.copy()
    for key, value in ads_annotations.items():
        if key in output:
            output[key] += ' ' + value
        else:
            output[key] = value
    return output
