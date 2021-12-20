import time
import logging
from datetime import date, datetime, timedelta
import numpy as np
import pandas as pd
from pandas.api.types import CategoricalDtype

from common.date_utils import convert_to_date
from common.db_utils import get_dataframe_from_query
from common.operators.ml_ltv_operator import MLLTVOperator


def get_logger():
    return logging.getLogger(__name__)

def init_logging(log_level=logging.WARN):
    logging.basicConfig(format='%(levelname)s:%(message)s', level=log_level)
    logging.debug('Logging initialized.')
    logger = get_logger()
    logger.setLevel(log_level)

def update_df_index(df, n_days_observation, n_days_horizon, index_column='idfv'):
    """Include 'n_days_observation' and 'n_days_horizon' in the index to avoid later accounting mistakes.
    """
    df = (
        df
            .reset_index()
            .assign(n_days_observation=n_days_observation)
            .assign(n_days_horizon=n_days_horizon)
            .set_index([index_column, 'n_days_observation', 'n_days_horizon'])
    )
    return df

def get_features_df(ds, conn, model_s3_filepaths):
    """Entry point to pass to `IndexedMultiDFGetterToS3Operator`.

    """
    predict_date = MLLTVOperator.get_predict_date_from_ds(ds)
    # determine the parameters to pass to the feature query getters based on the standard filename
    model_s3_path = MLLTVOperator.get_model_s3_path(
        max_date=predict_date,
        date_column_name='report_date', # FIXME: this should be an internal implementation detail, not part of the API
        model_s3_filepaths=model_s3_filepaths,
    )
    model_info = MLLTVOperator.get_model_info_from_filename(model_s3_path)
    app_name = model_info['app_name']
    assert app_name == 'worldwinner_mobile', 'Expected app_name "worldwinner_mobile", instead got "{}"'.format(app_name)
    n_days_observation = model_info['n_days_observation']
    n_days_horizon = model_info['n_days_horizon']

    install_date = MLLTVOperator.get_install_date(predict_date, n_days_observation)

    df = _get_features_df(
        install_start_date=install_date,
        install_end_date=install_date,
        n_days_observation=n_days_observation,
        n_days_horizon=n_days_horizon,
        connection=conn,
        verbose=False
    ).reset_index()
    df['n_days_observation'] = n_days_observation
    df['n_days_horizon'] = n_days_horizon
    return df


def _get_features_df(install_start_date, install_end_date, n_days_observation, n_days_horizon, connection=None,
                    verbose=False, categorical_encode=True):
    """Create a dataframe with features only (target not included) for installs between
    `install_start_date` and `install_end_date` inclusive.
    install_start_date   Start of the install window.
    install_end_date     End of the install window.  Includes this date.
    n_days_observation   Number of days from install for observation of game behavior.
    n_days_horizon       Number of days from install for the LTP prediction.
    """
    install_start_date = convert_to_date(install_start_date)
    install_end_date = convert_to_date(install_end_date)
    assert install_start_date <= install_end_date, 'install_start_date must be on or before install_end_date'
    assert n_days_observation < n_days_horizon, 'n_days_observation must be less than n_days_horizon'
    assert install_end_date <= date.today() - timedelta(
        days=n_days_observation), 'install_end_date is not distant enough'

    ## Get IDFV Base
    #print(get_idfv_base.__name__)
    df = get_idfv_base(
        install_start_date,
        install_end_date,
        connection=connection,
        categorical_encode=categorical_encode,
        onehot_encode=False,
    )
    #print('User install size: {}'.format(df.shape))
    ## Append All Feature Columns
    feature_callables = [
        get_install_time_info,
        get_attribution_info,
        get_iap_revenue,
        get_previous_days_revenue,
        get_previous_ltd_cef,
        get_events_rfm
    ]
    for feature_callable in feature_callables:
        #print(feature_callable.__name__)
        temp = feature_callable(
            install_start_date,
            install_end_date,
            n_days=n_days_observation,
            connection=connection,
        )
        #print('feature query shape:{}'.format(temp.shape))
        df = pd.merge(
            left=df,
            right=temp,
            how='left',
            left_index=True,
            right_index=True,
        )
        #print('training_df shape after merge:{}'.format(df.shape))

    ## Any feature that should not fillna with zeros is responsible for returning rows for all installs!
    df = df.fillna(0)

    ## Add feature for "stagnant" iap_revenue
    cols = [c for c in df if (c.startswith('day') & c.endswith('_iap_revenue'))]
    if len(cols) <= 1:
        pass
    else:
        feature_name = 'obs_window_equals_previous_window_cef'
        #print('adding feature: {}...'.format(feature_name))
        cols.sort()
        cols = cols[::-1]
        df[feature_name] = 0
        for c in range(1, len(cols)):
            df[feature_name] += (df[cols[0]] == df[cols[c]]) * 1

    ## Fix for what should be numeric columns being queried as objects.
    ## Convert them to float (not sure if this is needed in Airflow, but it was in data-science-notebooks JupyterHub dsstage)
    cols = [c for c in df if ((df[c].dtypes == 'object') & (c != 'idfv') & (c != 'install_timestamp'))]
    # print('cols to fix: {}'.format(cols))
    for col in cols:
        df[col] = df[col].astype(float)

    return df


def get_idfv_base(install_start_date, install_end_date, onehot_encode=False, connection=None, categorical_encode=True):
    """
    """
    query = """
    /* Get install cohort for WW LTV */
    with user_base as (
      select idfv 
      from gsnmobile.kochava_device_summary_phoenix
      where install_timestamp::date between '{install_start_date}' and '{install_end_date}'
    )
      select *
      from user_base;
    """.format(install_start_date=install_start_date, install_end_date=install_end_date)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['idfv'])
    return df


def get_install_date_df(install_start_date, install_end_date, connection=None):
    """
    """
    query = """
    with user_base_with_install_time as (
      select idfv, install_timestamp
      from gsnmobile.kochava_device_summary_phoenix
      where install_timestamp::date between '{install_start_date}' and '{install_end_date}'
    )
      select *
      from user_base_with_install_time;
    """.format(install_start_date=install_start_date, install_end_date=install_end_date)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['idfv'])
    return df


def get_iap_revenue(install_start_date, install_end_date, n_days=7, connection=None):
    """
    """
    query = """
    -- Day N CEF
    with user_base as (
      select idfv, install_timestamp from
      gsnmobile.kochava_device_summary_phoenix
      where install_timestamp::date between '{install_start_date}' and '{install_end_date}'
    ),
    cef_events as (
        SELECT
          idfv,
          event_time,
          cef
        FROM phoenix.tournament_cef
    ),
    day_n_cef as (
        select user_base.idfv, coalesce(sum(cef),0) as day{n_days}_iap_revenue
        from user_base
        left join cef_events
        on user_base.idfv = cef_events.idfv
        and datediff('second', install_timestamp, event_time) between 0 and 86400 * ({n_days})
        group by user_base.idfv
    )
    select *
    from day_n_cef
    """.format(install_start_date=install_start_date, install_end_date=install_end_date, n_days=n_days)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['idfv'])
    return df


def get_events_rfm(install_start_date, install_end_date, n_days=7, connection=None):
    """
    """
    query = """
    -- RFM events
    with user_base as (
      select idfv
      from gsnmobile.kochava_device_summary_phoenix
      where install_timestamp::date between '{install_start_date}' and '{install_end_date}'
    ),
    phoenix_events as (
      SELECT b.install_timestamp, a.*
      FROM phoenix.events_client a
      LEFT JOIN gsnmobile.kochava_device_summary_phoenix b
      USING (idfv)
      WHERE datediff('second', install_timestamp, event_time) BETWEEN 0 AND (86400 * {n_days})
    ),
    events_length as(
      select idfv, datediff('second', min(event_time), max(event_time)) as event_length
      from phoenix_events
      group by idfv
    )
      select *
      FROM (
        SELECT
          a.idfv,
          count(event_id) AS num_events
        FROM user_base a
          LEFT JOIN phoenix_events b
          USING (idfv)
        GROUP BY a.idfv
      ) X
        LEFT JOIN events_length c
        USING (idfv)
    """.format(install_start_date=install_start_date, install_end_date=install_end_date, n_days=n_days)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['idfv'])
    return df


def get_previous_ltd_cef(install_start_date, install_end_date, n_days=7, connection=None):
    """
    """

    query = """
    -- previous ltd cef
    with user_base as (
      select idfv
      from gsnmobile.kochava_device_summary_phoenix
      where install_timestamp::date between '{install_start_date}' and '{install_end_date}'
    ),
    phoenix_events as (
      SELECT a.user_id, a.idfv, a.event_time, install_timestamp
      FROM phoenix.events_client a
      LEFT JOIN gsnmobile.kochava_device_summary_phoenix b
      USING (idfv)
      WHERE datediff('second', install_timestamp, event_time) BETWEEN 0 AND (86400 * {n_days})
    ),
    last_active_query AS (
      SELECT
      user_id, idfv,
      min(install_timestamp) as install_timestamp, -- could be included in group by instead
      min(first_event_time) as first_event_time,
      min(last_event_time) as last_event_time
      FROM (
        SELECT user_id, idfv,
          install_timestamp,
          FIRST_VALUE(event_time) OVER ( PARTITION BY user_id, idfv ORDER BY event_time ASC) AS first_event_time,
          FIRST_VALUE(event_time) OVER ( PARTITION BY user_id, idfv ORDER BY event_time DESC) AS last_event_time
        FROM phoenix_events a
        WHERE datediff('second', install_timestamp, event_time) BETWEEN 0 AND (86400 * {n_days})
      ) x
      group by user_id, idfv
    ),
    joined_idfvs as (
      select a.idfv, b.user_id, b.install_timestamp, b.first_event_time, b.last_event_time
      from user_base a
      left join last_active_query b
      on a.idfv = b.idfv
    ),
    joined_idfvs_ww_lifetime_cef as(
      select a.idfv, a.user_id, install_timestamp, first_event_time, last_event_time, sum(cef) as previous_ltd_cef
      from joined_idfvs a
      left join (
                  select x.user_id,
                  trans_date,
                  amount*-1 as cef
                  from ww.internal_transactions x
                  inner join (select distinct user_id
                  from phoenix.events_client) y
                  on x.user_id = y.user_id::INT
                  where y.user_id is not null
                  and trans_type = 'SIGNUP'
                ) b
      on a.user_id = b.user_id
      and b.trans_date < a.first_event_time
      where b.user_id is not null
      group by a.idfv, a.user_id, install_timestamp, first_event_time, last_event_time
    ),
    agged_idfvs as (
      select idfv, count(user_id) as count_last_active
      from joined_idfvs
      group by idfv
    )

    select X.*, Y.last_active_user_id_previous_ltd_cef
    from(
        select a.idfv, 
        coalesce(sum(previous_ltd_cef), 0) as previous_ltd_cef, 
        count(user_id) as count_user_ids
        from user_base a
        left join joined_idfvs_ww_lifetime_cef b
        using(idfv)
        group by a.idfv, install_timestamp
    ) X
    left join (
        select idfv,
        min(last_active_user_id) as last_active_user_id,
        min(last_active_user_id_previous_ltd_cef) as last_active_user_id_previous_ltd_cef
        from(
            select a.idfv,
            FIRST_VALUE(b.user_id) OVER(PARTITION BY a.idfv ORDER BY last_event_time DESC) as last_active_user_id,
            FIRST_VALUE(b.previous_ltd_cef) OVER(PARTITION BY a.idfv ORDER BY last_event_time DESC) as last_active_user_id_previous_ltd_cef
            from user_base a
            left join joined_idfvs_ww_lifetime_cef b
            using(idfv)
        )Z
        group by idfv
    ) Y
    using(idfv)

    """.format(install_start_date=install_start_date, install_end_date=install_end_date, n_days=n_days)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['idfv'])
    return df


def get_install_time_info(install_start_date, install_end_date, n_days=7, connection=None):
    """
    """

    query = """
    -- Install Time Features
    with user_base as (
      select idfv, install_timestamp
      from gsnmobile.kochava_device_summary_phoenix
      where install_timestamp::date between '{install_start_date}' and '{install_end_date}'
    ),
    time_of_install_subquery as (
      select idfv, MOD((DATE_PART('SECOND', install_timestamp) + DATE_PART('MINUTE', install_timestamp) * (60) + DATE_PART('HOUR', install_timestamp) * (60*60)), 60*60*24) as seconds_into_day
      from user_base
    )
    select i.idfv,
    DAYOFWEEK(install_timestamp) as dow_sun1_sat7,
    COS(2*PI()*seconds_into_day / (60*60*24)) as cos_installed_at,
    SIN(2*PI()*seconds_into_day / (60*60*24)) as sin_installed_at
    FROM user_base i
    LEFT JOIN time_of_install_subquery as e
    USING(idfv)
    """.format(install_start_date=install_start_date, install_end_date=install_end_date, n_days=n_days)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['idfv'])
    return df


def get_attribution_info(install_start_date, install_end_date, n_days=7, connection=None):
    """
    """

    query = """
    /* android started on 4/2/2018.  Before that, only platform is ios */
    with user_base as (
      select *
      from gsnmobile.kochava_device_summary_phoenix
      where install_timestamp::date between '{install_start_date}' and '{install_end_date}'
    )
    select user_base.idfv, coalesce(datediff('hour', install_timestamp, first_ww_reg) / 24, {n_days}) as days_since_reg,
    platform,
    network_name as is_organic
    from user_base
    """.format(install_start_date=install_start_date, install_end_date=install_end_date, n_days=n_days)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['idfv'])

    # convert categoricals to integers
    categoricals = {
        'platform': ['ios', 'android'],
        'is_organic': ['unattributed'],
    }
    for column_name, categories in categoricals.items():
        category = CategoricalDtype(categories=categories, ordered=True)
        df[column_name] = df[column_name].astype(category)
        df[column_name] = df[column_name].cat.codes
    return df


def get_target_df(install_start_date, install_end_date, n_days=90, connection=None):
    """
    Create a dataframe with the target only for installs between `install_start_date` and
    `install_end_date` inclusive.
    """
    df = get_iap_revenue(install_start_date, install_end_date, n_days=n_days)
    # Fill NAs with 0
    df = df.fillna(0)
    return df


def get_previous_days_revenue(install_start_date, install_end_date, n_days=7, connection=None):
    #print('getting previous days revenue...')
    previous_window_lengths = [x for x in [1, 2, 3, 4, 5, 6, 7, 14, 30, 45, 60, 90] if x < n_days]
    if len(previous_window_lengths) == 0:
        #print('no previous observation window to {}-days observed window'.format(n_days))
        pass
    else:
        iap_revenue_query = """
            -- Day N CEF
            with user_base as (
              select idfv, install_timestamp from
              gsnmobile.kochava_device_summary_phoenix
              where install_timestamp::date between '{install_start_date}' and '{install_end_date}'
            ),
            cef_events as (
                SELECT
                  user_id,
                  idfv,
                  int_trans_id,
                  tournament_id,
                  event_time,
                  cef
                FROM phoenix.tournament_cef
            ),
            day_n_cef as (
                select user_base.idfv, coalesce(sum(cef),0) as day{n_days}_iap_revenue
                from user_base
                left join cef_events
                on user_base.idfv = cef_events.idfv
                and datediff('second', install_timestamp, event_time) BETWEEN 0 AND (86400 * {n_days})
                group by user_base.idfv
            )
            select *
            from day_n_cef
            """

        for i, wndw_len in enumerate(previous_window_lengths):
            # print('days{}...'.format(wndw_len))
            if i == 0:
                df = get_dataframe_from_query(
                    iap_revenue_query.format(install_start_date=install_start_date, install_end_date=install_end_date,
                                             n_days=wndw_len), connection=connection)
            else:
                df = pd.merge(
                    df,
                    get_dataframe_from_query(iap_revenue_query.format(install_start_date=install_start_date,
                                                                      install_end_date=install_end_date,
                                                                      n_days=wndw_len,
                                                                      ), connection=connection),
                    how='left',
                    on='idfv'
                )
        df = df.set_index('idfv')
        return df
