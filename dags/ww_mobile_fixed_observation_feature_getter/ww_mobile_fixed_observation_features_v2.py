"""Feature queries for WorldWinner Mobile LTV.

Full list of feature queries here:
- https://docs.google.com/spreadsheets/d/1GE0qLEoWvVAK_y5t5xhGkDdauy6KCellB00ngdlZ92g/edit#gid=390655813
"""

from collections import namedtuple
from datetime import date, datetime, timedelta
from functools import reduce
from joblib import Parallel, delayed
import logging
import numpy as np
import pandas as pd
from pandas.api.types import CategoricalDtype

from common.date_utils import convert_to_date
from common.db_utils import get_dataframe_from_query


def update_df_index(df, n_days_observation, index_column='idfv'):
    """Include 'n_days_observation' in the index to avoid later accounting mistakes.
    """
    df = (
        df
            .reset_index()
            .assign(n_days_observation=n_days_observation)
            .set_index([index_column, 'install_date', 'n_days_observation'])
    )
    return df


def get_idfv_base(install_start_date, install_end_date, n_days=7, connection=None):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=ffcdbe0d-8f9d-4ae0-93b3-36d2e6aea207
    """
    query = """
    /* Get install cohort for WW Mobile LTV */
    with idfv_base as (
      select idfv, install_day as install_date
      from gsnmobile.kochava_device_summary_phoenix
      where install_timestamp::date between '{install_start_date}' and '{install_end_date}'
    )
    select /*+ label('airflow-ml-ww_mobile_fixed_observation_features_v2') */
    *
    from idfv_base
    """.format(install_start_date=install_start_date, n_days=n_days, install_end_date=install_end_date)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['idfv'])
    return df


def get_install_date(install_start_date, install_end_date, n_days=7, connection=None):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=eec6c7e9-3b99-4fd0-a0b7-f17012842ed2
    """
    query = """
    /* Gets idfv install_timestamp from kochava_device_summary_phoenix */
    with idfv_base_with_install_time as (
      select idfv, install_day as install_date
      from gsnmobile.kochava_device_summary_phoenix
      where install_day::date between '{install_start_date}' and '{install_end_date}'
    )
    select /*+ label('airflow-ml-ww_mobile_fixed_observation_features_v2') */
    *
    from idfv_base_with_install_time
    """.format(install_start_date=install_start_date, n_days=n_days, install_end_date=install_end_date)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['idfv'])
    return df


def get_iap_revenue(install_start_date, install_end_date, n_days=7, connection=None):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=c8d5a936-bd07-4cc6-abef-d7fc9dd78337
    """
    query = """
    /* Day N CEF */
    with idfv_base as (
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
      select a.idfv, coalesce(sum(cef),0) as day{n_days}_iap_revenue
      from idfv_base a
      left join cef_events b
      on a.idfv = b.idfv
      and datediff('second', install_timestamp, event_time) between 0 and 86400 * ({n_days})
      group by a.idfv
    )
    select /*+ label('airflow-ml-ww_mobile_fixed_observation_features_v2') */
    *
    from day_n_cef
    """.format(install_start_date=install_start_date, n_days=n_days, install_end_date=install_end_date)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['idfv'])
    return df


def get_rfm_events(install_start_date, install_end_date, n_days=7, connection=None):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=ec4a4551-9f9c-4621-902f-5b11a17e98f9
    """
    query = """
    /* RFM Events */
    with idfv_base as (
      select idfv
      from gsnmobile.kochava_device_summary_phoenix
      where install_timestamp::date between '{install_start_date}' and '{install_end_date}'
    ),
    phoenix_events as (
      SELECT a.install_timestamp, b.*
      FROM gsnmobile.kochava_device_summary_phoenix a
      LEFT JOIN phoenix.events_client b
      USING (idfv)
      WHERE datediff('second', install_timestamp, event_time) BETWEEN 0 AND (86400 * {n_days})
      --AND event_name in ('userClicked', 'gameSelect', 'tournamentSelect', 'popupOpen')
    ),
    events_agg as(
      select idfv,
      datediff('second', min(event_time), max(event_time)) as event_length,
      count(event_id) AS num_events
      from phoenix_events
      group by idfv
    )
    select /*+ label('airflow-ml-ww_mobile_fixed_observation_features_v2') */
    b.*
    from idfv_base a
    left join events_agg b
    USING (idfv)
    """.format(install_start_date=install_start_date, n_days=n_days, install_end_date=install_end_date)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['idfv'])
    return df


def get_previous_ltd_cef(install_start_date, install_end_date, n_days=7, connection=None):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=19377761-158a-4b2b-9dfc-e2c381c74542
    """
    query = """
    /* previous user_id lifetime-to-date CEF */
    with idfv_base as (
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
      from idfv_base a
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
      and datediff('second', a.first_event_time, b.trans_date) between -{lookback_days}*86400 and 0
      where b.user_id is not null
      group by a.idfv, a.user_id, install_timestamp, first_event_time, last_event_time
    ),
    agged_idfvs as (
      select idfv, count(user_id) as count_last_active
      from joined_idfvs
      group by idfv
    )

    select /*+ label('airflow-ml-ww_mobile_fixed_observation_features_v2') */
    X.*, Y.last_active_user_id_previous_ltd_cef
    from(
      select a.idfv,
      coalesce(sum(previous_ltd_cef), 0) as previous_ltd_cef,
      count(user_id) as count_user_ids
      from idfv_base a
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
        from idfv_base a
        left join joined_idfvs_ww_lifetime_cef b
        using(idfv)
      )Z
      group by idfv
    ) Y
    using(idfv)
    """.format(install_start_date=install_start_date, n_days=n_days, install_end_date=install_end_date,
               lookback_days=90)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['idfv'])
    return df


def get_install_time_cyclical(install_start_date, install_end_date, n_days=7, connection=None):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=18f1199e-0de5-4356-a0d2-5fd0f5361c53
    """
    query = """
     /* Cyclically Encoded Time of Installs */
     with idfv_base as (
       select idfv, install_timestamp
       from gsnmobile.kochava_device_summary_phoenix
       where install_timestamp::date between '{install_start_date}' and '{install_end_date}'
     ),
     time_of_install_subquery as (
       select idfv, MOD((DATE_PART('SECOND', install_timestamp) + DATE_PART('MINUTE', install_timestamp) * (60) + DATE_PART('HOUR', install_timestamp) * (60*60)), 60*60*24) as seconds_into_day
       from idfv_base
     )
     select /*+ label('airflow-ml-ww_mobile_fixed_observation_features_v2') */
     i.idfv,
     DAYOFWEEK(install_timestamp) as dow_sun1_sat7,
     COS(2*PI()*seconds_into_day / (60*60*24)) as cos_installed_at,
     SIN(2*PI()*seconds_into_day / (60*60*24)) as sin_installed_at
     FROM idfv_base i
     LEFT JOIN time_of_install_subquery as e
     USING(idfv)
    """.format(install_start_date=install_start_date, n_days=n_days, install_end_date=install_end_date)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['idfv'])
    return df


def get_network_name(install_start_date, install_end_date, n_days=7, connection=None):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=bc08b45f-c8bf-489e-bafb-348e6b8d2d09
    """
    query = """
    with idfv_base as (
      select idfv, network_name
      from gsnmobile.kochava_device_summary_phoenix
      where install_timestamp::date between '{install_start_date}' and '{install_end_date}'
    )
    select /*+ label('airflow-ml-ww_mobile_fixed_observation_features_v2') */
    idfv, network_name
    from idfv_base
    """.format(install_start_date=install_start_date, n_days=n_days, install_end_date=install_end_date)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['idfv'])
    return df


def get_all_user_info(install_start_date, install_end_date, n_days=7, connection=None):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=18090cb3-47b5-4387-8717-2fcabc5a9711
    """
    query = """
    /* Aggregate user info for user_ids observed on each device (IDFV) */
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
      install_timestamp,
      min(first_event_time) as first_event_time,
      min(last_event_time) as last_event_time
      FROM (
        SELECT user_id, idfv,
        install_timestamp,
        FIRST_VALUE(event_time) OVER ( PARTITION BY user_id, idfv ORDER BY event_time ASC) AS first_event_time,
        FIRST_VALUE(event_time) OVER ( PARTITION BY user_id, idfv ORDER BY event_time DESC) AS last_event_time
        FROM phoenix_events a
        WHERE datediff('second', install_timestamp, event_time) BETWEEN 0 AND (86400 * {n_days})
        AND user_id is not null
      ) x
      group by user_id, idfv, install_timestamp
    ),
    joined_idfvs as (
      select a.idfv, b.user_id, b.install_timestamp, b.first_event_time, b.last_event_time
      from user_base a
      left join last_active_query b
      on a.idfv = b.idfv
    ),
    user_info as (
      select user_id, 
      case when ISUTF8(username) then username else 'non_utf_eight' end as username,
      case when ISUTF8(city) then city else 'non_utf_eight' end as city,
      case when ISUTF8(state) then state else 'non_utf_eight' end as state,
      case when ISUTF8(zip) then zip else 'non_utf_eight' end as zip,
      case when ISUTF8(country) then country else 'non_utf_eight' end as country,
      case when ISUTF8(email) then email else 'non_utf_eight@non_utf_eight.com' end as email,  
      case when ISUTF8(advertiser_id) then advertiser_id else 'non_utf_eight' end as advertiser_id,
      case when ISUTF8(partnertag) then partnertag else 'non_utf_eight' end as partnertag,
      cid, sendemail, play_money, show_email_check, user_banned, password, createdate, lastname, firstname, middlename,
      namesuffix, address1, address2, address_verified, real_money_date, first_game_played_date, unsubscribed_flag, 
      universe, user_banned_date, account_state, cobrand_id, deposit_credits,
      balance, accesslevel, gender, age, referrer_id,
      SPLIT_PART(username, '.', 1) as username_prefix,
      SPLIT_PART(username, '.', 2) as username_suffix,
      SPLIT_PART(email, '@', 1) as email_username,
      SPLIT_PART(email, '@', 2) as email_domain
      from ww.dim_users
      where createdate between '{install_start_date}' and (date('{install_end_date}') + interval '{n_days} days')
      and user_id is not null
    ),
    idfv_user_agg as (
      select idfv, install_timestamp, first_event_time, last_event_time,
      b.*,
      /* Feature Engineering */
      -- Geolocation --

      -- User Name -- 
      --username_prefix,
      --username_suffix,
      LENGTH(username_prefix) as num_char_username_prefix,
      LENGTH(username_suffix) as num_char_username_suffix,
      REGEXP_REPLACE(username, '\D', '') as numbers_in_username,
      REGEXP_COUNT(username, '[0-9]') as count_numbers_in_username,
      -- Email --
      --email_username,
      --email_domain,
      (email_domain like '%.edu%')::INT as is_student,
      REGEXP_COUNT(email_username, '\.') as num_dots_in_email_username,
      LENGTH(email_username) as num_char_email_username,
      REGEXP_REPLACE(email_username, '\D', '') as numbers_in_email_username,
      REGEXP_COUNT(email_username, '[0-9]') as count_numbers_in_email_username,
      REGEXP_SUBSTR(email_domain, '^.+\.') as email_domain_company,
      REGEXP_SUBSTR(email_domain, '\..+$') as email_domain_ending,
      case when sendemail = 't' then 1 when sendemail ='f' then 0 else 0 end as allows_emails,
      case when show_email_check = 't' then 1 when show_email_check ='f' then 0 else 0 end as shows_emailaddress_in_profile,
      -- Banned --
      coalesce((user_banned > 0 and datediff('second', install_timestamp, user_banned_date) between 0 and (86400 * {n_days}))::INT, 0) as is_banned, 
      LEAST(coalesce(datediff('second', install_timestamp, user_banned_date)/(60*60), 24*{n_days}), 24*{n_days}) as time_to_ban_hr,
      -- Other --
      (username_prefix = email_username)::INT as username_equals_email_username
      /* End */
      from joined_idfvs a
      left join user_info b
      on a.user_id::int = b.user_id::int
      and datediff('second', install_timestamp, createdate) BETWEEN 0 AND (86400 * {n_days})
    ),
    idfv_agg as (
      select idfv, --install_timestamp, first_event_time, last_event_time,
      -- ww.dim_users portion --
      max(username) as username,
      max(createdate) as createdate,
      max(city) as city,
      max(state) as state,
      max(zip) as zip,
      max(country) as country,
      max(balance) as balance,
      max(email) as email,
      max(accesslevel) as accesslevel,
      max(gender) as gender,
      max(age) as age,
      max(referrer_id) as referrer_id,
      max(advertiser_id) as advertiser_id,
      max(partnertag) as partnertag,
      max(cid) as cid,
      max(sendemail) as sendemail,
      max(play_money) as play_money,
      max(show_email_check) as show_email_check,
      max(user_banned) as user_banned,
      max(address_verified) as address_verified,
      max(real_money_date) as real_money_date,
      max(first_game_played_date) as first_game_played_date,
      max(unsubscribed_flag) as unsubscribed_flag,
      max(universe) as universe,
      max(user_banned_date) as user_banned_date,
      max(account_state) as account_state,
      max(cobrand_id) as cobrand_id,
      max(deposit_credits) as deposit_credits,
      -- feature engineered portion --
      -- Geolocation --

      -- User Name -- 
      max(username_prefix) as username_prefix,
      max(num_char_username_prefix) as num_char_username_prefix,
      max(username_suffix) as username_suffix,
      max(num_char_username_suffix) as num_char_username_suffix,
      max(case when numbers_in_username = '' then '-999' else numbers_in_username end) as numbers_in_username,
      max(count_numbers_in_username) as count_numbers_in_username,
      -- Email --
      max(email_username) email_username,
      max(email_domain) email_domain,
      max(is_student) is_student,
      max(num_dots_in_email_username) num_dots_in_email_username,
      max(num_char_email_username) num_char_email_username,
      max(case when numbers_in_email_username = '' then '-999' else numbers_in_email_username end) numbers_in_email_username,
      max(count_numbers_in_email_username) count_numbers_in_email_username,
      max(email_domain_company) email_domain_company,
      max(email_domain_ending) email_domain_ending,
      max(allows_emails) allows_emails,
      max(shows_emailaddress_in_profile) shows_emailaddress_in_profile,
      -- Banned --
      max(is_banned) as is_banned, 
      max(time_to_ban_hr) as time_to_ban_hr,
      -- Other --
      max(username_equals_email_username) as username_equals_email_username
      /* End */
      from idfv_user_agg a
      group by idfv--, install_timestamp, first_event_time, last_event_time
    )
    select /*+ label('airflow-ml-ww_mobile_fixed_observation_features_v2') */
    *
    from idfv_agg
    """.format(install_start_date=install_start_date, n_days=n_days, install_end_date=install_end_date)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['idfv'])
    return df


def get_rfm_deposits_withdrawals(install_start_date, install_end_date, n_days=7, connection=None):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=875f2cf1-16b9-4856-b0fc-05b80244d886
    """
    query = """
    /* RFM Observed Deposits and Withdrawals */
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
      AND user_id is not null
    ),
    last_active_query AS (
      SELECT
      user_id, idfv,
      install_timestamp,
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
      group by user_id, idfv, install_timestamp
    ),
    joined_idfvs as (
      select a.idfv, b.user_id, b.install_timestamp, b.first_event_time, b.last_event_time
      from user_base a
      left join last_active_query b
      on a.idfv = b.idfv
    ),
    dw_query as (
      select user_id, trans_type, trans_date, amount
      from ww.internal_transactions
      where (lower(trans_type) like '%deposit%' or lower(trans_type) like '%withdraw%')
      and trans_date between (date('{install_start_date}') - interval '{lookback_days} days') and (date('{install_end_date}') + interval '{n_days} days')
      and user_id is not null
    ),
    idfv_user_agg as (
      select a.user_id, a.idfv, a.install_timestamp,
      SUM(case when lower(trans_type) like '%deposit%' then amount else null end) as deposit_amount,
      SUM(case when lower(trans_type) like '%deposit%' then 1 else NULL end) as deposit_frequency,
      MIN(case when lower(trans_type) like '%deposit%' then datediff('second', install_timestamp, (trans_date))/(60*60) else null end) as first_deposit_hrs,
      MAX(case when lower(trans_type) like '%deposit%' then datediff('second', install_timestamp, (trans_date))/(60*60) else null end) as last_deposit_hrs,
      SUM(case when lower(trans_type) like '%withdraw%' then amount else null end) as withdrawal_amount,
      SUM(case when lower(trans_type) like '%withdraw%' then 1 else NULL end) as withdrawal_frequency,
      MIN(case when lower(trans_type) like '%withdraw%' then datediff('second', install_timestamp, (trans_date))/(60*60) else null end) as first_withdrawal_hrs,
      MAX(case when lower(trans_type) like '%withdraw%' then datediff('second', install_timestamp, (trans_date))/(60*60) else null end) as last_withdrawal_hrs,
      SUM(case when lower(trans_type) like '%deposit%' then amount else 0 end) +
	  SUM(case when lower(trans_type) like '%withdraw%' then amount else 0 end) as net_withdrawal_amount
      from joined_idfvs a
      left join dw_query b
      on a.user_id::int = b.user_id::int
      and datediff('second', install_timestamp, trans_date) between (86400 * -{lookback_days}) and (86400 * {n_days})
      group by a.user_id, a.idfv, a.install_timestamp
    ),
    idfv_agg as (
      select idfv,
      max(deposit_amount) as deposit_amount,
      max(deposit_frequency) as deposit_frequency,
      max(first_deposit_hrs) as first_deposit_hrs,
      max(last_deposit_hrs) as last_deposit_hrs,
      max(withdrawal_amount) as withdrawal_amount,
      max(withdrawal_frequency) as withdrawal_frequency,
      max(first_withdrawal_hrs) as first_withdrawal_hrs,
      max(last_withdrawal_hrs) as last_withdrawal_hrs,
      max(net_withdrawal_amount) as net_withdrawal_amount
      from idfv_user_agg 
      group by idfv
    )
    select /*+ label('airflow-ml-ww_mobile_fixed_observation_features_v2') */
    *
    from idfv_agg
    """.format(install_start_date=install_start_date, n_days=n_days, install_end_date=install_end_date,
               lookback_days=540)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['idfv'])
    return df


def get_past_churn_metrics(install_start_date, install_end_date, n_days=7, connection=None):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=16699094-9256-423a-b96b-1ac1fe660a4a
    """
    query = """
    /* Past churn metrics by observed user_ids on this IDFV */
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
      install_timestamp,
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
      group by user_id, idfv, install_timestamp
    ),
    joined_idfvs as (
      select a.idfv, b.user_id, b.install_timestamp, b.first_event_time, b.last_event_time
      from user_base a
      left join last_active_query b
      on a.idfv = b.idfv
    ),
    deltas as(
      select * from(
        select user_id, start_date_str, start_date,
        start_date - LAG(start_date, 1, 0) OVER (PARTITION BY user_id
                                                 ORDER BY start_date) AS difference,
        RANK() OVER (PARTITION BY user_id
                                                 ORDER BY start_date) AS rn
        from (
          select distinct a.user_id, datediff('day', a.install_timestamp, b.start_time) as start_date, start_time::date as start_date_str
          from joined_idfvs a
          join (select * from ww.played_games
          WHERE start_time BETWEEN (date('{install_start_date}') - interval '{lookback_days} days') AND (date('{install_end_date}') + interval '{n_days} days')) b
          on a.user_id::int = b.user_id::int
          and datediff('second', a.install_timestamp, b.start_time) between -{lookback_days}*86400 and ({n_days}*86400)
        ) as x
      ) as y
      where user_id is not null
    ),
    idfv_user_agg as (
      select a.idfv, a.user_id, a.install_timestamp, a.first_event_time, a.last_event_time,
      COUNT(case when abs(difference) >= 30 then abs(difference) else NULL end) as num_churns_over30,
      MAX(abs(difference)) as max_churn_length,
      AVG(case when abs(difference) >= 30 then abs(difference) else NULL end) as avg_churn_length_over30,
      AVG(case when abs(difference) < 30 then abs(difference) else NULL end) as avg_churn_length_under30,
      MAX(case when rn = 1 then start_date else NULL end) as last_seen_days_ago
      from joined_idfvs a
      left join deltas b
      on a.user_id::INT = b.user_id::INT
      and datediff('second', install_timestamp, start_date_str) between -{lookback_days}*86400  and ({n_days}*86400)
      group by a.idfv, a.user_id, a.install_timestamp, a.first_event_time, a.last_event_time
    ),
    idfv_agg as (
      select idfv,
      max(num_churns_over30) as num_churns_over30,
      min(max_churn_length) as max_churn_length,
      max(avg_churn_length_over30) as avg_churn_length_over30,
      min(avg_churn_length_under30) as avg_churn_length_under30,
      max(last_seen_days_ago) as last_seen_days_ago
      from idfv_user_agg
      where user_id is not null
      group by idfv
    )
    select /*+ label('airflow-ml-ww_mobile_fixed_observation_features_v2') */
    a.idfv,
    b.num_churns_over30,
    b.max_churn_length,
    b.avg_churn_length_over30,
    b.avg_churn_length_under30,
    b.last_seen_days_ago
    from user_base a
    left join idfv_agg b
    on a.idfv = b.idfv
    """.format(install_start_date=install_start_date, n_days=n_days, install_end_date=install_end_date,
               lookback_days=30)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['idfv'])
    return df


def get_rfm_days_played(install_start_date, install_end_date, n_days=7, connection=None):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=87dbeb95-a1d0-46c6-8d36-9a8af4793102
    """
    query = """
    /* RFM of days played (games played) in the observation window */
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
      WHERE datediff('second', install_timestamp, event_time) BETWEEN 0 AND ({n_days}*86400)
    ),
    last_active_query AS (
      SELECT
      user_id, idfv,
      install_timestamp,
      min(first_event_time) as first_event_time,
      min(last_event_time) as last_event_time
      FROM (
        SELECT user_id, idfv,
        install_timestamp,
        FIRST_VALUE(event_time) OVER ( PARTITION BY user_id, idfv ORDER BY event_time ASC) AS first_event_time,
        FIRST_VALUE(event_time) OVER ( PARTITION BY user_id, idfv ORDER BY event_time DESC) AS last_event_time
        FROM phoenix_events a
        WHERE datediff('second', install_timestamp, event_time) BETWEEN 0 AND ({n_days}*86400)
      ) x
      group by user_id, idfv, install_timestamp
    ),
    joined_idfvs as (
      select a.idfv, b.user_id, b.install_timestamp, b.first_event_time, b.last_event_time
      from user_base a
      left join last_active_query b
      on a.idfv = b.idfv
    ),
    days_played as (
      select distinct pg.user_id, pg.start_time::date as trans_date
      from (
        select user_id, tournament_id, gametype_id, start_time
        from ww.played_games as pg
        where start_time::date between '{install_start_date}' and (date('{install_end_date}') + interval '{n_days} days')
      ) as pg
      /* left join tournament info */
      left join(
        select tournament_id, ttemplate_id, fee
        from ww.tournaments
        ) as t
      using(tournament_id)
    ),
    idfv_user_agg as (
      select a.user_id, a.idfv,
      count(b.trans_date) as days_played,
      coalesce(datediff('day', install_timestamp::date, min(trans_date)), -1) + 1 as first_played,
      coalesce(datediff('day', install_timestamp::date, max(trans_date)), -1) + 1 as last_played
      from joined_idfvs a
      left join days_played b
      on a.user_id = b.user_id
      and datediff('second', install_timestamp::date, trans_date) between 0 and ({n_days}*86400)
      group by a.user_id, a.idfv, install_timestamp::date
      order by days_played  DESC
    ),
    idfv_agg as (
      select idfv,
      max(days_played) as days_played,
      min(first_played) as first_played,
      max(last_played) as last_played
      from idfv_user_agg
      where user_id is not null
      group by idfv
    )
    select /*+ label('airflow-ml-ww_mobile_fixed_observation_features_v2') */
    a.idfv,
    b.days_played,
    b.first_played,
    b.last_played
    from user_base a
    left join idfv_agg b
    on a.idfv = b.idfv
    """.format(install_start_date=install_start_date, n_days=n_days, install_end_date=install_end_date)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['idfv'])
    return df


def get_web_sessions(install_start_date, install_end_date, n_days=7, connection=None):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=a42ba363-85d8-469e-a2f0-a08f8aece0e1
    """
    query = """
    /* Web Session counts and lengths */
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
      WHERE datediff('second', install_timestamp, event_time) BETWEEN 0 AND ({n_days}*86400)
    ),
    last_active_query AS (
      SELECT
      user_id, idfv,
      install_timestamp,
      min(first_event_time) as first_event_time,
      min(last_event_time) as last_event_time
      FROM (
        SELECT user_id, idfv,
        install_timestamp,
        FIRST_VALUE(event_time) OVER ( PARTITION BY user_id, idfv ORDER BY event_time ASC) AS first_event_time,
        FIRST_VALUE(event_time) OVER ( PARTITION BY user_id, idfv ORDER BY event_time DESC) AS last_event_time
        FROM phoenix_events a
        WHERE datediff('second', install_timestamp, event_time) BETWEEN 0 AND ({n_days}*86400)
      ) x
      group by user_id, idfv, install_timestamp
    ),
    joined_idfvs as (
      select a.idfv, b.user_id, b.install_timestamp, b.first_event_time, b.last_event_time
      from user_base a
      left join last_active_query b
      on a.idfv = b.idfv
    ),
    web_sessions as(
      select user_id, start_time, session, duration_seconds
      from ww.events_web_sessions
      where start_time between (date('{install_start_date}')-interval'{lookback_days} days') and (date('{install_end_date}') + interval '{n_days} days')
    ),
    idfv_user_agg as (
      select a.idfv, a.user_id,
      sum(session) as count_sessions,
      sum(duration_seconds) as length_sessions,
      avg(duration_seconds) as average_duration_seconds,
      min(duration_seconds) as minimum_duration_seconds,
      max(duration_seconds) as maximum_duration_seconds
      from joined_idfvs a
      left join web_sessions b
      on a.user_id::int = b.user_id::int
      and datediff('second', install_timestamp, start_time) between -{lookback_days}*86400 and ({n_days}*86400)
      group by a.idfv, a.user_id
    ),
    idfv_agg as (
      select idfv,
      sum(count_sessions) as web_count_sessions,
      sum(length_sessions) as web_length_sessions,
      avg(average_duration_seconds) as web_average_duration_seconds,
      min(minimum_duration_seconds) as web_minimum_duration_seconds,
      max(maximum_duration_seconds) as web_maximum_duration_seconds
      from idfv_user_agg
      where user_id is not null
      group by idfv
    )
    select /*+ label('airflow-ml-ww_mobile_fixed_observation_features_v2') */
    a.idfv,
    b.web_count_sessions,
    b.web_length_sessions,
    b.web_average_duration_seconds,
    b.web_minimum_duration_seconds,
    b.web_maximum_duration_seconds
    from user_base a
    left join idfv_agg b
    on a.idfv = b.idfv
    """.format(install_start_date=install_start_date, n_days=n_days, install_end_date=install_end_date,
               lookback_days=80)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['idfv'])
    return df


def get_games_played(install_start_date, install_end_date, n_days=7, connection=None):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=90cfc51b-7709-4f24-be4d-f0c4e9c13faa
    """
    query = """
    /* Gets Counts of games played by category and gametype_id */
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
      WHERE datediff('second', install_timestamp, event_time) BETWEEN 0 AND ({n_days}*86400)
    ),
    last_active_query AS (
      SELECT
      user_id, idfv,
      install_timestamp,
      min(first_event_time) as first_event_time,
      min(last_event_time) as last_event_time
      FROM (
        SELECT user_id, idfv,
        install_timestamp,
        FIRST_VALUE(event_time) OVER ( PARTITION BY user_id, idfv ORDER BY event_time ASC) AS first_event_time,
        FIRST_VALUE(event_time) OVER ( PARTITION BY user_id, idfv ORDER BY event_time DESC) AS last_event_time
        FROM phoenix_events a
        WHERE datediff('second', install_timestamp, event_time) BETWEEN 0 AND ({n_days}*86400)
      ) x
      group by user_id, idfv, install_timestamp
    ),
    joined_idfvs as (
      select a.idfv, b.user_id, b.install_timestamp, b.first_event_time, b.last_event_time
      from user_base a
      left join last_active_query b
      on a.idfv = b.idfv
    ),
    games_played as ( -- 931 Million Rows
      select user_id, gametype_id, start_date
      from ww.played_games
      where start_date between (date('{install_start_date}') - interval '{lookback_days} days') and (date('{install_end_date}') + interval '{n_days} days')
    ),
    game_types as (
      select gametype_id, category
      from ww.dim_gametypes
    ),
    idfv_agg as (
      select a.idfv,
      SUM(case when c.category = 'Arcade' then 1 else 0 end) as arcade_games,
      SUM(case when c.category = 'Word' then 1 else 0 end) as word_games,
      SUM(case when c.category = 'Strategy' then 1 else 0 end) as strategy_games,
      SUM(case when c.category = 'Card' then 1 else 0 end) as card_games,
      SUM(case when c.category = 'Game Show' then 1 else 0 end) as gameshow_games,
      SUM(case when b.gametype_id not in (34, 109, 107, 20, 118, 33, 125, 114, 128, 126, 120, 35, 201, 24, 122,
                                          154, 127, 124, 23, 86, 1, 38, 83, 29, 121, 108, 115, 117) then 1 else 0 end) as other_games,
      -- Arcade --
      SUM(case when b.gametype_id = 34 then 1 else 0 end) as bejeweled_games,
      SUM(case when b.gametype_id = 109 then 1 else 0 end) as vegasnights2_games,
      SUM(case when b.gametype_id = 107 then 1 else 0 end) as bejeweledblitz_games,
      SUM(case when b.gametype_id = 20 then 1 else 0 end) as swapit_games,
      SUM(case when b.gametype_id = 118 then 1 else 0 end) as bigmoneyhtml5_games,
      SUM(case when b.gametype_id = 33 then 1 else 0 end) as luxor_games,
      SUM(case when b.gametype_id = 125 then 1 else 0 end) as swapithtml5_games,
      SUM(case when b.gametype_id = 114 then 1 else 0 end) as swipehype_games,
      SUM(case when b.gametype_id = 128 then 1 else 0 end) as twodots_games,
      SUM(case when b.gametype_id = 126 then 1 else 0 end) as pacman_games,
      SUM(case when b.gametype_id = 120 then 1 else 0 end) as tetris_games,
      -- Word --
      SUM(case when b.gametype_id = 35 then 1 else 0 end) as scrabblecubes_games,
      SUM(case when b.gametype_id = 201 then 1 else 0 end) as boggle_games,
      SUM(case when b.gametype_id = 24 then 1 else 0 end) as wordmojo_games,
      SUM(case when b.gametype_id = 122 then 1 else 0 end) as trivialpursuit_games,
      -- Strategy --
      SUM(case when b.gametype_id = 154 then 1 else 0 end) as dynomite_games,
      SUM(case when b.gametype_id = 127 then 1 else 0 end) as popandplunderhtml5_games,
      SUM(case when b.gametype_id = 124 then 1 else 0 end) as angrybirds_games,
      SUM(case when b.gametype_id = 23 then 1 else 0 end) as cubis_games,
      SUM(case when b.gametype_id = 86 then 1 else 0 end) as mjd_games,
      -- Card --
      SUM(case when b.gametype_id = 1 then 1 else 0 end) as solitairerush_games,
      SUM(case when b.gametype_id = 38 then 1 else 0 end) as spidersolitaire_games,
      SUM(case when b.gametype_id = 83 then 1 else 0 end) as catch21_games,
      SUM(case when b.gametype_id = 29 then 1 else 0 end) as wwspades_games,
      SUM(case when b.gametype_id = 121 then 1 else 0 end) as tripeakssolitaire_games,
      SUM(case when b.gametype_id = 108 then 1 else 0 end) as pyramidsolitaire_games,
      -- Game Show --
      SUM(case when b.gametype_id = 115 then 1 else 0 end) as wheeloffortunehtml5_games,
      SUM(case when b.gametype_id = 117 then 1 else 0 end) as plinkohtml5_games
      from joined_idfvs a
      left join games_played b -- 931 Million Rows Here
      on a.user_id = b.user_id
      and datediff('second', install_timestamp, start_date) between -{lookback_days}*86400 and ({n_days}*86400)
      left join game_types c -- Supplementary Game Info, no problem
      on b.gametype_id = c.gametype_id
      group by a.idfv
    )
    select /*+ label('airflow-ml-ww_mobile_fixed_observation_features_v2') */
    *
    from idfv_agg
    """.format(install_start_date=install_start_date, n_days=n_days, install_end_date=install_end_date,
               lookback_days=90)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['idfv'])
    return df


def get_first_last_balance(install_start_date, install_end_date, n_days=7, connection=None):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=aedb200a-4294-4155-862f-5086ead14283
    """
    query = """
    /* Gets first and last playable balances by IDFV */
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
      WHERE datediff('second', install_timestamp, event_time) BETWEEN 0 AND ({n_days}*86400)
    ),
    last_active_query AS (
      SELECT
      user_id, idfv,
      install_timestamp,
      min(first_event_time) as first_event_time,
      min(last_event_time) as last_event_time
      FROM (
        SELECT user_id, idfv,
        install_timestamp,
        FIRST_VALUE(event_time) OVER ( PARTITION BY user_id, idfv ORDER BY event_time ASC) AS first_event_time,
        FIRST_VALUE(event_time) OVER ( PARTITION BY user_id, idfv ORDER BY event_time DESC) AS last_event_time
        FROM phoenix_events a
        WHERE datediff('second', install_timestamp, event_time) BETWEEN 0 AND ({n_days}*86400)
      ) x
      group by user_id, idfv, install_timestamp
    ),
    joined_idfvs as (
      select a.idfv, b.user_id, b.install_timestamp, b.first_event_time, b.last_event_time
      from user_base a
      left join last_active_query b
      on a.idfv = b.idfv
      and user_id is not null
    ),
    ranked_trans_first as(
      SELECT it.user_id, a.idfv, trans_date,
      (balance + play_money) as first_playable_balance,
      (balance) as first_real_amount_balance,
      (play_money) as first_play_amount_balance,
      ROW_NUMBER() OVER (
        PARTITION BY it.user_id, a.idfv
        ORDER BY trans_date ASC
      ) AS rn
      from joined_idfvs a
      left join ww.internal_transactions as it
      on a.user_id = it.user_id
      and datediff('second', install_timestamp, trans_date) between 0 and {n_days}*86400
      where trans_date between '{install_start_date}' and (date('{install_end_date}') + interval '{n_days} days')
    ),
    ranked_trans_last as(
      SELECT it.user_id, a.idfv, trans_date,
      (balance + play_money) as last_playable_balance,
      (balance) as last_real_amount_balance,
      (play_money) as last_play_amount_balance,
      ROW_NUMBER() OVER (
        PARTITION BY it.user_id, a.idfv
        ORDER BY trans_date DESC
      ) AS rn
      from joined_idfvs a
      left join ww.internal_transactions as it
      on a.user_id = it.user_id
      and datediff('second', install_timestamp, trans_date) between 0 and {n_days}*86400
      where trans_date between '{install_start_date}' and (date('{install_end_date}') + interval '{n_days} days')
    ),
    idfv_user_agg as (
      select a.idfv, a.user_id,
      first_playable_balance, first_real_amount_balance, first_play_amount_balance,
      last_playable_balance, last_real_amount_balance, last_play_amount_balance
      from joined_idfvs a
      left join ranked_trans_first b
      on a.user_id::int = b.user_id::int
      and b.rn = 1
      and datediff('second', install_timestamp, b.trans_date) BETWEEN 0 AND ({n_days}*86400)
      left join ranked_trans_last c
      on a.user_id::int = c.user_id::int
      and c.rn = 1
      and datediff('second', install_timestamp, c.trans_date) BETWEEN 0 AND ({n_days}*86400)
    ),
    idfv_agg as (
      select idfv,
      max(first_playable_balance) as first_playable_balance,
      max(first_real_amount_balance) as first_real_amount_balance,
      max(first_play_amount_balance) as first_play_amount_balance,
      max(last_playable_balance) as last_playable_balance,
      max(last_real_amount_balance) as last_real_amount_balance,
      max(last_play_amount_balance) as last_play_amount_balance
      from idfv_user_agg
      group by idfv
    )
    select /*+ label('airflow-ml-ww_mobile_fixed_observation_features_v2') */
    idfv,
    first_playable_balance,
    first_real_amount_balance,
    first_play_amount_balance,
    last_playable_balance,
    last_real_amount_balance,
    last_play_amount_balance
    from idfv_agg
    """.format(install_start_date=install_start_date, n_days=n_days, install_end_date=install_end_date)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['idfv'])
    return df


def get_net_winnings(install_start_date, install_end_date, n_days=7, connection=None):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=1527d78c-11d3-4a1c-9b5a-27d03f8cd948
    """
    query = """
    /* Get total net winnings */
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
      WHERE datediff('second', install_timestamp, event_time) BETWEEN 0 AND ({n_days}*86400)
    ),
    last_active_query AS (
      SELECT
      user_id, idfv,
      install_timestamp,
      min(first_event_time) as first_event_time,
      min(last_event_time) as last_event_time
      FROM (
        SELECT user_id, idfv,
        install_timestamp,
        FIRST_VALUE(event_time) OVER ( PARTITION BY user_id, idfv ORDER BY event_time ASC) AS first_event_time,
        FIRST_VALUE(event_time) OVER ( PARTITION BY user_id, idfv ORDER BY event_time DESC) AS last_event_time
        FROM phoenix_events a
        WHERE datediff('second', install_timestamp, event_time) BETWEEN 0 AND ({n_days}*86400)
      ) x
      group by user_id, idfv, install_timestamp
    ),
    joined_idfvs as (
      select a.idfv, b.user_id, b.install_timestamp, b.first_event_time, b.last_event_time
      from user_base a
      left join last_active_query b
      on a.idfv = b.idfv
    ),
    game_transactions as (
      select user_id, trans_date, amount, trans_type
      from ww.internal_transactions
       where trans_date between (date('{install_start_date}') - interval '{lookback_days} days') and (date('{install_end_date}') + interval '{n_days} days')
      and trans_type in ('WINNINGS', 'SIGNUP')
    ),
    idfv_user_agg as (
      select a.idfv, a.user_id, a.install_timestamp,
      coalesce(SUM(case when trans_type = 'WINNINGS' then amount else NULL end), 0) as total_winnings,
      coalesce(SUM(case when trans_type = 'SIGNUP' then amount else NULL end), 0) as total_signups,
      coalesce(SUM(case when trans_type = 'WINNINGS' then amount else NULL end), 0) + coalesce(SUM(case when trans_type = 'SIGNUP' then amount else NULL end), 0) as total_net_winnings
      from joined_idfvs a
      left join game_transactions b
      on a.user_id::int = b.user_id::int
      and datediff('second', install_timestamp, trans_date) BETWEEN -{lookback_days}*86400 AND ({n_days}*86400)
      group by a.idfv, a.user_id, a.install_timestamp
    ),
    idfv_agg as (
      select idfv,
      sum(total_winnings) as total_winnings,
      sum(total_signups) as total_signups,
      sum(total_net_winnings) as total_net_winnings
      from idfv_user_agg
      group by idfv
    )
    select /*+ label('airflow-ml-ww_mobile_fixed_observation_features_v2') */
    idfv,
    total_winnings,
    total_signups,
    total_net_winnings
    from idfv_agg
    """.format(install_start_date=install_start_date, n_days=n_days, install_end_date=install_end_date,
               lookback_days=365)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['idfv'])
    return df


def get_freeplays(install_start_date, install_end_date, n_days=7, connection=None):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=3a45898e-61bb-4f5e-b174-26dbfe2e132e
    """
    query = """
    /* Gets the number of free and paid games signed up for */
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
      WHERE datediff('second', install_timestamp, event_time) BETWEEN 0 AND ({n_days}*86400)
    ),
    last_active_query AS (
      SELECT
      user_id, idfv,
      install_timestamp,
      min(first_event_time) as first_event_time,
      min(last_event_time) as last_event_time
      FROM (
        SELECT user_id, idfv,
        install_timestamp,
        FIRST_VALUE(event_time) OVER ( PARTITION BY user_id, idfv ORDER BY event_time ASC) AS first_event_time,
        FIRST_VALUE(event_time) OVER ( PARTITION BY user_id, idfv ORDER BY event_time DESC) AS last_event_time
        FROM phoenix_events a
        WHERE datediff('second', install_timestamp, event_time) BETWEEN 0 AND ({n_days}*86400)
      ) x
      group by user_id, idfv, install_timestamp
    ),
    joined_idfvs as (
      select a.idfv, b.user_id, b.install_timestamp, b.first_event_time, b.last_event_time
      from user_base a
      left join last_active_query b
      on a.idfv = b.idfv
    ),
    games_played as (
      select pg.user_id, start_time, fee, purse
      from (
        select user_id, tournament_id, gametype_id, start_time
        from ww.played_games as pg
        where start_time between (date('{install_start_date}') - interval '{lookback_days} days') and (date('{install_end_date}') + interval '{n_days} days')
      ) as pg
      /* left join tournament info */
      left join(
        select tournament_id, ttemplate_id, fee, purse
        from ww.tournaments
        ) as t
      using(tournament_id)
    ),
    idfv_user_agg as (
      select a.idfv, a.user_id, a.install_timestamp,
      SUM(case when purse = 0 then 1 else 0 end) as no_payout_games_played,
      SUM(case when fee = 0 and purse > 0 then 1 else 0 end) as freeplays_played,
      SUM(case when fee = 0 and purse = 150 then 1 else 0 end) as premier_freeplays_played,
      SUM(case when fee = 0 and purse = 250 then 1 else 0 end) as regular_freeplays_played,
      SUM(case when fee > 0 and purse > 0 then 1 else 0 end) as paid_games_played
      from joined_idfvs a
      left join games_played b
      on a.user_id::int = b.user_id::int
      and datediff('second', install_timestamp, start_time) BETWEEN -{lookback_days}*86400 AND ({n_days}*86400)
      group by a.idfv, a.user_id, a.install_timestamp
    ),
    idfv_agg as(
      select idfv,
      sum(no_payout_games_played) as no_payout_games_played,
      sum(freeplays_played) as freeplays_played,
      sum(premier_freeplays_played) as premier_freeplays_played,
      sum(regular_freeplays_played) as regular_freeplays_played,
      sum(paid_games_played) as paid_games_played
      from idfv_user_agg
      group by idfv
    )
    select /*+ label('airflow-ml-ww_mobile_fixed_observation_features_v2') */
    idfv,
    no_payout_games_played,
    freeplays_played,
    premier_freeplays_played,
    regular_freeplays_played,
    paid_games_played
    from idfv_agg
    """.format(install_start_date=install_start_date, n_days=n_days, install_end_date=install_end_date,
               lookback_days=90)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['idfv'])
    return df


def get_entry_sizes(install_start_date, install_end_date, n_days=7, connection=None):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=b0a78999-ab3c-4948-8327-3aa36df770e3
    """
    query = """
    /* Gets the average, max, etc., entry fee signed up for */
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
      WHERE datediff('second', install_timestamp, event_time) BETWEEN 0 AND ({n_days}*86400)
    ),
    last_active_query AS (
      SELECT
      user_id, idfv,
      install_timestamp,
      min(first_event_time) as first_event_time,
      min(last_event_time) as last_event_time
      FROM (
        SELECT user_id, idfv,
        install_timestamp,
        FIRST_VALUE(event_time) OVER ( PARTITION BY user_id, idfv ORDER BY event_time ASC) AS first_event_time,
        FIRST_VALUE(event_time) OVER ( PARTITION BY user_id, idfv ORDER BY event_time DESC) AS last_event_time
        FROM phoenix_events a
        WHERE datediff('second', install_timestamp, event_time) BETWEEN 0 AND ({n_days}*86400)
      ) x
      group by user_id, idfv, install_timestamp
    ),
    joined_idfvs as (
      select a.idfv, b.user_id, b.install_timestamp, b.first_event_time, b.last_event_time
      from user_base a
      left join last_active_query b
      on a.idfv = b.idfv
    ),
    games_played as (
      select pg.user_id, start_time, fee, purse
      from (
        select user_id, tournament_id, gametype_id, start_time
        from ww.played_games as pg
        where start_time between (date('{install_start_date}') - interval '{lookback_days} days') and (date('{install_end_date}') + interval '{n_days} days')
      ) as pg
      /* left join tournament info */
      left join(
        select tournament_id, ttemplate_id, fee, purse
        from ww.tournaments
        ) as t
      using(tournament_id)
      where user_id is not null
    ),
    idfv_user_unagg as (
      select a.idfv, a.user_id, fee, start_time
      from joined_idfvs a
      left join games_played b
      on a.user_id::int = b.user_id::int
      and datediff('second', install_timestamp, start_time) BETWEEN -{lookback_days}*86400 AND ({n_days}*86400)
      where fee > 0 and purse > 0
    ),
    idfv_agg as (
      select x.idfv, x.max_entry_fee, x.avg_entry_fee, x.var_entry_fee, y.median_entry_fee
      FROM (
        select idfv,
        MAX(fee) as max_entry_fee,
        AVG(fee) as avg_entry_fee,
        round(VARIANCE(fee), 4) as var_entry_fee
        from idfv_user_unagg
        group by idfv
      ) x
      LEFT JOIN (
        select distinct idfv, 
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY fee ASC) OVER (PARTITION BY idfv) as median_entry_fee
        from idfv_user_unagg
      ) y
      using(idfv)
    )
    select /*+ label('airflow-ml-ww_mobile_fixed_observation_features_v2') */
    a.idfv, max_entry_fee, avg_entry_fee, var_entry_fee, median_entry_fee
    from (select distinct idfv from joined_idfvs) a
    left join idfv_agg b
    using(idfv)
    """.format(install_start_date=install_start_date, n_days=n_days, install_end_date=install_end_date,
               lookback_days=270)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['idfv'])
    return df


def get_device_info(install_start_date, install_end_date, n_days=7, connection=None):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=173091e7-1b93-4735-bb56-ba5a1c9ed801
    """
    query = """
    with user_base as (
      select idfv, install_day 
      from gsnmobile.kochava_device_summary_phoenix
      where install_timestamp::date between '{install_start_date}' and '{install_end_date}'
    ),
    hardware_extended as (
      select idfv, lower(device) device, release_date, platform, model, category, model_code, model_name, code_name, 
        oem_id, brand, cpu_speed, display_resolution, display_diagonal_mm
      from gsnmobile.kochava_device_summary_phoenix_extended
      where not ((release_date is null) and (cpu_speed is null) and (display_resolution) is null and (display_diagonal_mm is null))
    ),
    device_data as (
      select a.idfv, a.install_day,
      platform,
      model,
      category,
      model_code, model_name, code_name, oem_id, brand,
      datediff('day', release_date, install_day) as device_days_since_release,
      release_date, 
      split_part(display_resolution, 'x', 1) as screen_width,
      split_part(display_resolution, 'x', 2) as screen_height,
      case when display_resolution is null then null else
      split_part(display_resolution, 'x', 1) * split_part(display_resolution, 'x', 2) end as num_pixels,
      display_diagonal_mm::float as display_diagonal_mm,
      cpu_speed::float as cpu_speed
      from user_base a 
      left join hardware_extended b
      on a.idfv = b.idfv
      and datediff('day', release_date, install_day) >= 0
    )
    select /*+ label('airflow-ml-ww_mobile_fixed_observation_features_v2') */
    *
    from device_data
    """.format(install_start_date=install_start_date, n_days=n_days, install_end_date=install_end_date)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['idfv'])
    return df

def get_mobile_sessions(install_start_date, install_end_date, n_days=7, connection=None):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=90d805ef-a6ae-43c0-b541-df66347db5f0
    """
    query = """
    /* Mobile Session RFM */
    with user_base as (
      select idfv, install_timestamp
      from gsnmobile.kochava_device_summary_phoenix
      where install_timestamp::date between '{install_start_date}' and '{install_end_date}'
    ),
    mobile_sessions as(
      select idfv, first_active_timestamp, sessions, play_duration_s
      from phoenix.device_day
      where first_active_timestamp between '{install_start_date}' and (date('{install_end_date}') + interval '{n_days} days')
    )
    select /*+ label('airflow-ml-ww_mobile_fixed_observation_features_v2') */
    a.idfv,
    sum(sessions) as count_sessions,
    sum(play_duration_s) as length_sessions,
    avg(play_duration_s) as average_duration_seconds,
    min(play_duration_s) as minimum_duration_seconds,
    max(play_duration_s) as maximum_duration_seconds
    from user_base a
    left join mobile_sessions b
    on a.idfv = b.idfv
    and datediff('second', install_timestamp, first_active_timestamp) between -{lookback_days}*86400 and ({n_days}*86400)
    group by a.idfv
    order by count_sessions DESC
    """.format(install_start_date=install_start_date, n_days=n_days, install_end_date=install_end_date,
               lookback_days=365)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['idfv'])
    return df


def get_previous_days_revenue(install_start_date, install_end_date, n_days=7, connection=None):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=0945c014-b99d-4296-8394-cdf69c4e51b4
    """

    print('getting previous days revenue...')
    previous_window_lengths = [x for x in [1, 2, 3, 4, 5, 6, 7, 14, 30, 45, 60, 90] if x < n_days]
    if len(previous_window_lengths) == 0:
        print('no previous observation window to {}-days observed window'.format(n_days))
        pass
    else:
        iap_revenue_query = """
            -- Day N CEF, with revenue column value & name returned dynamically based on {n_days}
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
                and datediff('second', install_timestamp, event_time) BETWEEN 0 AND ({n_days}*86400)
                group by user_base.idfv
            )
            select /*+ label('airflow-ml-ww_mobile_fixed_observation_features_v2') */
            *
            from day_n_cef
            """

        for i, wndw_len in enumerate(previous_window_lengths):
            print('days{}...'.format(wndw_len))
            if i == 0:
                df = get_dataframe_from_query(
                    iap_revenue_query.format(install_start_date=install_start_date, install_end_date=install_end_date,
                                             n_days=wndw_len), connection=connection)
            else:
                df = pd.merge(
                    df,
                    get_dataframe_from_query(iap_revenue_query.format(install_start_date=install_start_date,
                                                                      install_end_date=install_end_date,
                                                                      n_days=wndw_len), connection=connection),
                    how='left',
                    on='idfv'
                )
        df = df.set_index('idfv')
        return df


def get_platform_cef(install_start_date, install_end_date, n_days=7, connection=None):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=1a079a90-fa18-429a-bb8e-e10de8c719bb
    """
    query = """
    /* Observed CEF by platform */
    with idfv_base as (
      select idfv, install_timestamp
      from gsnmobile.kochava_device_summary_phoenix
      where install_timestamp::date between '{install_start_date}' and '{install_end_date}'
    ),
    cef_events as (
      SELECT
      idfv,
      event_time,
      ww_platform ,
      cef
      FROM phoenix.tournament_cef
    ),
    platform_cef as (
      select a.idfv, 
      coalesce(sum(case when ww_platform  = 'app' then cef else null end),0) as app_iap_revenue,
      coalesce(sum(case when ww_platform  = 'moweb' then cef else null end),0) as moweb_iap_revenue,
      coalesce(sum(case when ww_platform  = 'desktop' then cef else null end),0) as desktop_iap_revenue
      from idfv_base a 
      left join cef_events b
      on a.idfv = b.idfv
      and datediff('second', install_timestamp, event_time) between 0 and ({n_days}*86400)
      group by a.idfv
    )
    select /*+ label('airflow-ml-ww_mobile_fixed_observation_features_v2') */
    *
    from platform_cef
    """.format(install_start_date=install_start_date, n_days=n_days, install_end_date=install_end_date)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['idfv'])
    return df


def get_other_game_spend(install_start_date, install_end_date, n_days=7, connection=None):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=d7f3e922-42df-4bdf-878c-e8c953210d14
    """
    query = """
    /* Get spend in other GSN Games */
    with user_base as (
      select idfv, install_day 
      from gsnmobile.kochava_device_summary_phoenix
      where install_timestamp::date between '{install_start_date}' and '{install_end_date}'
    ),
    device_mapping as (
      select idfv, id as synthetic_id
      from phoenix.dim_device_mapping
      where id_type = 'synthetic_id'
      and event_time between '{install_start_date}' and (date('{install_end_date}') + interval '{n_days} days')
    ),
    mapped_devices as (
      select a.idfv, a.install_day, b.synthetic_id
      from user_base a
      left join device_mapping b
      using(idfv)
    ),
    nonww_payments as (
      SELECT
      synthetic_id,
      app,
      event_day,
      event_time,
      coalesce(amount_paid_usd,0) as amount_paid_usd
      FROM gsnmobile.events_payments
      WHERE TRUE /*Should be where app is not worldwinner, but worldwinner is not in this table currently*/
      and date(event_day) between (date('{install_start_date}') - interval '{lookback_days} days') and (date('{install_end_date}') + interval '{n_days} days')
      /* TODO: Add bingobash revenue: no mapping exists though */
    )
    SELECT /*+ label('airflow-ml-ww_mobile_fixed_observation_features_v2') */
    i.idfv,
    count(distinct app) as other_apps_payed,
    datediff('day',  min(p1.event_day), max(p1.event_day)) as window_nonww_purchases,
    datediff('day',  max(p1.event_day), min(i.install_day)) as days_since_nonww_purchase,
    count(p1.amount_paid_usd) as nonww_transactions,
    coalesce(sum(p1.amount_paid_usd),0) as nonww_iap_revenue		
    FROM mapped_devices as i
    LEFT JOIN nonww_payments as p1
    using (synthetic_id)
    WHERE datediff('second', i.install_day, p1.event_day) BETWEEN -{lookback_days}*86400 AND ({n_days}*86400)
    GROUP BY i.idfv
    """.format(install_start_date=install_start_date, n_days=n_days, install_end_date=install_end_date,
               lookback_days=730)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['idfv'])
    return df


### New Getter Functions ###
def get_feature_callables(training_start_date=None, categorical_handler=None):
    """Return a list of functions each of which returns a dataframe with features.

    The first callable in the list is responsible for returning the full dataframe that
    all others are left joined against.

    :param training_start_date: The earliest date used for training.  In the case of a training set,
    this is the earliest install date in the training set.  In the case of a test set, it is the
    earliest install date for the training set used to train the model.  This ensures the train and
    test sets have the same features.  If this is None, all feature callables will be included.

    """
    if training_start_date is None:
        training_start_date = date.today()
    training_start_date = convert_to_date(training_start_date)

    default_date = date(2014, 1, 1)
    Feature = namedtuple('Feature',
                         ['feature_callable', 'start_date', 'default_callable', 'ordinal_callable', 'onehot_callable'])
    Feature.__new__.__defaults__ = (
    None, None, None)  # mark default_callable, ordinal_callable, and onehot_callable optional, default None
    features_definitions = [
        Feature(get_idfv_base, default_date),
        Feature(get_iap_revenue, default_date),
        Feature(get_rfm_events, default_date),
        Feature(get_previous_ltd_cef, default_date),
        Feature(get_install_time_cyclical, default_date),
        Feature(get_network_name, default_date),
        Feature(get_all_user_info, default_date),
        Feature(get_rfm_deposits_withdrawals, default_date),
        Feature(get_past_churn_metrics, default_date),
        Feature(get_rfm_days_played, default_date),
        Feature(get_web_sessions, default_date),
        Feature(get_games_played, default_date),
        Feature(get_first_last_balance, default_date),
        Feature(get_net_winnings, default_date),
        Feature(get_freeplays, default_date),
        Feature(get_entry_sizes, default_date),
        Feature(get_device_info, default_date),
        Feature(get_mobile_sessions, default_date),
        Feature(get_platform_cef, default_date),
        Feature(get_other_game_spend, default_date),
    ]
    feature_callables = []
    for features_definition in features_definitions:
        if training_start_date >= features_definition.start_date:
            if categorical_handler == 'default' and features_definition.default_callable is not None:
                feature_callables.append(features_definition.default_callable)
            elif categorical_handler == 'ordinal' and features_definition.ordinal_callable is not None:
                feature_callables.append(features_definition.ordinal_callable)
            elif categorical_handler == 'onehot' and features_definition.onehot_callable is not None:
                feature_callables.append(features_definition.onehot_callable)
            else:
                feature_callables.append(features_definition.feature_callable)
    return feature_callables


def get_feature_df(feature_callable, install_start_date, install_end_date, n_days=1, connection=None):
    """Get each feature dataframe using `feature_callable` and do some basic validation on each."""
    df = feature_callable(install_start_date=install_start_date, install_end_date=install_end_date, n_days=n_days,
                          connection=connection)
    return df


def get_features_df(install_start_date, install_end_date, n_days_observation=1, n_days_horizon=360,
                    categorical_handler='default', training_start_date=None, n_jobs=1, connection=None):
    """Create a dataframe with features only (target not included) for installs between
    `install_start_date` and `install_end_date` inclusive.

    install_start_date   Start of the install window.
    install_end_date     End of the install window.  Includes this date.
    n_days_observation   Number of days from install for observation of game behavior.
    n_days_horizon       Number of days from install for the LTV prediction.
    categorical_handler  String name for the categorical handler.  'default', 'ordinal', 'onehot', None
    training_start_date  Used to determine the set of features to use in this data set.
                         In the case of gathering a data set for training, this will be the beginning of
                         the training range. In the case of gathering a data set for validation or testing,
                         this will still be the beginning of the training range used for the model.
    include_weights      If True, include a column 'weight' to be used for sample weighing.
    n_jobs               Concurrency of queries.  If greater than 1, this will use joblib Parallel.
    connection           (Optional) connection object to use.
    """
    install_start_date = convert_to_date(install_start_date)
    install_end_date = convert_to_date(install_end_date)
    assert install_start_date <= install_end_date, 'install_start_date must be on or before install_end_date'
    assert n_days_observation < n_days_horizon, 'n_days_observation must be less than n_days_horizon'
    assert install_end_date <= date.today() - timedelta(
        days=n_days_observation), 'install_end_date is not distant enough'
    feature_callables = get_feature_callables(training_start_date=training_start_date,
                                              categorical_handler=categorical_handler)
    # append all feature columns
    if n_jobs > 1:
        df_list = Parallel(n_jobs=n_jobs)(
            delayed(get_feature_df)(
                feature_callable,
                install_start_date,
                install_end_date,
                n_days=n_days_observation,
                connection=connection
            ) for feature_callable in feature_callables
        )
        df = reduce(lambda left_df, right_df: pd.merge(left=left_df, right=right_df, how='left', left_index=True,
                                                       right_index=True), df_list)
    else:
        df = None
        for feature_callable in feature_callables:
            print(feature_callable.__name__)
            if df is None:
                df = get_feature_df(
                    feature_callable,
                    install_start_date,
                    install_end_date,
                    n_days=n_days_observation,
                    connection=connection,
                )
            else:
                df = pd.merge(
                    left=df,
                    right=feature_callable(
                        install_start_date,
                        install_end_date,
                        n_days=n_days_observation,
                        connection=connection
                    ),
                    how='left',
                    left_index=True,
                    right_index=True,
                )
            print(df.shape)

    df = update_df_index(df, n_days_observation)
    #df = update_dataframe_index_with_days_observation_days_horizon(df, n_days_observation=n_days_observation,
    #                                                               n_days_horizon=n_days_horizon)
    # df = df.fillna(0)
    return df
