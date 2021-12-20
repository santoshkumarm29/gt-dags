"""
Features for WW Segmentation
"""

import pandas as pd
import numpy as np
import re
from common.db_utils import get_dataframe_from_query
from pandas.api.types import CategoricalDtype
from sklearn.preprocessing import MinMaxScaler


def update_df_index(df, start_date=None, n_days_activity=None, n_days_churn=None, n_min_days_since_install=None):
    """Include 'n_days_observation' and 'n_days_horizon' in the index to avoid later accounting mistakes.
    """
    df = (
        df
            .reset_index()
            .assign(start_date=start_date)
            .assign(n_days_activity=n_days_activity)
            .assign(n_days_churn=n_days_churn)
            .assign(n_min_days_since_install=n_min_days_since_install)
            .set_index(['user_id', 'start_date', 'n_days_activity', 'n_days_churn', 'n_min_days_since_install'])
    )
    return df


def get_features_df(start_date, n_days_activity, n_days_churn, n_min_days_since_install, connection=None,
                    verbose=False, model_dct_all=None):
    """
    """

    ## Get Feature Devices
    print(get_user_id_base.__name__)
    df = get_user_id_base(
        start_date=start_date,
        n_days_activity=n_days_activity,
        n_days_churn=n_days_churn,
        n_min_days_since_install=n_min_days_since_install,
        connection=connection
    )
    # Append all feature columns
    feature_callables = [
        get_activity_games_info,
        get_past_90days_games_info,
        get_rfm_deposits_withdrawals,
        get_session_metrics,
        get_last_balance,
        get_mobile_player,
        get_past_churn_metrics,
        get_net_winnings,
        get_net_winnings_lifetime,
        get_rfm_deposits_withdrawals_lifetime,
        get_session_preference,
        get_cef_preference,
        get_monthly_time_of_day_preference_df,
        get_lifetime_time_of_day_preference_df,
        get_monthly_tournament_preference_df,
        get_lifetime_tournament_preference_df
    ]
    for feature_callable in feature_callables:
        print(feature_callable.__name__)
        temp = feature_callable(
            start_date=start_date,
            n_days_activity=n_days_activity,
            n_days_churn=n_days_churn,
            n_min_days_since_install=n_min_days_since_install,
            connection=connection,
            model_dct_all=model_dct_all
        )
        df = pd.merge(
            left=df,
            right=temp,
            how='left',
            left_index=True,
            right_index=True,
        )

        # Any feature that should not fillna with zeros is responsible for returning rows for all installs!
    df = df.fillna(0)

    # Ad-hoc columns added
    df['positive_total_net_winnings'] = df['total_net_winnings']
    df['negative_total_net_winnings'] = df['total_net_winnings']
    df['positive_lifetime_total_net_winnings'] = df['lifetime_total_net_winnings']
    df['negative_lifetime_total_net_winnings'] = df['lifetime_total_net_winnings']

    return df


def get_user_id_base(start_date, n_days_activity, n_days_churn, n_min_days_since_install, connection=None, **kwargs):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=41bf7762-fd5f-483f-b28b-6d89499e99c8
    """

    query = """
    with users as (
      select user_id, createdate::date as create_date,
      case when ISUTF8(state) then state
      when ISUTF8(state) = False then 'not_utf8'
      when state IS NULL then 'unknown'
      else 'unknown' end as state,
      case when ISUTF8(country) then country
      when ISUTF8(country) = False then 'not_utf8'
      when country IS NULL then 'unknown'
      else 'unknown' end as country,
      datediff('second', createdate, first_game_played_date) / (60*60) as hrs_from_creation_date_to_first_game,
      datediff('second', first_game_played_date, coalesce(real_money_date, '2050-01-01'))/ (60*60) as hrs_from_first_game_to_real_money_game
      from ww.dim_users
    ),
    activity as (
      select user_id, min(start_time) as min_activity_event_day, max(start_time) as max_activity_event_day, 
      count(distinct start_time::date) as days_played_frequency
      from ww.played_games as pg
      where start_time >= date('{start_date}') and start_time <= (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')
      group by user_id
    ),
    veterans as (
      select u.user_id, state, country, hrs_from_creation_date_to_first_game, hrs_from_first_game_to_real_money_game,
      days_played_frequency, 
      datediff('day', max_activity_event_day, (date('{start_date}') + interval '{n_days_activity} days')) as days_played_recency,
      datediff('day', date('{start_date}'), min_activity_event_day) as forwards_days_played_recency,
      datediff('day', create_date, '{start_date}') as player_age_days
      from users u
      join activity a
      using(user_id)
      where datediff('day', create_date, min_activity_event_day) >= '{n_min_days_since_install}'
    )
        select *
        from veterans
    """.format(start_date=start_date, n_days_activity=n_days_activity, n_days_churn=n_days_churn,
               n_min_days_since_install=n_min_days_since_install)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['user_id'])

    categoricals = {
        'country': ['unknown', 'US', 'CA', 'GB', 'AU', 'TH', 'NZ', 'DE', 'SG', 'IN', 'PH'],

        'state': ['unknown', 'CA', 'NY', 'TX', 'PA', 'OH', 'MI', 'GA', 'NC', 'FL']
    }
    for column_name, categories in categoricals.items():
        category = CategoricalDtype(categories=categories, ordered=True)
        df[column_name] = df[column_name].astype(category)
        df[column_name] = df[column_name].cat.codes

    df.loc[((df.country == 1)) & (df.state <= 0), 'state'] = -2
    df.loc[((df.country == 2)) & (df.state <= 0), 'state'] = -3

    return df


def get_target(start_date, n_days_activity, n_days_churn, n_min_days_since_install, connection=None, **kwargs):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=03e689ef-0094-4b8e-8cb8-77d007dcb456
    """

    query = """
    with users as (
      select user_id, createdate::date as create_date
      from ww.dim_users
    ),
    activity as (
      select user_id, min(start_time) as min_activity_event_day
      from ww.played_games as pg
      where start_time >= date('{start_date}') and start_time <= (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')
      group by user_id
    ),
    veterans as (
      select u.user_id
      from users u
      join activity a
      using(user_id)
      where datediff('day', create_date, min_activity_event_day) >= '{n_min_days_since_install}'
    ),
    churn as (
      select user_id, min(start_time) as min_churn_event_day
      from ww.played_games as pg
      where start_time >= (date('{start_date}') + interval '{n_days_activity} days')
      and start_time <= (date('{start_date}') + interval '{n_days_activity} days' + interval '{n_days_churn}' - interval '1 days')
      group by user_id
    )
        select veterans.user_id, case when min_churn_event_day IS NULL then 1 else 0 end as churn
        from veterans
        left join churn
        using(user_id)
    """.format(start_date=start_date, n_days_activity=n_days_activity, n_days_churn=n_days_churn,
               n_min_days_since_install=n_min_days_since_install)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['user_id'])
    return df


def get_activity_games_info(start_date, n_days_activity, n_days_churn, n_min_days_since_install, connection=None,
                            **kwargs):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=91c041eb-40d5-49f9-8914-6a3fee41e46e
    """

    query = """
    with users as (
      select user_id, createdate::date as create_date
      from ww.dim_users
    ),
    activity as (
      select user_id, min(start_time) as min_activity_event_day
      from ww.played_games as pg
      where start_time >= date('{start_date}') and start_time <= (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')
      group by user_id
    ),
    veterans as (
      select u.user_id
      from users u
      join activity a
      using(user_id)
      where datediff('day', create_date, min_activity_event_day) >= '{n_min_days_since_install}'
    ),
    games_played as (
      select pg.user_id, start_time, gametype_id, t.tournament_id as tournament_id, tt.ttemplate_id, t.fee as tournament_fee,
      coalesce(tt.purse, 0) as template_purse,
      coalesce(tt.fee, 0) as template_fee,
      coalesce(tt.external_fee, 0) as template_external_fee,
      coalesce(tt.max_players, 9999) as template_max_players,
      coalesce(tt.games_min, 0) as template_games_min
      from (
        select user_id, tournament_id, gametype_id, start_time
        from ww.played_games as pg
        where start_time >= date('{start_date}') and start_time <= (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')
      ) as pg
      left join(
        select tournament_id, ttemplate_id, fee
        from ww.tournaments
        ) as t
      using(tournament_id)
      left join ww.dim_tournament_templates as tt
      using(ttemplate_id) 
    ),
    raw_games_info as(
        select *
        from veterans
        left join games_played
        using(user_id)
    )
        select user_id,
        count(distinct ttemplate_id) as num_distinct_tournament_types,
        count(distinct tournament_id) as num_distinct_tournaments_entered,
        count(start_time) as num_all_tournament_entries,
        SUM(case when tournament_fee = 0 then 1 else 0 end) as num_free_entries,
        SUM(case when tournament_fee > 0 then 1 else 0 end) as num_paid_entries
        from raw_games_info
        group by user_id
    """.format(start_date=start_date, n_days_activity=n_days_activity, n_days_churn=n_days_churn,
               n_min_days_since_install=n_min_days_since_install)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['user_id'])
    return df


def get_past_90days_games_info(start_date, n_days_activity, n_days_churn, n_min_days_since_install, connection=None,
                               **kwargs):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=fe60e149-cc1b-455b-95b5-ebffd123130f
    """

    query = """
    with users as (
      select user_id, createdate::date as create_date
      from ww.dim_users
    ),
    activity as (
      select user_id, min(start_time) as min_activity_event_day
      from ww.played_games as pg
      where start_time >= date('{start_date}') and start_time <= (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')
      group by user_id
    ),
    veterans as (
      select u.user_id
      from users u
      join activity a
      using(user_id)
      where datediff('day', create_date, min_activity_event_day) >= '{n_min_days_since_install}'
    ),
    games_played as (
      select pg.user_id, start_time, gametype_id, t.tournament_id as tournament_id, tt.ttemplate_id, t.fee as tournament_fee,
      coalesce(tt.purse, 0) as template_purse,
      coalesce(tt.fee, 0) as template_fee,
      coalesce(tt.external_fee, 0) as template_external_fee,
      coalesce(tt.max_players, 9999) as template_max_players,
      coalesce(tt.games_min, 0) as template_games_min -- could be used as a delta for closeness to unlocking tournaments?  
      from (
        select user_id, tournament_id, gametype_id, start_time
        from ww.played_games as pg
        where start_time < date('{start_date}') and start_time >= (date('{start_date}') - interval '90 days')
      ) as pg
      left join(
        select tournament_id, ttemplate_id, fee
        from ww.tournaments
        ) as t
      using(tournament_id)
      left join ww.dim_tournament_templates as tt
      using(ttemplate_id) 
    ),
    raw_games_info as(
        select *
        from veterans
        left join games_played
        using(user_id)
    )
        select user_id,
        count(distinct ttemplate_id) as past_num_distinct_tournament_types,
        count(distinct tournament_id) as past_num_distinct_tournaments_entered,
        count(start_time) as past_num_all_tournament_entries,
        SUM(case when tournament_fee = 0 then 1 else 0 end) as past_num_free_entries,
        SUM(case when tournament_fee > 0 then 1 else 0 end) as past_num_paid_entries
        from raw_games_info
        group by user_id
    """.format(start_date=start_date, n_days_activity=n_days_activity, n_days_churn=n_days_churn,
               n_min_days_since_install=n_min_days_since_install)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['user_id'])
    return df


def get_rfm_deposits_withdrawals(start_date, n_days_activity, n_days_churn, n_min_days_since_install, connection=None,
                                 **kwargs):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=d5423a32-9279-400c-99e9-6dc92f4ae3e5
    """

    query = """
    with users as (
      select user_id, createdate::date as create_date
      from ww.dim_users
    ),
    activity as (
      select user_id, min(start_time) as min_activity_event_day
      from ww.played_games as pg
      where start_time >= date('{start_date}') and start_time <= (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')
      group by user_id
    ),
    veterans as (
      select u.user_id
      from users u
      join activity a
      using(user_id)
      where datediff('day', create_date, min_activity_event_day) >= '{n_min_days_since_install}'
    ),
        rfm_d as (
          select user_id, sum(amount) as deposits_amount, count(amount) as deposits_frequency, datediff('day', max(trans_date),
                 (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')) as deposits_recency
          from ww.internal_transactions
          where trans_date >= date('{start_date}') and trans_date <= (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')
          and trans_type='DEPOSIT'
          group by user_id
        ),
        rfm_w as (
          select user_id, sum(amount) as withdrawals_amount, count(amount) as withdrawals_frequency, datediff('day', max(trans_date),
                 (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')) as withdrawals_recency
          from ww.internal_transactions
          where trans_date >= date('{start_date}') and trans_date <= (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')
          and trans_type='WITHDRAWAL'
          group by user_id
        )
          select veterans.user_id,
          withdrawals_recency, withdrawals_frequency, withdrawals_amount,
          deposits_recency, deposits_frequency, deposits_amount
          from veterans
          left join rfm_d
          using(user_id)
          left join rfm_w
          using(user_id)
    """.format(start_date=start_date, n_days_activity=n_days_activity, n_days_churn=n_days_churn,
               n_min_days_since_install=n_min_days_since_install)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['user_id'])
    df = df.fillna(0)
    return df


def get_rfm_deposits_withdrawals_lifetime(start_date, n_days_activity, n_days_churn, n_min_days_since_install,
                                          connection=None, **kwargs):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=d5423a32-9279-400c-99e9-6dc92f4ae3e5
    """

    query = """
    with users as (
      select user_id, createdate::date as create_date
      from ww.dim_users
    ),
    activity as (
      select user_id, min(start_time) as min_activity_event_day
      from ww.played_games as pg
      where start_time >= date('{start_date}') and start_time <= (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')
      group by user_id
    ),
    veterans as (
      select u.user_id
      from users u
      join activity a
      using(user_id)
      where datediff('day', create_date, min_activity_event_day) >= '{n_min_days_since_install}'
    ),
        rfm_d as (
          select user_id, sum(amount) as deposits_amount, count(amount) as deposits_frequency, datediff('day', max(trans_date),
                 (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')) as deposits_recency
          from ww.internal_transactions
          where trans_date <= (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')
          and trans_type='DEPOSIT'
          group by user_id
        ),
        rfm_w as (
          select user_id, sum(amount) as withdrawals_amount, count(amount) as withdrawals_frequency, datediff('day', max(trans_date),
                 (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')) as withdrawals_recency
          from ww.internal_transactions
          where trans_date <= (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')
          and trans_type='WITHDRAWAL'
          group by user_id
        )
          select veterans.user_id,
          withdrawals_recency as lifetime_withdrawals_recency, 
          withdrawals_frequency as lifetime_withdrawals_frequency,
          withdrawals_amount as lifetime_withdrawals_amount,
          deposits_recency as lifetime_deposits_recency,
          deposits_frequency as lifetime_deposits_frequency,
          deposits_amount as lifetime_deposits_amount
          from veterans
          left join rfm_d
          using(user_id)
          left join rfm_w
          using(user_id)
    """.format(start_date=start_date, n_days_activity=n_days_activity, n_days_churn=n_days_churn,
               n_min_days_since_install=n_min_days_since_install)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['user_id'])
    df = df.fillna(0)
    return df


def get_session_metrics(start_date, n_days_activity, n_days_churn, n_min_days_since_install, connection=None, **kwargs):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=5a11c060-5dad-4e39-ba82-473f33c9d645
    """

    query = """
    with users as (
      select user_id, createdate::date as create_date
      from ww.dim_users
    ),
    activity as (
      select user_id, min(start_time) as min_activity_event_day
      from ww.played_games as pg
      where start_time >= date('{start_date}') and start_time <= (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')
      group by user_id
    ),
    veterans as (
      select u.user_id
      from users u
      join activity a
      using(user_id)
      where datediff('day', create_date, min_activity_event_day) >= '{n_min_days_since_install}'
    ),
    web_sessions as(
      select user_id, 
      sum(session) as count_sessions,
      sum(duration_seconds) as length_sessions,
      avg(duration_seconds) as average_duration_seconds,
      min(duration_seconds) as minimum_duration_seconds,
      max(duration_seconds) as maximum_duration_seconds
      from ww.events_web_sessions
      where start_time >= date('{start_date}') and start_time <= (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')
      group by user_id
    )
        select *
        from veterans
        left join web_sessions
        using(user_id)
    """.format(start_date=start_date, n_days_activity=n_days_activity, n_days_churn=n_days_churn,
               n_min_days_since_install=n_min_days_since_install)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['user_id'])
    return df


def get_last_balance(start_date, n_days_activity, n_days_churn, n_min_days_since_install, connection=None, **kwargs):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=29cfdf47-52ec-425f-a8b4-eda90bc70a53
    """

    query = """
    with users as (
      select user_id, createdate::date as create_date
      from ww.dim_users
    ),
    activity as (
      select user_id, min(start_time) as min_activity_event_day
      from ww.played_games as pg
      where start_time >= date('{start_date}') and start_time <= (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')
      group by user_id
    ),
    veterans as (
      select u.user_id
      from users u
      join activity a
      using(user_id)
      where datediff('day', create_date, min_activity_event_day) >= '{n_min_days_since_install}'
    ),
    ranked_trans as(
      SELECT user_id,
      (balance + play_money) as last_playable_balance,
      (balance) as last_real_amount_balance,
      (play_money) as last_play_amount_balance,
      ROW_NUMBER() OVER (
        PARTITION BY user_id
        ORDER BY trans_date DESC
      ) AS rn
      from ww.internal_transactions as it
      where trans_date >= date('{start_date}') and trans_date <= (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')
    )
        select veterans.user_id, last_playable_balance, last_real_amount_balance--, last_play_amount_balance
        from veterans
        left join ranked_trans
        using(user_id)
        where rn = 1
    """.format(start_date=start_date, n_days_activity=n_days_activity, n_days_churn=n_days_churn,
               n_min_days_since_install=n_min_days_since_install)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['user_id'])
    return df


def get_mobile_player(start_date, n_days_activity, n_days_churn, n_min_days_since_install, connection=None, **kwargs):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=64cf975b-04d1-49bb-9872-8ba4981288e2
    """

    query = """
    with users as (
      select user_id, createdate::date as create_date
      from ww.dim_users
    ),
    activity as (
      select user_id, min(start_time) as min_activity_event_day
      from ww.played_games as pg
      where start_time >= date('{start_date}') and start_time <= (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')
      group by user_id
    ),
    veterans as (
      select u.user_id
      from users u
      join activity a
      using(user_id)
      where datediff('day', create_date, min_activity_event_day) >= '{n_min_days_since_install}'
    ),
    has_installed_mobile as(
      select id::int as user_id, min(event_time) as mobile_install_date
      from phoenix.dim_device_mapping
      where event_time <= (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')
      and id_type = 'user_id'
      group by id
    ),
    RFM_mobile_play as(
      select user_id,
      datediff('day', (date('{start_date}') - interval '90 days'), max(event_day)) as mobile_play_recency,
      COUNT(cef) as mobile_play_frequency,
      SUM(cef) as mobile_play_amount
      from phoenix.tournament_cef
      where event_day <= (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')
      and event_day >= (date('{start_date}') - interval '90 days')
      group by user_id
    )
        select veterans.user_id,
        (case when mobile_install_date IS NOT NULL then 1 else 0 end) as has_installed_mobile_app,
        coalesce(mobile_play_recency, 0) as mobile_play_recency,
        coalesce(mobile_play_frequency, 0) as mobile_play_frequency,
        coalesce(mobile_play_amount, 0) as mobile_play_amount
        from veterans
        left join has_installed_mobile
        using(user_id)
        left join RFM_mobile_play
        using(user_id)
    """.format(start_date=start_date, n_days_activity=n_days_activity, n_days_churn=n_days_churn,
               n_min_days_since_install=n_min_days_since_install)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['user_id'])
    return df


def get_past_churn_metrics(start_date, n_days_activity, n_days_churn, n_min_days_since_install, connection=None,
                           **kwargs):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=3418e606-b41f-4811-8276-d66299afef1a
    """

    query = """
    with users as (
      select user_id, createdate::date as create_date
      from ww.dim_users
    ),
    activity as (
      select user_id, min(start_time) as min_activity_event_day
      from ww.played_games as pg
      where start_time >= date('{start_date}') and start_time <= (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')
      group by user_id
    ),
    veterans as (
      select u.user_id
      from users u
      join activity a
      using(user_id)
      where datediff('day', create_date, min_activity_event_day) >= '{n_min_days_since_install}'
    ),
    deltas as(
      select * from(
        select user_id, start_date_str, start_date, 
        start_date - LAG(start_date, 1, 0) OVER (PARTITION BY user_id
                                                 ORDER BY start_date) AS difference,
        RANK() OVER (PARTITION BY user_id
                                                 ORDER BY start_date) AS rn
        from (
          select distinct user_id, datediff('day', start_time::date, '{start_date}') as start_date, start_time::date as start_date_str
          from ww.played_games
          WHERE start_time <= date('{start_date}') and start_time >= (date('{start_date}') - interval '1095 days')
        ) as x
      ) as y
    )	
        select veterans.user_id,
        COUNT(case when difference >= 30 then difference else NULL end) as num_churns_over30,
        coalesce(MAX(difference), -999) as max_churn_length,
        coalesce(AVG(case when difference >= 30 then difference else NULL end), -999) as avg_churn_length_over30,
        coalesce(AVG(case when difference < 30 then difference else NULL end), -999) as avg_churn_length_under30,
        coalesce(MAX(case when rn = 1 then start_date else NULL end), -999) as last_seen_days_ago
        from veterans
        left join deltas
        using (user_id)
        group by veterans.user_id
    """.format(start_date=start_date, n_days_activity=n_days_activity, n_days_churn=n_days_churn,
               n_min_days_since_install=n_min_days_since_install)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['user_id'])
    return df


def get_games_played(start_date, n_days_activity, n_days_churn, n_min_days_since_install, connection=None, **kwargs):
    ## NOTE: The python portion of this query is taking a LONG time... can performance be optimized by being written in SQL?
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=3dc70587-1c61-4563-a885-116d3a012a6c
    """

    query = """
    /* Gets Counts of games played by category and/or gametype_id */
    with users as (
      select user_id, createdate::date as create_date
      from ww.dim_users
    ),
    activity as (
      select user_id, min(start_time) as min_activity_event_day
      from ww.played_games as pg
      where start_time >= date('{start_date}') and start_time <= (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')
      group by user_id
    ),
    veterans as (
      select u.user_id
      from users u
      join activity a
      using(user_id)
      where datediff('day', create_date, min_activity_event_day) >= '{n_min_days_since_install}'
    ),
        games_played as ( -- 931 Million Rows
          select user_id user_id, gametype_id
          from ww.played_games
          where start_date <= (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')
        ),
        game_types as (
          select gametype_id, category 
          from ww.dim_gametypes
        )

        select veterans.user_id, category, games_played.gametype_id, count(*) as games_played
        from veterans
        left join games_played -- 931 Million Rows Here
        using(user_id)
        left join game_types -- Supplementary Game Info, no problem
        using(gametype_id)
        group by veterans.user_id, category, games_played.gametype_id
    """.format(start_date=start_date, n_days_activity=n_days_activity, n_days_churn=n_days_churn,
               n_min_days_since_install=n_min_days_since_install)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['user_id'])
    df = df.reset_index()

    ## number of games played, by category
    gp_cat = df.pivot_table(index='user_id', columns=['category'], values='games_played', aggfunc=np.sum)
    ## number of games played, by gametype_id
    gp_game = df.pivot_table(index='user_id', columns=['gametype_id'], values='games_played', aggfunc=np.sum)

    ## Game Category Distribution
    df = gp_cat.apply(lambda x: (x / np.sum(x.fillna(0))), 1)
    ## Favorite Game
    df['favorite_game'] = gp_game.idxmax(1)
    ## Favorite Category
    df['favorite_gametype'] = gp_cat.idxmax(1)
    ## Favorite Game_ID Dedication
    df['favorite_game_dedication'] = gp_game.apply(lambda x: (x / np.sum(x.fillna(0))).max(), 1)
    ## Favorite Category Dedication
    df['favorite_gametype_dedication'] = gp_cat.apply(lambda x: (x / np.sum(x.fillna(0))).max(), 1)

    categoricals = {
        'favorite_gametype': ['Arcade', 'Card', 'Game Show', 'Sports', 'Strategy', 'Word'],
    }
    for column_name, categories in categoricals.items():
        category = CategoricalDtype(categories=categories, ordered=True)
        df[column_name] = df[column_name].astype(category)
        df[column_name] = df[column_name].cat.codes
    df = df.fillna(0)
    df = df.set_index('user_id')

    return df


def get_net_winnings(start_date, n_days_activity, n_days_churn, n_min_days_since_install, connection=None, **kwargs):
    """
    """
    query = """
    /* Get total net winnings */
    with users as (
      select user_id, createdate::date as create_date
      from ww.dim_users
    ),
    activity as (
      select user_id, min(start_time) as min_activity_event_day
      from ww.played_games as pg
      where start_time between date('{start_date}') and (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')
      group by user_id
    ),
    veterans as (
      select u.user_id
      from users u
      join activity a
      using(user_id)
      where datediff('day', create_date, min_activity_event_day) >= '{n_min_days_since_install}'
    ),
    game_transactions as (
      select user_id, trans_date, amount, trans_type
      from ww.internal_transactions
      where trans_date between date('{start_date}') and (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')
      and trans_type in ('WINNINGS', 'SIGNUP')
    ),
    user_agg as (
      select a.user_id,
      coalesce(SUM(case when trans_type = 'WINNINGS' then amount else NULL end), 0) as total_winnings,
      coalesce(SUM(case when trans_type = 'SIGNUP' then amount else NULL end), 0) as total_signups,
      coalesce(SUM(case when trans_type = 'WINNINGS' then amount else NULL end), 0) + coalesce(SUM(case when trans_type = 'SIGNUP' then amount else NULL end), 0) as total_net_winnings
      from veterans a
      left join game_transactions b
      on a.user_id::int = b.user_id::int
      group by a.user_id
    )
        select user_id, total_winnings, total_signups, total_net_winnings
        from user_agg
    """.format(start_date=start_date, n_days_activity=n_days_activity, n_days_churn=n_days_churn,
               n_min_days_since_install=n_min_days_since_install)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['user_id'])
    return df


def get_net_winnings_lifetime(start_date, n_days_activity, n_days_churn, n_min_days_since_install, connection=None,
                              **kwargs):
    """
    """
    query = """
    /* Get total net winnings in lifetime*/
    with users as (
      select user_id, createdate::date as create_date
      from ww.dim_users
    ),
    activity as (
      select user_id, min(start_time) as min_activity_event_day
      from ww.played_games as pg
      where start_time between date('{start_date}') and (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')
      group by user_id
    ),
    veterans as (
      select u.user_id
      from users u
      join activity a
      using(user_id)
      where datediff('day', create_date, min_activity_event_day) >= '{n_min_days_since_install}'
    ),
    game_transactions as (
      select user_id, trans_date, amount, trans_type
      from ww.internal_transactions
      where trans_date <= (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')
      and trans_type in ('WINNINGS', 'SIGNUP')
    ),
    user_agg as (
      select a.user_id,
      coalesce(SUM(case when trans_type = 'WINNINGS' then amount else NULL end), 0) as lifetime_total_winnings,
      coalesce(SUM(case when trans_type = 'SIGNUP' then amount else NULL end), 0) as lifetime_total_signups,
      coalesce(SUM(case when trans_type = 'WINNINGS' then amount else NULL end), 0) + coalesce(SUM(case when trans_type = 'SIGNUP' then amount else NULL end), 0) as lifetime_total_net_winnings
      from veterans a
      left join game_transactions b
      on a.user_id::int = b.user_id::int
      group by a.user_id
    )
        select user_id, lifetime_total_winnings, lifetime_total_signups, lifetime_total_net_winnings
        from user_agg
    """.format(start_date=start_date, n_days_activity=n_days_activity, n_days_churn=n_days_churn,
               n_min_days_since_install=n_min_days_since_install)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['user_id'])
    return df


def get_session_preference(start_date, n_days_activity, n_days_churn, n_min_days_since_install, connection=None,
                           **kwargs):
    """
    https:
    """

    query = """
    with users as (
      select user_id, createdate::date as create_date
      from ww.dim_users
    ),
    activity as (
      select user_id, min(start_time) as min_activity_event_day
      from ww.played_games as pg
      where start_time >= date('{start_date}') and start_time <= (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')
      group by user_id
    ),
    registration_cohort as (
      select u.user_id
      from users u
      join activity a
      using(user_id)
      where datediff('day', create_date, min_activity_event_day) >= '{n_min_days_since_install}'
    ),
    app_sessions as   
    (
      select distinct
      user_id,
      sum(duration_s) as total_duration_s_app,
      count(distinct session||event_day||idfv) as total_sessions_app,
      count(distinct event_day) as app_days,
      count(distinct week(event_day)||year(event_day)) as app_weeks,
      count(distinct month(event_day)||year(event_day)) as app_months
      from
      (
        select distinct
        e.user_id,
        ds.idfv,
        ds.event_day,
        ds.session,
        ds.start_ts,
        ds.end_ts,
        ds.duration_s
        from registration_cohort r
        join phoenix.events_client e
        on e.user_id = r.user_id
        join phoenix.device_sessions ds
        on ds.idfv = e.idfv
        and e.event_time >= ds.start_ts
        and e.event_time <= ds.end_ts
        where e.user_id is not null
        and e.event_time >= date('{start_date}') and e.event_time <= (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')
        and ds.start_ts >= date('{start_date}') and ds.start_ts <= (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')
        and duration_s > 0
      ) x
      group by 1
    ),
    web_sessions as
    (
      select distinct
      r.user_id,
      sum(ew.duration_seconds) as total_duration_s_web,
      count(distinct ew.session) as total_sessions_web,
      count(distinct date(ew.start_time)) as web_days,
      count(distinct week(ew.start_time)||year(ew.start_time)) as web_weeks,
      count(distinct month(ew.start_time)||year(ew.start_time)) as web_months
      from registration_cohort r
      join ww.events_web_sessions ew
      on r.user_id = ew.user_id
      where ew.start_time >= date('{start_date}') and ew.start_time <= (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')
      and duration_seconds > 0
      group by 1
    ),
    data as(
      select r.user_id, 
      coalesce(total_duration_s_app, 0) as total_duration_s_app,
      total_sessions_app, app_days, app_weeks, app_months, 
      coalesce(total_duration_s_web, 0) as total_duration_s_web,
      total_sessions_web, web_days, web_weeks, web_months
      from registration_cohort r
      left join app_sessions a
      on r.user_id::int = a.user_id::int
      left join web_sessions b
      on r.user_id::int = b.user_id::int
    ),
    data_agg as(
      select user_id, 
      total_duration_s_app / (total_duration_s_app + total_duration_s_web + 0.000001) as pct_app,
      total_duration_s_web / (total_duration_s_app + total_duration_s_web + 0.000001) as pct_web
      from data
    )
    select *
    from data_agg
    """.format(start_date=start_date, n_days_activity=n_days_activity, n_days_churn=n_days_churn,
               n_min_days_since_install=n_min_days_since_install)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['user_id'])
    return df


def get_cef_preference(start_date, n_days_activity, n_days_churn, n_min_days_since_install, connection=None, **kwargs):
    """
    https:
    """

    query = """
    with users as (
      select user_id, createdate::date as create_date
      from ww.dim_users
    ),
    activity as (
      select user_id, min(start_time) as min_activity_event_day
      from ww.played_games as pg
      where start_time >= date('{start_date}') and start_time <= (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')
      group by user_id
    ),
    registration_cohort as (
      select u.user_id
      from users u
      join activity a
      using(user_id)
      where datediff('day', create_date, min_activity_event_day) >= '{n_min_days_since_install}'
    ),
    cef_by_platform as
    (
    select distinct
      user_id,
      case 
        when c.device is null then 'DESKTOP'
        when c.device is not null
        and (c.platform not in ('skill-app', 'skill-solrush') or c.platform is null) then 'MOWEB'
        when c.device is not null
        and c.platform = 'skill-app' then 'APP'
        when c.device is not null
        and c.platform = 'skill-solrush' then 'SOLRUSHAPP'
        else 'unknown'
        end as platform,
      it.trans_date,
      sum(it.amount) * -1 as cef
    from ww.internal_transactions it
    left join ww.clid_combos_ext c
        on it.client_id = c.client_id
    where it.trans_date >= date('{start_date}') and it.trans_date <= (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')
    and it.trans_type = 'SIGNUP'
    group by 1,2,3
    ),
    data as(
      select r.user_id, platform, count(trans_date) as entries, sum(cef) as cef
      from registration_cohort r
      left join cef_by_platform a
      on r.user_id::int = a.user_id::int
      group by 1, 2
    ),
    data_agg as(
      select user_id, platform, 
      coalesce((entries / sum(entries + .000001) OVER (PARTITION BY user_id))::NUMERIC(18,3), 0) as pct_entries,
      coalesce((cef / sum(cef + .000001) OVER (PARTITION BY user_id))::NUMERIC(18,3), 0) as pct_cef
      from data
    ),
    data_agg2 as(
        select user_id,
        MAX(case when platform = 'APP' then pct_cef else 0 end) as APP,
        MAX(case when platform = 'DESKTOP' then pct_cef else 0 end) as DESKTOP,
        MAX(case when platform = 'MOWEB' then pct_cef else 0 end) as MOWEB
        from data_agg
        group by user_id
    )
    select *
    from data_agg2
    """.format(start_date=start_date, n_days_activity=n_days_activity, n_days_churn=n_days_churn,
               n_min_days_since_install=n_min_days_since_install)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['user_id'])
    return df


def get_monthly_tournament_preference_df(start_date, n_days_activity, n_days_churn, n_min_days_since_install,
                                         connection=None, model_dct_all=None, **kwargs):
    monthly_tournament_preference_df = get_dataframe_from_query(f"""
    with a as(
        select distinct user_id
        from ww.played_games
        where start_time::date >= '{start_date}'
        and start_time::date <= (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')
    ),
    b as (
      select user_id, start_time
      from ww.played_games
      where start_time BETWEEN '{start_date}' and (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')
    ),
    c as (
      select pg.user_id, start_time, gametype_id, t.tournament_id as tournament_id, tt.ttemplate_id, t.fee as tournament_fee,
      coalesce(tt.victory_points, 0) as reward_points,
      coalesce(tt.purse, 0) as template_purse,
      coalesce(tt.fee, 0) as template_fee,
      coalesce(tt.external_fee, 0) as template_external_fee,
      coalesce(tt.max_players, 9999) as template_max_players,
      coalesce(tt.type, 9999) as template_type,
      coalesce(tt.games_min, 0) as template_games_min -- could be used as a delta for closeness to unlocking tournaments?  
      from (
        select user_id, tournament_id, gametype_id, start_time
        from ww.played_games as pg
        where start_time BETWEEN '{start_date}' and (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')      
      ) as pg
      left join(
        select tournament_id, ttemplate_id, fee
        from ww.tournaments
        where start_time BETWEEN '{start_date}' and (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')
      ) as t
      using(tournament_id)
      left join ww.dim_tournament_templates as tt
      using(ttemplate_id) 
    )
    SELECT 
    user_id,
    max_players,
    tournament_fee,
    template_purse,
    signups / sum(signups) OVER(partition by user_id) as pct_entries
    FROM (
        SELECT a.user_id, 
        case when template_max_players = 9999 and tournament_fee != 0 then 200 else template_max_players end as max_players,
        tournament_fee,
        template_purse,
        count(a.user_id) as signups
        from (select distinct b.user_id from a join b using(user_id) group by 1 having count(start_time) >= 50) a -- active players who played at least 50 games in the last month   
        left join c
        using(user_id)
        --where tournament_fee > 0 and template_purse > 0
        group by 1, 2, 3, 4
    ) X 
    ORDER BY max_players, tournament_fee, template_purse
    """, connection=connection)
    cluster_type = 'monthly_tournament_preference'
    model_dct = model_dct_all['model'][cluster_type]
    features = model_dct['features']
    transforms = model_dct['transforms']
    scalers = model_dct['scalers']

    p = monthly_tournament_preference_df.pivot_table(index='user_id',
                                                     columns=['max_players', 'tournament_fee', 'template_purse'],
                                                     values='pct_entries').fillna(0)
    _sort = p.columns.droplevel(0).droplevel(1)
    col_sort = np.argsort(_sort)
    p.columns = [f'm_fee_{y}__purse__{x}__max_players_{z}' for x, y, z in
                 zip(p.columns.droplevel(0).droplevel(0), p.columns.droplevel(0).droplevel(1),
                     p.columns.droplevel(2).droplevel(1))]

    #try:
    #    p[features]
    #except Exception as e:
    #    missing_columns = str(e)
    #    missing_columns = re.sub('\\n', ',', missing_columns)
    #    missing_columns = eval(missing_columns)
    #    missing_columns = re.sub(' not in index', '', missing_columns)
    #    missing_columns = eval(missing_columns)
    missing_columns = [f for f in features if f not in p]
    if len(missing_columns) > 0:
        temp = pd.DataFrame(np.zeros((p.shape[0], len(missing_columns))), columns=missing_columns)
        temp = temp.assign(user_id=p.index.values).set_index('user_id')
        p = pd.concat([
            p,
            temp
        ], axis=1)
    p = p[features]
    assert p.shape[1] == len(transforms), 'hmmm'

    X = scalers['standard_scaler'].transform(p)
    X = scalers['min_max_scaler'].transform(X)
    X = pd.DataFrame(X, columns=p.columns.values)
    X.loc[:, 'user_id'] = p.index.values
    X = X.set_index('user_id')
    return (X)


def get_lifetime_tournament_preference_df(start_date, n_days_activity, n_days_churn, n_min_days_since_install,
                                          connection=None, model_dct_all=None, **kwargs):
    lifetime_tournament_preference_df = get_dataframe_from_query(f"""
    with a as(
        select distinct user_id
        from ww.played_games
        where start_time::date >= '{start_date}'     
        and start_time::date <= (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days') 
    ),
    b as (
      select user_id, start_time
      from ww.played_games
      where start_time <= (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')      
    ),
    c as (
      select pg.user_id, start_time, gametype_id, t.tournament_id as tournament_id, tt.ttemplate_id, t.fee as tournament_fee,
      coalesce(tt.victory_points, 0) as reward_points,
      coalesce(tt.purse, 0) as template_purse,
      coalesce(tt.fee, 0) as template_fee,
      coalesce(tt.external_fee, 0) as template_external_fee,
      coalesce(tt.max_players, 9999) as template_max_players,
      coalesce(tt.type, 9999) as template_type,
      coalesce(tt.games_min, 0) as template_games_min -- could be used as a delta for closeness to unlocking tournaments?  
      from (
        select user_id, tournament_id, gametype_id, start_time
        from ww.played_games as pg
        where start_time <= (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')   
      ) as pg
      left join(
        select tournament_id, ttemplate_id, fee
        from ww.tournaments
        where start_time <= (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')   
      ) as t
      using(tournament_id)
      left join ww.dim_tournament_templates as tt
      using(ttemplate_id) 
    )
    SELECT 
    user_id,
    max_players,
    tournament_fee,
    template_purse,
    signups as entries
    FROM (
        SELECT a.user_id, 
        case when template_max_players = 9999 and tournament_fee != 0 then 200 else template_max_players end as max_players,
        tournament_fee,
        template_purse,
        count(a.user_id) as signups
        from (select distinct b.user_id from a join b using(user_id) group by 1 having count(start_time) >= 50) a -- active players who played at least 50 games in their lifetime
        left join c
        using(user_id)
        --where tournament_fee > 0 and template_purse > 0
        group by 1, 2, 3, 4
    ) X 
    ORDER BY max_players, tournament_fee, template_purse
    """, connection=connection)
    cluster_type = 'lifetime_tournament_preference'
    model_dct = model_dct_all['model'][cluster_type]
    features = model_dct['features']
    transforms = model_dct['transforms']
    scalers = model_dct['scalers']

    p = lifetime_tournament_preference_df.pivot_table(index='user_id',
                                                      columns=['max_players', 'tournament_fee', 'template_purse'],
                                                      values='entries').fillna(0)
    p.columns = [f'l_fee_{y}__purse__{x}__max_players_{z}' for x, y, z in
                 zip(p.columns.droplevel(0).droplevel(0), p.columns.droplevel(0).droplevel(1),
                     p.columns.droplevel(2).droplevel(1))]
    #try:
    #    p = p[features]
    #except Exception as e:
        #missing_columns = str(e)#
        #missing_columns = re.sub('\\n', ',', missing_columns)
        #missing_columns = eval(missing_columns)
        #missing_columns = re.sub(' not in index', '', missing_columns)
        #missing_columns = eval(missing_columns)
    missing_columns = [f for f in features if f not in p]
    if len(missing_columns) > 0:
        temp = pd.DataFrame(np.zeros((p.shape[0], len(missing_columns))), columns=missing_columns)
        temp = temp.assign(user_id=p.index.values).set_index('user_id')
        p = pd.concat([
            p,
            temp
        ], axis=1)
    p = p[features]
    assert p.shape[1] == len(transforms), 'hmmm'

    ## min max scale row-wise
    p.loc[:, :] = MinMaxScaler().fit_transform(p.T).T

    X = scalers['standard_scaler'].transform(p)
    X = scalers['min_max_scaler'].transform(X)
    X = pd.DataFrame(X, columns=p.columns.values)
    X.loc[:, 'user_id'] = p.index.values
    X = X.set_index('user_id')
    return (X)


def get_lifetime_time_of_day_preference_df(start_date, n_days_activity, n_days_churn, n_min_days_since_install,
                                           connection=None, model_dct_all=None, **kwargs):
    ## Time of Day Preference
    lifetime_time_of_day_preference_df = get_dataframe_from_query(f"""
    with a as(
        select distinct user_id
        from ww.played_games
        where start_time::date >= '{start_date}'
        and start_time::date <= (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days') 
    ),
    c as(
        select user_id, start_time, hour(start_time) as hour_of_day
        from ww.played_games
        where start_time::date <= (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days') 
    )
    SELECT user_id, hour_of_day, entries / sum(entries) OVER (PARTITION BY user_id) as pct_entries
    FROM (
       select a.user_id, hour_of_day, count(a.user_id) as entries
        from (select distinct c.user_id from a join c using(user_id) group by 1 having count(start_time) >= 50) a -- active players who played at least 50 games in their lifetime
        left join c
        using(user_id)
        group by a.user_id, hour_of_day
    ) X
    """, connection=connection)
    lifetime_time_of_day_preference_df = lifetime_time_of_day_preference_df.pivot_table(index='user_id',
                                                                                        columns=['hour_of_day'],
                                                                                        values='pct_entries').fillna(0)
    lifetime_time_of_day_preference_df = lifetime_time_of_day_preference_df.add_prefix('hour_')

    cluster_type = 'lifetime_time_of_day_preference'
    model_dct = model_dct_all['model'][cluster_type]
    scalers = model_dct['scalers']

    filename = 'lifetime_time_of_day_preference'
    X = scalers['standard_scaler'].transform(lifetime_time_of_day_preference_df)
    X = scalers['min_max_scaler'].transform(X)
    X = pd.DataFrame(X, columns=[f"hour_{i}" for i in range(24)])
    X['user_id'] = lifetime_time_of_day_preference_df.index.values
    X = X.set_index('user_id')
    X = X.add_prefix('l_')
    return (X)


def get_monthly_time_of_day_preference_df(start_date, n_days_activity, n_days_churn, n_min_days_since_install,
                                          connection=None, model_dct_all=None, **kwargs):
    monthly_time_of_day_preference_df = get_dataframe_from_query(f"""
    with a as(
        select distinct user_id
        from ww.played_games
        where start_time::date >= '{start_date}'
        and start_time::date <= (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days') 
    ),
    c as(
        select user_id, start_time, hour(start_time) as hour_of_day
        from ww.played_games
        where start_time::date >= '{start_date}'
        and start_time::date <= (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days') 
    )
    SELECT user_id, hour_of_day, entries / sum(entries) OVER (PARTITION BY user_id) as pct_entries
    FROM (
       select a.user_id, hour_of_day, count(a.user_id) as entries
        from (select distinct c.user_id from a join c using(user_id) group by 1 having count(start_time) >= 50) a -- active players who played at least 50 games this month
        left join c
        using(user_id)
        group by a.user_id, hour_of_day
    ) X
    """, connection=connection)
    monthly_time_of_day_preference_df = monthly_time_of_day_preference_df.pivot_table(index='user_id',
                                                                                      columns=['hour_of_day'],
                                                                                      values='pct_entries').fillna(0)
    monthly_time_of_day_preference_df = monthly_time_of_day_preference_df.add_prefix('hour_')

    cluster_type = 'monthly_time_of_day_preference'
    model_dct = model_dct_all['model'][cluster_type]
    scalers = model_dct['scalers']

    X = scalers['standard_scaler'].transform(monthly_time_of_day_preference_df)
    X = scalers['min_max_scaler'].transform(X)
    X = pd.DataFrame(X, columns=[f"hour_{i}" for i in range(24)])
    X['user_id'] = monthly_time_of_day_preference_df.index.values
    X = X.set_index('user_id')
    X = X.add_prefix('m_')
    return (X)
