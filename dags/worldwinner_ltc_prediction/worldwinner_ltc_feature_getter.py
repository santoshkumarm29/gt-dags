from datetime import date, datetime, timedelta
import pandas as pd
import numpy as np
from pandas.api.types import CategoricalDtype

from common.db_utils import get_dataframe_from_query
from common.operators.ml_ltc_operator import MLLTCOperator #copied


def update_df_index(df, start_date=None, n_days_activity=None, n_days_churn=None, n_min_days_since_install=None):
    """Include 'n_days_observation' and 'n_days_horizon' in the index to avoid later accounting mistakes.

    FIXME: Use ltv_utils.update_dataframe_index_with_days_observation_days_horizon

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


def get_features_df(ds, conn, model_s3_filepaths):
    predict_date = MLLTCOperator.get_predict_date_from_ds(ds)
    # determine the parameters to pass to the feature query getters based on the standard filename
    model_s3_path = MLLTCOperator.get_model_s3_path(
        max_date=predict_date,
        date_column_name='predict_date',
        model_s3_filepaths=model_s3_filepaths,
    )
    model_info = MLLTCOperator.get_model_info_from_filename(model_s3_path)
    n_days_churn = model_info['n_days_churn']
    n_days_activity = model_info['n_days_activity']
    n_min_days_since_install = model_info['n_min_days_since_install']

    start_date = predict_date - timedelta(days=n_days_activity)
    df = _get_features_df(
        start_date=start_date,
        n_days_activity=n_days_activity,
        n_days_churn=n_days_churn,
        n_min_days_since_install=n_min_days_since_install,
        connection=conn
    )
    df = update_df_index(
        df,
        start_date=start_date,
        n_days_activity=n_days_activity,
        n_days_churn=n_days_churn,
        n_min_days_since_install=n_min_days_since_install
    ).reset_index()
    return df


def _get_features_df(start_date, n_days_activity, n_days_churn, n_min_days_since_install, connection=None,
                    verbose=False):
    """
    """

    ## Get Feature Devices
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
        get_games_played
    ]
    for feature_callable in feature_callables:
        temp = feature_callable(
            start_date=start_date,
            n_days_activity=n_days_activity,
            n_days_churn=n_days_churn,
            n_min_days_since_install=n_min_days_since_install,
            connection=connection
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
    return df


def get_user_id_base(start_date, n_days_activity, n_days_churn, n_min_days_since_install, connection=None):
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
        select /*+ LABEL ('airflow-ml-worldwinner_ltc_prediction_v1_0-worldwinner_ltc_feature_getter')*/ *
        from veterans
    """.format(start_date=start_date, n_days_activity=n_days_activity, n_days_churn=n_days_churn,
               n_min_days_since_install=n_min_days_since_install)
    df = get_dataframe_from_query(query, connection=connection).set_index('user_id')

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


def get_target(start_date, n_days_activity, n_days_churn, n_min_days_since_install, connection=None):
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
        select /*+ LABEL ('airflow-ml-worldwinner_ltc_prediction_v1_0-worldwinner_ltc_feature_getter')*/
        veterans.user_id, case when min_churn_event_day IS NULL then 1 else 0 end as churn
        from veterans
        left join churn
        using(user_id)
    """.format(start_date=start_date, n_days_activity=n_days_activity, n_days_churn=n_days_churn,
               n_min_days_since_install=n_min_days_since_install)
    df = get_dataframe_from_query(query, connection=connection).set_index('user_id')
    return df


def get_activity_games_info(start_date, n_days_activity, n_days_churn, n_min_days_since_install, connection=None):
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
        select /*+ LABEL ('airflow-ml-worldwinner_ltc_prediction_v1_0-worldwinner_ltc_feature_getter')*/ user_id,
        count(distinct ttemplate_id) as num_distinct_tournament_types,
        count(distinct tournament_id) as num_distinct_tournaments_entered,
        count(start_time) as num_all_tournament_entries,
        SUM(case when tournament_fee = 0 then 1 else 0 end) as num_free_entries,
        SUM(case when tournament_fee > 0 then 1 else 0 end) as num_paid_entries
        from raw_games_info
        group by user_id
    """.format(start_date=start_date, n_days_activity=n_days_activity, n_days_churn=n_days_churn,
               n_min_days_since_install=n_min_days_since_install)
    df = get_dataframe_from_query(query, connection=connection).set_index('user_id')
    return df


def get_past_90days_games_info(start_date, n_days_activity, n_days_churn, n_min_days_since_install, connection=None):
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
        select  /*+ LABEL ('airflow-ml-worldwinner_ltc_prediction_v1_0-worldwinner_ltc_feature_getter')*/ user_id,
        count(distinct ttemplate_id) as past_num_distinct_tournament_types,
        count(distinct tournament_id) as past_num_distinct_tournaments_entered,
        count(start_time) as past_num_all_tournament_entries,
        SUM(case when tournament_fee = 0 then 1 else 0 end) as past_num_free_entries,
        SUM(case when tournament_fee > 0 then 1 else 0 end) as past_num_paid_entries
        from raw_games_info
        group by user_id
    """.format(start_date=start_date, n_days_activity=n_days_activity, n_days_churn=n_days_churn,
               n_min_days_since_install=n_min_days_since_install)
    df = get_dataframe_from_query(query, connection=connection).set_index('user_id')
    return df


def get_rfm_deposits_withdrawals(start_date, n_days_activity, n_days_churn, n_min_days_since_install, connection=None):
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
          select  /*+ LABEL ('airflow-ml-worldwinner_ltc_prediction_v1_0-worldwinner_ltc_feature_getter')*/ veterans.user_id,
          withdrawals_recency, withdrawals_frequency, withdrawals_amount,
          deposits_recency, deposits_frequency, deposits_amount
          from veterans
          left join rfm_d
          using(user_id)
          left join rfm_w
          using(user_id)
    """.format(start_date=start_date, n_days_activity=n_days_activity, n_days_churn=n_days_churn,
               n_min_days_since_install=n_min_days_since_install)
    df = get_dataframe_from_query(query, connection=connection).set_index('user_id')
    df = df.fillna(0)
    return df


def get_session_metrics(start_date, n_days_activity, n_days_churn, n_min_days_since_install, connection=None):
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
        select  /*+ LABEL ('airflow-ml-worldwinner_ltc_prediction_v1_0-worldwinner_ltc_feature_getter')*/ *
        from veterans
        left join web_sessions
        using(user_id)
    """.format(start_date=start_date, n_days_activity=n_days_activity, n_days_churn=n_days_churn,
               n_min_days_since_install=n_min_days_since_install)
    df = get_dataframe_from_query(query, connection=connection).set_index('user_id')
    return df


def get_last_balance(start_date, n_days_activity, n_days_churn, n_min_days_since_install, connection=None):
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
        select  /*+ LABEL ('airflow-ml-worldwinner_ltc_prediction_v1_0-worldwinner_ltc_feature_getter')*/ veterans.user_id, last_playable_balance, last_real_amount_balance--, last_play_amount_balance
        from veterans
        left join ranked_trans
        using(user_id)
        where rn = 1
    """.format(start_date=start_date, n_days_activity=n_days_activity, n_days_churn=n_days_churn,
               n_min_days_since_install=n_min_days_since_install)
    df = get_dataframe_from_query(query, connection=connection).set_index('user_id')
    return df


def get_mobile_player(start_date, n_days_activity, n_days_churn, n_min_days_since_install, connection=None):
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
      where ww_platform = 'app' and event_day <= (date('{start_date}') + interval '{n_days_activity} days' - interval '1 days')
      and event_day >= (date('{start_date}') - interval '90 days')
      group by user_id
    )
        select  /*+ LABEL ('airflow-ml-worldwinner_ltc_prediction_v1_0-worldwinner_ltc_feature_getter')*/ veterans.user_id,
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
    df = get_dataframe_from_query(query, connection=connection).set_index('user_id')
    return df


def get_past_churn_metrics(start_date, n_days_activity, n_days_churn, n_min_days_since_install, connection=None):
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
        select  /*+ LABEL ('airflow-ml-worldwinner_ltc_prediction_v1_0-worldwinner_ltc_feature_getter')*/ veterans.user_id,
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
    df = get_dataframe_from_query(query, connection=connection).set_index('user_id')
    return df


def get_games_played(start_date, n_days_activity, n_days_churn, n_min_days_since_install, connection=None):
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

        select  /*+ LABEL ('airflow-ml-worldwinner_ltc_prediction_v1_0-worldwinner_ltc_feature_getter')*/ veterans.user_id, category, games_played.gametype_id, count(*) as games_played
        from veterans
        left join games_played -- 931 Million Rows Here
        using(user_id)
        left join game_types -- Supplementary Game Info, no problem
        using(gametype_id)
        group by veterans.user_id, category, games_played.gametype_id
    """.format(start_date=start_date, n_days_activity=n_days_activity, n_days_churn=n_days_churn,
               n_min_days_since_install=n_min_days_since_install)
    df = get_dataframe_from_query(query, connection=connection).set_index('user_id')

    # number of games played, by category
    gp_cat = df.pivot_table(index='user_id', columns=['category'], values='games_played', aggfunc=np.sum)
    # number of games played, by gametype_id
    gp_game = df.pivot_table(index='user_id', columns=['gametype_id'], values='games_played', aggfunc=np.sum)

    # Game Category Distribution
    df = gp_cat.apply(lambda x: (x / np.sum(x.fillna(0))), 1)
    # Favorite Game
    df['favorite_game'] = gp_game.idxmax(1)
    # Favorite Category
    df['favorite_gametype'] = gp_cat.idxmax(1)
    # Favorite Game_ID Dedication
    df['favorite_game_dedication'] = gp_game.apply(lambda x: (x / np.sum(x.fillna(0))).max(), 1)
    # Favorite Category Dedication
    df['favorite_gametype_dedication'] = gp_cat.apply(lambda x: (x / np.sum(x.fillna(0))).max(), 1)

    categoricals = {
        'favorite_gametype': ['Arcade', 'Card', 'Game Show', 'Sports', 'Strategy', 'Word'],
    }
    for column_name, categories in categoricals.items():
        category = CategoricalDtype(categories=categories, ordered=True)
        df[column_name] = df[column_name].astype(category)
        df[column_name] = df[column_name].cat.codes
    df = df.fillna(0)

    return df
