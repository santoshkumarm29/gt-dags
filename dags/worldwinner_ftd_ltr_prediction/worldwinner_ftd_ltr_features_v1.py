"""Feature queries for WorldWinner LTR based on First Time Deposit Date
"""

from datetime import date, datetime, timedelta
import numpy as np
import pandas as pd
from pandas.api.types import CategoricalDtype

from common.date_utils import convert_to_date
from common.db_utils import get_dataframe_from_query

def update_df_index(df, n_days_observation, n_days_horizon, index_column='user_id'):
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


def get_features_df(install_start_date, install_end_date, n_days_observation, n_days_horizon, connection=None,
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

    ## Get User ID Base
    print(get_user_id_base.__name__)
    df = get_user_id_base(
        install_start_date,
        install_end_date,
        connection=connection,
        categorical_encode=categorical_encode,
        onehot_encode=False,
    )
    print('User install size: {}'.format(df.shape))
    feature_callables = [
        get_iap_revenue,
        get_days_played_pg,
        get_days_played_it,
        get_sin_cos_install_time,
        # get_total_winnings,
        get_free_paid_games,
        get_entry_sizes,
        get_favorite_game,
        get_total_net_winnings,
        get_total_net_withdrawals,
        get_first_last_balance,
        get_is_banned,
        get_user_info,
        get_games_played_by_id,
        get_time_features,
        get_mobile_play,
        get_session_info,
        get_days_to_ftd
    ]

    for feature_callable in feature_callables:
        # print(feature_callable.__name__)
        temp = feature_callable(
            install_start_date,
            install_end_date,
            n_days=n_days_observation,
            connection=connection,
        )
        # print('feature query shape:{}'.format(temp.shape))
        df = pd.merge(
            left=df,
            right=temp,
            how='left',
            left_index=True,
            right_index=True,
        )
        # print('training_df shape after merge:{}'.format(df.shape))

    ## Any feature that should not fillna with zeros is responsible for returning rows for all installs!
    df = df.fillna(0)

    ## TODO: Use Manually Selected Features ONLY
    return df


def get_user_id_base(install_start_date, install_end_date, onehot_encode=False, connection=None,
                     categorical_encode=True):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=b3d08aca-f46e-47de-b2e3-4a0c3e30cbe8
    """
    query = """
    /* Get install cohort for WW LTV */
    with users as(
      select user_id, real_money_date
      from ww.dim_users
      where real_money_date::date between '{install_start_date}' and '{install_end_date}'
    )
        select user_id
        from users
    """.format(install_start_date=install_start_date, install_end_date=install_end_date)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['user_id'])
    return df


def get_install_date_df(install_start_date, install_end_date, connection=None):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=89ff6a89-a033-4538-8a06-60828b5d15f2
    """
    query = """
    /* Get install date by user_id for WW LTV */
    with users as(
      select user_id, real_money_date as install_date
      from ww.dim_users
      where real_money_date::date  between '{install_start_date}' and '{install_end_date}'
    )
        select user_id, install_date
        from users
    """.format(install_start_date=install_start_date, install_end_date=install_end_date)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['user_id'])
    return df


def get_iap_revenue(install_start_date, install_end_date, n_days=1, connection=None):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=360839fd-92bc-427a-8d13-f1fcbb16e356
    """
    query = """
    /* Get CEF as a proxy for LTV */
    with users as(
      select user_id, real_money_date
      from ww.dim_users
      where real_money_date::date  between '{install_start_date}' and '{install_end_date}'
    ),
    games_played as (
      select user_id, trans_date::date as trans_date, amount
      from ww.internal_transactions
      where trans_date::date between '{install_start_date}' and (date('{install_end_date}') + interval '{n_days} days')
      and trans_type = 'SIGNUP'
    ),
    raw_games_info as(
        select users.user_id, amount
        from users
        left join games_played
        on users.user_id = games_played.user_id
        and datediff('day', real_money_date::date , trans_date::date ) between 0 and ({n_days}-1)
    )
    select user_id, coalesce(SUM(amount), 0)*(-1) as iap_revenue
    from raw_games_info
    group by user_id
    """.format(install_start_date=install_start_date, install_end_date=install_end_date, n_days=n_days)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['user_id'])
    return df


def get_retention_df(install_start_date, install_end_date, n_days_observation=1, n_days_horizon=30, connection=None):
    """
    Create a dataframe with the target only for installs between `install_start_date` and
    `install_end_date` inclusive.  Target is the binary retention event in the <n_days> retention window

    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=0924cba7-40a1-42f4-994b-29a10b267cdc
    """
    query = """
    /* Get install cohort for WW LTV */
    with users as(
      select user_id, real_money_date
      from ww.dim_users
      where real_money_date::date  between '{install_start_date}' and '{install_end_date}'
    ),
    days_played_retention as (
      select distinct X.user_id, X.trans_date
      from (
        select user_id, trans_date
        from ww.internal_transactions it
        where trans_date::date  between '{install_start_date}' and (date('{install_end_date}') + interval '{n_days_horizon} days')
        and trans_type = 'SIGNUP'
      ) X
    )
        select a.user_id, MAX(case when b.trans_date is not null then 1 else 0 end) as target
        from users a 
        left join days_played_retention b
        on a.user_id = b.user_id
        and datediff('day', a.real_money_date::date , b.trans_date::date) between {n_days_observation} and ({n_days_horizon}-1)
        group by 1
    """.format(install_start_date=install_start_date, install_end_date=install_end_date,
               n_days_observation=n_days_observation, n_days_horizon=n_days_horizon)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['user_id'])
    return df


def get_days_played_pg(install_start_date, install_end_date, n_days=1, connection=None):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=e7bf6320-8a44-4c51-8c33-4b857dd6e6cb
    """
    query = """
    /* Worldwinner Days Played */
    with users as(
      select user_id, real_money_date
      from ww.dim_users
      where real_money_date::date  between '{install_start_date}' and '{install_end_date}'
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
    days_played_info as(
        select users.user_id, users.real_money_date, trans_date
        from users
        left join days_played
        on users.user_id = days_played.user_id
        and datediff('day', real_money_date::date , trans_date::date ) between 0 and ({n_days}-1)
    )
    select user_id, 
    count(trans_date) as pg_days_played,
    coalesce(datediff('day', real_money_date::date , min(trans_date::date)), -1) + 1 as pg_first_played,
    coalesce(datediff('day', real_money_date::date , max(trans_date::date)), -1) + 1 as pg_last_played
    from days_played_info
    group by user_id, real_money_date
    """.format(install_start_date=install_start_date, install_end_date=install_end_date, n_days=n_days)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['user_id'])
    return df


def get_days_played_it(install_start_date, install_end_date, n_days=1, connection=None):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=e7bf6320-8a44-4c51-8c33-4b857dd6e6cb
    """
    query = """
    /* Worldwinner Days Played */
    with users as(
        select user_id, real_money_date
        from ww.dim_users
        where real_money_date::date  between '{install_start_date}' and '{install_end_date}'
    ),
    days_played as (
        select distinct user_id, trans_date
        from ww.internal_transactions
        where trans_date between '{install_start_date}' and (date('{install_end_date}') + interval '{n_days} days')
        and trans_type = 'SIGNUP'
    ),
    days_played_info as(
        select users.user_id, users.real_money_date, trans_date
        from users
        left join days_played
        on users.user_id = days_played.user_id
        and datediff('day', real_money_date::date , trans_date::date) between 0 and ({n_days}-1)
    )
    select user_id, 
    count(trans_date) as it_days_played,
    coalesce(datediff('day', real_money_date::date , min(trans_date::date)), -1) + 1 as it_first_played,
    coalesce(datediff('day', real_money_date::date , max(trans_date::date)), -1) + 1 as it_last_played
    from days_played_info
    group by user_id, real_money_date
    """.format(install_start_date=install_start_date, install_end_date=install_end_date, n_days=n_days)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['user_id'])
    return df


def get_sin_cos_install_time(install_start_date, install_end_date, n_days=1, connection=None):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=a28ac585-f91b-42cf-8512-9e7604c28575
    """
    query = """
    /* Get the cyclical transformation of install time of day */
    with users as(
      select user_id, real_money_date
      from ww.dim_users
      where real_money_date::date between '{install_start_date}' and '{install_end_date}'
    ),
    time_of_install_subquery as (
      select user_id, real_money_date, MOD((DATE_PART('SECOND', real_money_date) + DATE_PART('MINUTE', real_money_date) * (60) + DATE_PART('HOUR', real_money_date) * (60*60)), 60*60*24) as seconds_into_day
      from users
    )
    select users.user_id,
    COS(2*PI()*seconds_into_day / (60*60*24)) as cos_installed_at,
    SIN(2*PI()*seconds_into_day / (60*60*24)) as sin_installed_at
    FROM users
    LEFT JOIN time_of_install_subquery
    USING(user_id)
    """.format(install_start_date=install_start_date, install_end_date=install_end_date, n_days=n_days)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['user_id'])
    return df


def get_total_winnings(install_start_date, install_end_date, n_days=1, connection=None):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=58de75f5-4232-466e-a0e4-17d6ae0b71bd
    """
    query = """
    /* Get total winnings */
    with users as(
      select user_id, real_money_date
      from ww.dim_users
      where real_money_date::date between '{install_start_date}' and '{install_end_date}'
    ),
    game_winnings as (
      select user_id, trans_date::date as trans_date, amount
      from ww.internal_transactions
      where trans_date::date between '{install_start_date}' and (date('{install_end_date}') + interval '{n_days} days')
      and trans_type = 'WINNINGS'
    ),
    raw_winnings_info as(
        select users.user_id, amount
        from users
        left join game_winnings
        on users.user_id = game_winnings.user_id
        and datediff('day', real_money_date::date, trans_date::date) between 0 and ({n_days}-1)
    )
      select user_id, coalesce(SUM(amount), 0) as total_winnings
      from raw_winnings_info
      group by user_id
    """.format(install_start_date=install_start_date, install_end_date=install_end_date, n_days=n_days)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['user_id'])
    return df


def get_free_paid_games(install_start_date, install_end_date, n_days=1, connection=None):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=1f71bff8-05fe-45f0-a163-63a542698b3b
    """
    query = """
    /* Gets the number of free and paid games signed up for, and their fees */
    with users as(
      select user_id, real_money_date
      from ww.dim_users
      where real_money_date::date between '{install_start_date}' and '{install_end_date}'
    ),
    games_played as (
      select pg.user_id, pg.start_time::date as trans_date, fee, purse
      from (
        select user_id, tournament_id, gametype_id, start_time
        from ww.played_games as pg
        where start_time::date between '{install_start_date}' and (date('{install_end_date}') + interval '{n_days} days')
      ) as pg
      /* left join tournament info */
      left join(
        select tournament_id, ttemplate_id, fee, purse
        from ww.tournaments
        ) as t
      using(tournament_id)
    ),
    games_played_info as(
        select users.user_id, fee, purse
        from users
        left join games_played
        on users.user_id = games_played.user_id
        and datediff('day', real_money_date::date, trans_date::date) between 0 and ({n_days}-1)
    )
      select user_id, 
      SUM(case when fee = 0 and purse = 0 then 1 else 0 end) as no_payout_games_played,
      SUM(case when fee = 0 and purse > 0 then 1 else 0 end) as freeplays_played,
      SUM(case when fee > 0 and purse > 0 then 1 else 0 end) as paid_games_played
      from games_played_info
      group by user_id
    """.format(install_start_date=install_start_date, install_end_date=install_end_date, n_days=n_days)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['user_id'])
    return df


def get_favorite_game(install_start_date, install_end_date, n_days=1, connection=None):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=91b26894-a273-4726-adaf-5149d3d8d2d3
    """
    query = """
    /* Gets the most played gametype_id */
    with users as(
      select user_id, real_money_date
      from ww.dim_users
      where real_money_date::date between '{install_start_date}' and '{install_end_date}'
    ),
    games_played as (
      select pg.user_id, pg.start_time::date as trans_date, gametype_id
      from (
        select user_id, tournament_id, gametype_id, start_time
        from ww.played_games as pg
        where start_time::date between '{install_start_date}' and (date('{install_end_date}') + interval '{n_days} days')
      ) as pg
      /* left join tournament info */
      left join(
        select tournament_id, ttemplate_id
        from ww.tournaments
        ) as t
      using(tournament_id)
    ),
    games_played_info as(
        select users.user_id, gametype_id
        from users
        left join games_played
        on users.user_id = games_played.user_id
        and datediff('day', real_money_date::date, trans_date::date) between 0 and ({n_days}-1)
    )

        select distinct user_id, first_value(coalesce(gametype_id, 0)) OVER(PARTITION by user_id ORDER BY rank ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as favorite_game_id FROM(
        select user_id, gametype_id, RANK() OVER (PARTITION BY user_id ORDER BY game_count DESC) AS rank FROM(
        select user_id, gametype_id, count(*) as game_count
        from games_played_info
        group by user_id, gametype_id
      ) X WHERE gametype_id IS NOT NULL) Y
          WHERE rank = 1  
    """.format(install_start_date=install_start_date, install_end_date=install_end_date, n_days=n_days)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['user_id'])
    return df


def get_total_net_winnings(install_start_date, install_end_date, n_days=1, connection=None):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=fd7899fd-3419-4d8d-a2c4-b9e829b0b421
    """
    query = """
    /* Get total net winnings */
    with users as(
      select user_id, real_money_date
      from ww.dim_users
      where real_money_date::date between '{install_start_date}' and '{install_end_date}'
    ),
    game_transactions as (
      select user_id, trans_date::date as trans_date, amount, trans_type
      from ww.internal_transactions
      where trans_date::date between '{install_start_date}' and (date('{install_end_date}') + interval '{n_days} days')
      and trans_type in ('WINNINGS', 'SIGNUP')
    ),
    raw_transactions_info as(
        select users.user_id, amount, trans_type
        from users
        left join game_transactions
        on users.user_id = game_transactions.user_id
        and datediff('day', real_money_date::date, trans_date::date) between 0 and ({n_days}-1)
    )
      select user_id, 
      coalesce(SUM(case when trans_type = 'WINNINGS' then amount else NULL end), 0) as total_winnings,
      coalesce(SUM(case when trans_type = 'SIGNUP' then amount else NULL end), 0) as total_signups,
      coalesce(SUM(case when trans_type = 'WINNINGS' then amount else NULL end), 0) + coalesce(SUM(case when trans_type = 'SIGNUP' then amount else NULL end), 0) as total_net_winnings
      from raw_transactions_info
      group by user_id
    """.format(install_start_date=install_start_date, install_end_date=install_end_date, n_days=n_days)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['user_id'])
    return df


def get_total_net_withdrawals(install_start_date, install_end_date, n_days=1, connection=None):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=bda40708-5dff-4f88-9c08-a4baccfbb5d0
    """
    query = """
    /* Get total net withdrawls + deposit and withdrawal counts */
    with users as(
      select user_id, real_money_date
      from ww.dim_users
      where real_money_date::date between '{install_start_date}' and '{install_end_date}'
    ),
    game_transactions as (
      select user_id, trans_date::date as trans_date, amount, trans_type
      from ww.internal_transactions
      where trans_date::date between '{install_start_date}' and (date('{install_end_date}') + interval '{n_days} days')
      and trans_type in ('DEPOSIT', 'DEPOSITM', 'DEPOSITPP', 'DEPOSIT#1', 'DEPOSITCK', 'DEPOSIT#1M', 'WITHDRAWAL', 'WITHDRAWM')
    ),
    raw_transactions_info as(
        select users.user_id, amount, trans_type
        from users
        left join game_transactions
        on users.user_id = game_transactions.user_id
        and datediff('day', real_money_date::date, trans_date::date) between 0 and ({n_days}-1)
    )
      select user_id, 
      coalesce(SUM(case when trans_type in ('WITHDRAWAL', 'WITHDRAWM') then 1 else NULL end), 0) as count_withdrawals, 
      coalesce(SUM(case when trans_type in ('DEPOSIT', 'DEPOSITM', 'DEPOSITPP', 'DEPOSIT#1', 'DEPOSITCK', 'DEPOSIT#1M') 
                   then 1 else NULL end), 0) as count_deposits,
      coalesce(SUM(case when trans_type in ('WITHDRAWAL', 'WITHDRAWM') then -amount else NULL end), 0) + 
      coalesce(SUM(case when trans_type in ('DEPOSIT', 'DEPOSITM', 'DEPOSITPP', 'DEPOSIT#1', 'DEPOSITCK', 'DEPOSIT#1M') 
                   then -amount else NULL end), 0) as total_net_withdrawals
      from raw_transactions_info
      group by user_id
    """.format(install_start_date=install_start_date, install_end_date=install_end_date, n_days=n_days)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['user_id'])
    return df


def get_first_last_balance(install_start_date, install_end_date, n_days=1, connection=None):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=6cc0620a-2c4b-4078-af7f-80c7debb723b
    """
    query = """
    /* Gets the first and last total playable balances by user_id */
    with users as(
      select user_id, real_money_date
      from ww.dim_users
      where real_money_date::date between '{install_start_date}' and '{install_end_date}'
    ),
    games_played as (
      select user_id, trans_date as trans_time, trans_date::date as trans_date, trans_type, balance, play_money,
      deposit_credits, deposit_credits_amount
      from ww.internal_transactions
      where trans_date::date between '{install_start_date}' and (date('{install_end_date}') + interval '{n_days} days')
    ),
    raw_games_info as(
        select users.user_id, trans_time, trans_type, trans_date, balance, play_money,
        deposit_credits, deposit_credits_amount
        from users
        left join games_played
        on users.user_id = games_played.user_id
        and datediff('day', real_money_date::date, trans_date::date::date) between 0 and ({n_days}-1)
    ),
    last_balance_subquery as (
      SELECT distinct user_id, last_balance, last_balance_bonus_bucks, last_play_money
      FROM(
        SELECT user_id,
        last_value(play_money) OVER(PARTITION by user_id ORDER BY trans_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_play_money,
        last_value(balance) OVER(PARTITION by user_id ORDER BY trans_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_balance,
        last_value(deposit_credits_amount) OVER(PARTITION by user_id ORDER BY trans_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_balance_bonus_bucks
        FROM raw_games_info
        where trans_time IS NOT NULL
      ) X
    ),
    first_balance_subquery as (
      SELECT user_id,
      SUM(case when trans_type ilike '%deposit%' then balance else 0 end) + SUM(case when trans_type ilike '%deposit%' then play_money  else 0 end) as first_balance,
      SUM(case when trans_type = '1stTimeCR' then deposit_credits else 0 end) as first_balance_bonus_bucks
      FROM raw_games_info
      where trans_time IS NOT NULL
      group by user_id
    )
        select * 
        from first_balance_subquery
        left join last_balance_subquery
        using(user_id)
    """.format(install_start_date=install_start_date, install_end_date=install_end_date, n_days=n_days)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['user_id'])
    return df


def get_is_banned(install_start_date, install_end_date, n_days=1, connection=None):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=84f6528d-f387-48c9-bd17-84e4c73dbc4b
    """
    query = """
    with banned_df as (
      select user_id,
      case when user_banned_date is NULL then 0
      when datediff('day', real_money_date::date, user_banned_date::date) between 0 and ({n_days}-1) then 1
      else 0 end as is_banned
      from ww.dim_users
      where real_money_date::date between '{install_start_date}' and '{install_end_date}'
    )
    select *
    from banned_df
    """.format(install_start_date=install_start_date, install_end_date=install_end_date, n_days=n_days)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['user_id'])
    return df


def get_email_info(install_start_date, install_end_date, n_days=1, connection=None, categorical_encode=True):
    """
    https://bi.worldwinner.com/webreports/sqlide/ide#dynamic_param=true&guid=0ebdad63-11a7-48e3-a584-76fd37988abe
    """
    query = """
    /* Gets information from the users email address */
    with users as(
      select user_id, real_money_date
      from ww.dim_users
      where real_money_date::date between '{install_start_date}' and '{install_end_date}'
    ),
    email_df as (
      select user_id, email, SPLIT_PART(email, '@', 1) as email_username, SPLIT_PART(email, '@', 2) as email_domain
      from ww.dim_users
      where real_money_date::date between '{install_start_date}' and '{install_end_date}'
    ),
    student_df as (
      select user_id, (email_domain like '%.edu%')::INT as is_student
      from email_df
    ),
    syntax_df as (
      select user_id, 
      REGEXP_COUNT(email_domain, '\.') as num_dots_in_email_domain,
      LENGTH(email_username) as num_char_email_username,
      (REGEXP_COUNT(email_username, '[0-9]') > 0)::INT as numbers_in_email_username,
      REGEXP_SUBSTR(email_domain, '^.+\.') as email_domain_company,
      REGEXP_SUBSTR(email_domain, '\..+$') as email_domain_ending
      from email_df
    )

      select users.user_id, email_domain_company, email_domain_ending, is_student, num_char_email_username, numbers_in_email_username
      from users
      left join email_df
      using(user_id)
      left join student_df
      using(user_id)
      left join syntax_df
      using(user_id)
    """.format(install_start_date=install_start_date, install_end_date=install_end_date, n_days=n_days)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['user_id'])

    if categorical_encode == True:
        # convert categoricals to integers
        categoricals = {
            'email_domain_company': ['aol.', 'comcast.', 'gmail.', 'hotmail.', 'hotmail.co.',
                                     'icloud.', 'live.', 'msn.', 'outlook.', 'yahoo.'
                                     ],
            'email_domain_ending': ['.ca', '.co.in', '.co.uk', '.com', '.edu',
                                    '.fr', '.in', '.net', '.org', '.rr.com', 'other'
                                    ]
        }
        for column_name, categories in categoricals.items():
            category = CategoricalDtype(categories=categories, ordered=True)
            df[column_name] = df[column_name].astype(category)
            df[column_name] = df[column_name].cat.codes
    return df


def get_time_features(install_start_date, install_end_date, n_days=1, connection=None):
    """
    https:
    """
    # query = """
    # /* Get CEF as a proxy for LTV */
    # with users as (
    #  select user_id, createdate::date as create_date
    #  from ww.dim_users
    #  where createdate::date between '{install_start_date}' and '{install_end_date}'
    # )
    # select user_id, create_date
    # from users
    # """.format(install_start_date=install_start_date, install_end_date=install_end_date, n_days=n_days)
    # installs_df = get_dataframe_from_query(query, connection=connection)
    #
    # installs_df['year'] = [x.year for x in installs_df.create_date]
    # installs_df['month'] = [x.month for x in installs_df.create_date]
    # installs_df['day'] = [x.day for x in installs_df.create_date]
    # installs_df['isoweekday'] = [x.isoweekday() for x in installs_df.create_date]
    # installs_df['day_of_year'] = [1+(x - x.replace(month=1).replace(day=1)).days for x in installs_df.reset_index().create_date]
    # installs_df['days_since_2011'] = [1+(x - date(2011,1,1)).days for x in installs_df.reset_index().create_date]
    # df = installs_df.set_index('user_id').drop('create_date',1)

    query = """
    /* Get Day/Week/Month/Year Features */
    with users as(
      select user_id, real_money_date
      from ww.dim_users
      where real_money_date between '{install_start_date}' and '{install_end_date}'
    )
    select user_id,
    year(real_money_date) as year,
    month(real_money_date) as month,
    DAYOFWEEK(real_money_date) as day_of_week,
    DAYOFMONTH(real_money_date) as day_of_month,
    DAYOFYEAR(real_money_date) as day_of_year,
    WEEK_ISO(real_money_date) as iso_week,
    QUARTER(real_money_date) as quarter,
    DATEDIFF('day', '2008-01-01', real_money_date::date) as days_since_2008
    from users
    """.format(install_start_date=install_start_date, install_end_date=install_end_date, n_days=n_days)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['user_id'])
    return df


def get_days_to_ftd(install_start_date, install_end_date, n_days=1, connection=None):
    query = """
    select user_id, datediff('day', createdate, real_money_date) as days_to_ftd
    from ww.dim_users
    where real_money_date::date between '{install_start_date}' and '{install_end_date}'
    """.format(install_start_date=install_start_date, install_end_date=install_end_date, n_days=n_days)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['user_id'])
    return df


def get_mobile_play(install_start_date, install_end_date, n_days=1, connection=None):
    query = """
    /* Aggregate user info for user_ids observed on each device (IDFV) */
    with users as(
      select user_id, real_money_date
      from ww.dim_users
      where real_money_date::date between '{install_start_date}' and '{install_end_date}'
    ),
    phoenix_events as (
      SELECT a.user_id, a.real_money_date, b.idfv, b.event_time
      FROM users a
      LEFT JOIN phoenix.events_client b
      ON a.user_id::INT = b.user_id::INT
      WHERE datediff('day', a.real_money_date::date, event_day) BETWEEN 0 AND ({n_days}-1)
    ),
    last_active_query AS (
      SELECT
      user_id, idfv,
      real_money_date,
      min(first_event_time) as first_event_time,
      min(last_event_time) as last_event_time
      FROM (
        SELECT user_id, idfv,
        real_money_date,
        FIRST_VALUE(event_time) OVER ( PARTITION BY user_id, idfv ORDER BY event_time ASC) AS first_event_time,
        FIRST_VALUE(event_time) OVER ( PARTITION BY user_id, idfv ORDER BY event_time DESC) AS last_event_time
        FROM phoenix_events a
        WHERE user_id is not null
      ) x
      group by user_id, idfv, real_money_date
    ),
    joined_idfvs as (
      select a.user_id, a.real_money_date, b.idfv,  b.first_event_time, b.last_event_time
      from users a
      left join last_active_query b
      on a.user_id = b.user_id
    )
    select user_id, 
    MAX(case when idfv IS NOT NULL then 1 ELSE 0 end) as played_mobile,
    COALESCE(MIN(DATEDIFF('day', real_money_date::date, first_event_time::date)), -1) + 1 as first_mobile_event_time,
    COALESCE(MAX(DATEDIFF('day', real_money_date::date, last_event_time::date)), -1) + 1 as last_mobile_event_time
    from joined_idfvs
    where user_id IS NOT NULL
    group by 1
    """.format(install_start_date=install_start_date, install_end_date=install_end_date, n_days=n_days)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['user_id'])
    return df


def get_chum_ftue(install_start_date, install_end_date, n_days=1, connection=None):
    query = """
    /* CHUM/FTUE phoenix events */
    with users as(
      select user_id, real_money_date
      from ww.dim_users
      where real_money_date::date between '{install_start_date}' and '{install_end_date}'
    ),
    chum_trigger_bb as (
      select user_id, event_time, event_name, str_attr1,
      /* begin selecting ftue events */
      case when event_name = 'awardRedeemEvent' and str_attr1 = 'CHUM' then num_attr1 else 0 end as bonus_bucks_received,
      case when event_name = 'cashDeposit' and str_attr1 = 'PayPal' then num_attr1 else 0 end as PayPal,
      case when event_name = 'tutorialClickEvent' and str_attr1 = 'chum_inprogress_tournament_shown' then 1 else 0 end as chum_inprogress_tournament_shown,
      case when event_name = 'tutorialClickEvent' and str_attr1 = 'chum_lost_tournament_shown' then 1 else 0 end as chum_lost_tournament_shown,
      case when event_name = 'tutorialClickEvent' and str_attr1 = 'chum_money_tournament_shown' then 1 else 0 end as chum_money_tournament_shown,
      case when event_name = 'tutorialClickEvent' and str_attr1 = 'chum_won_tournament_shown' then 1 else 0 end as chum_won_tournament_shown,
      case when event_name = 'tutorialClickEvent' and str_attr1 = 'ftue_p1_results_shown' then 1 else 0 end as ftue_p1_results_shown,
      case when event_name = 'tutorialClickEvent' and str_attr1 = 'ftue_p2_results' then 1 else 0 end as ftue_p2_results,
      case when event_name = 'tutorialClickEvent' and str_attr1 = 'ftue_p2_warm_up_tournament' then 1 else 0 end as ftue_p2_warm_up_tournament,
      case when event_name = 'tutorialClickEvent' and str_attr1 = 'ftue_p3_results' then 1 else 0 end as ftue_p3_results,
      case when event_name = 'tutorialClickEvent' and str_attr1 = 'ftue_p3_warm_up_tournament' then 1 else 0 end as ftue_p3_warm_up_tournament,
      case when event_name = 'tutorialClickEvent' and str_attr1 = 'ftue_tournament_results_button' then 1 else 0 end as ftue_tournament_results_button,
      case when event_name = 'tutorialClickEvent' and str_attr1 = 'ftue_tournament_screen1' then 1 else 0 end as ftue_tournament_screen1,
      case when event_name = 'tutorialClickEvent' and str_attr1 = 'ftue_tournament_screen2' then 1 else 0 end as ftue_tournament_screen2,
      case when event_name = 'tutorialClickEvent' and str_attr1 = 'welcome_back' then 1 else 0 end as welcome_back,
      /* END */
      from phoenix.events_client
      where event_name in ('awardRedeemEvent', 'cashDeposit', 'tutorialClickEvent')
      and event_day between '{install_start_date}' and (date('{install_end_date}') + interval '{n_days} days')
    )
    select a.user_id,
    SUM(bonus_bucks_received) as bonus_bucks_received,
    SUM(chum_inprogress_tournament_shown) as chum_inprogress_tournament_shown,
    SUM(chum_lost_tournament_shown) as chum_lost_tournament_shown,
    SUM(chum_money_tournament_shown) as chum_money_tournament_shown,
    SUM(chum_won_tournament_shown) as chum_won_tournament_shown,
    SUM(ftue_p1_results_shown) as ftue_p1_results_shown,
    SUM(ftue_p2_results) as ftue_p2_results,
    SUM(ftue_p2_warm_up_tournament) as ftue_p2_warm_up_tournament,
    SUM(ftue_p3_results) as ftue_p3_results,
    SUM(ftue_p3_warm_up_tournament) as ftue_p3_warm_up_tournament,
    SUM(ftue_tournament_results_button) as ftue_tournament_results_button,
    SUM(ftue_tournament_screen1) as ftue_tournament_screen1,
    SUM(ftue_tournament_screen2) as ftue_tournament_screen2,
    SUM(welcome_back) as welcome_back,
    MAX(case when bonus_bucks_received > 0 then datediff('hour', real_money_date, event_time) else NULL end) as time_to_chum_hr
    from users a
    left join chum_trigger_bb b
    on a.user_id::int = b.user_id::int
    and datediff('day', real_money_date::date, event_time::date) between 0 and ({n_days}-1)
    group by 1,2
    """.format(install_start_date=install_start_date, install_end_date=install_end_date, n_days=n_days)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['user_id'])
    return df


def get_session_info(install_start_date, install_end_date, n_days=1, connection=None):
    query = """
    with users as(
      select user_id, real_money_date
      from ww.dim_users
      where real_money_date::date between '{install_start_date}' and '{install_end_date}'
    ),
    app_sessions as(
      select user_id,
      sum(duration_s) as total_duration_s_app,
      count(distinct session||idfv) as total_sessions_app,
      count(distinct event_day) as app_days,
      max(start_ts) as last_app_session_start_ts
      from (
        select distinct
        e.user_id,
        ds.idfv,
        ds.event_day,
        ds.session,
        ds.start_ts,
        ds.end_ts,
        ds.duration_s
        from users a            

        left join phoenix.events_client e
        on a.user_id::int = e.user_id::int
        and datediff('day', real_money_date::date, event_time::date) between 0 and ({n_days}-1)

        left join phoenix.device_sessions ds
        on e.idfv = ds.idfv    
        and e.event_time >= ds.start_ts
        and e.event_time <= ds.end_ts
        and datediff('day', real_money_date::date, start_ts::date) between 0 and ({n_days}-1)

        where e.user_id is not null
        and ds.event_day between '{install_start_date}' and (date('{install_end_date}') + interval '{n_days} days')
        and e.event_day between '{install_start_date}' and (date('{install_end_date}') + interval '{n_days} days')
      ) x
      group by user_id
    ),
    web_sessions as(
      select a.user_id, 
      sum(duration_seconds) as total_duration_s_web,
      count(distinct session) as total_sessions_web,
      count(distinct date(start_time)) as web_days,
      max(start_time) as last_web_session_start_ts
      from users a
      left join ww.events_web_sessions b
      on a.user_id::int = b.user_id::int
      and datediff('day', real_money_date::date, start_time::date) between 0 and ({n_days}-1)
      where b.user_id is not null
      and start_time::date between '{install_start_date}' and (date('{install_end_date}') + interval '{n_days} days')
      group by a.user_id
    )
    select a.user_id,
    COALESCE(total_duration_s_app, 0) as total_duration_s_app,
    COALESCE(total_sessions_app, 0) as total_sessions_app,
    COALESCE(app_days, 0) as app_days,
    datediff('hour', coalesce(last_app_session_start_ts, real_money_date::date), real_money_date::date + interval '{n_days} days') as hrs_since_last_app_session,
    COALESCE(total_duration_s_web, 0) as total_duration_s_web,
    COALESCE(total_sessions_web, 0) as total_sessions_web,
    COALESCE(web_days, 0) as web_days,
    datediff('hour', coalesce(last_web_session_start_ts, real_money_date::date), real_money_date::date + interval '{n_days} days') as hrs_since_last_web_session
    from users a
    left join app_sessions b
    on a.user_id::int = b.user_id::int
    and datediff('day', real_money_date::date, last_app_session_start_ts::date) between 0 and ({n_days}-1)
    left join web_sessions c
    on a.user_id::int = c.user_id::int
    and datediff('day', real_money_date::date, last_web_session_start_ts::date) between 0 and ({n_days}-1)  
    """.format(install_start_date=install_start_date, install_end_date=install_end_date, n_days=n_days)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['user_id'])
    return df


def get_games_played_by_id(install_start_date, install_end_date, n_days=1, connection=None):
    query = """
    /* Gets Counts of games played by category and gametype_id */
    with users as(
      select user_id, real_money_date
      from ww.dim_users
      where real_money_date::date between '{install_start_date}' and '{install_end_date}'
    ),
    games_played as ( -- 931 Million Rows
      select user_id, gametype_id, start_date
      from ww.played_games
      where start_date between '{install_start_date}' and (date('{install_end_date}') + interval '{n_days} days' - interval '1 days')
    ),
    game_types as (
      select gametype_id, category 
      from ww.dim_gametypes
    ),
    user_agg as (
      select a.user_id,
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
      from users a
      left join games_played b -- 931 Million Rows Here
      on a.user_id::int = b.user_id::int
      and datediff('day', real_money_date::date, start_date) BETWEEN 0 AND ({n_days}-1)
      left join game_types c -- Supplementary Game Info, no problem
      on b.gametype_id = c.gametype_id
      group by a.user_id
    )
    select *
    from user_agg
    """.format(install_start_date=install_start_date, install_end_date=install_end_date, n_days=n_days)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['user_id'])
    return df


def get_user_info(install_start_date, install_end_date, n_days=1, connection=None):
    query = """
    /* Aggregate user info for user_ids observed on each device (IDFV) */
    with users as(
      select user_id, real_money_date
      from ww.dim_users
      where real_money_date::date between '{install_start_date}' and '{install_end_date}'
    ),
    user_info as (
      select *
      from ww.dim_users
      where real_money_date::date between '{install_start_date}' and (date('{install_end_date}') + interval '{n_days} days' - interval '1 days')
      and user_id is not null
    ),
    user_agg as (
      select a.user_id as a_user_id, a.real_money_date as a_real_money_date, 
      b.*,
      /* Feature Engineering */
      -- Geolocation --

      -- User Name -- 
      SPLIT_PART(username, '.', 1) as username_prefix,
      LENGTH(SPLIT_PART(username, '.', 1)) as num_char_username_prefix,
      SPLIT_PART(username, '.', 2) as username_suffix,
      LENGTH(SPLIT_PART(username, '.', 2)) as num_char_username_suffix,
      REGEXP_REPLACE(username, '\D', '') as numbers_in_username,
      REGEXP_COUNT(username, '[0-9]') as count_numbers_in_username,
      -- Email --
      SPLIT_PART(email, '@', 1) as email_username,
      SPLIT_PART(email, '@', 2) as email_domain,
      (SPLIT_PART(email, '@', 2) like '%.edu%')::INT as is_student,
      REGEXP_COUNT(SPLIT_PART(email, '@', 1), '\.') as num_dots_in_email_username,
      LENGTH(SPLIT_PART(email, '@', 1)) as num_char_email_username,
      REGEXP_REPLACE(SPLIT_PART(email, '@', 1), '\D', '') as numbers_in_email_username,
      REGEXP_COUNT(SPLIT_PART(email, '@', 1), '[0-9]') as count_numbers_in_email_username,
      REGEXP_SUBSTR(SPLIT_PART(email, '@', 2), '^.+\.') as email_domain_company,
      REGEXP_SUBSTR(SPLIT_PART(email, '@', 2), '\..+$') as email_domain_ending,
      case when sendemail = 't' then 1 when sendemail ='f' then 0 else 0 end as allows_emails,
      case when show_email_check = 't' then 1 when show_email_check ='f' then 0 else 0 end as shows_emailaddress_in_profile,
      -- Banned --
      --coalesce((user_banned > 0 and datediff('second', a.real_money_date, user_banned_date) between 0 and (86400 * {n_days}))::INT, 0) as is_banned, -- captured in another feature query!
      --LEAST(coalesce(datediff('second', a.real_money_date, user_banned_date)/(60*60), 24*{n_days}), 24*{n_days}) as time_to_ban_hr,
      -- Other --
      (SPLIT_PART(username, '.', 1) = SPLIT_PART(email, '@', 1))::INT as username_equals_email_username
      /* End */
      from users a
      left join user_info b
      on a.user_id::int = b.user_id::int
    ),
    agg as (
      select a_user_id as user_id,
      -- ww.dim_users portion --
      --max(username) as username,
      --max(createdate) as createdate,
      max(case when ISUTF8(city) then country else NULL end) as city,
      max(case when ISUTF8(state) then country else NULL end) as state,
      max(case when ISUTF8(country) then country else NULL end) as country,
      max(case when ISUTF8(zip) then zip else NULL end) as zip,    
      --max(balance) as balance, -- Likely a leakage feature
      --max(email) as email,
      max(accesslevel) as accesslevel,
      max(gender) as gender, -- can be updated at anytime, so slight leakage potential
      max(age) as age, -- can be updated at anytime, so slight leakage potential
      max(case when referrer_id is not null then 1 else 0 end) as was_referred,
      max(advertiser_id) as advertiser_id,
      LEFT(max(partnertag), 4) as partnertag,
      max(case when cid is NULL then 1 else 0 end) as cid_is_null,
      max(case when sendemail = 't' then 1 else 0 end) as sendemail,
      --max(play_money) as play_money,
      max(case when show_email_check = 't' then 1 else 0 end) as show_email_check,
      --max(user_banned) as user_banned,
      --max(address_verified) as address_verified, -- Likely a leakage feature 
      --max(a.real_money_date) as a.real_money_date, -- not used
      --max(first_game_played_date) as first_game_played_date, -- not used
      --max(unsubscribed_flag) as unsubscribed_flag, -- not used
      --max(universe) as universe, -- Likely a leakage feature
      --max(user_banned_date) as user_banned_date, -- not used
      --max(account_state) as account_state, -- Likely a leakage feature 
      max(cobrand_id) as cobrand_id,
      -- max(deposit_credits) as deposit_credits, -- Likely a leakage feature 
      -- feature engineered portion --
      -- Geolocation --

      -- User Name -- 
      --max(username_prefix) as username_prefix,
      max(num_char_username_prefix) as num_char_username_prefix,
      max(username_suffix) as username_suffix,
      max(num_char_username_suffix) as num_char_username_suffix,
      --max(case when numbers_in_username = '' then '-999' else numbers_in_username end) as numbers_in_username,
      max(count_numbers_in_username) as count_numbers_in_username
      -- Email -- /* forget about email for now... due to CCPA and GDPR */
      -- max(email_username) email_username,
      -- max(email_domain) email_domain,
      -- max(is_student) is_student,
      -- max(num_dots_in_email_username) num_dots_in_email_username,
      -- max(num_char_email_username) num_char_email_username,
      -- max(case when numbers_in_email_username = '' then '-999' else numbers_in_email_username end) numbers_in_email_username,
      -- max(count_numbers_in_email_username) count_numbers_in_email_username,
      -- max(email_domain_company) email_domain_company,
      -- max(email_domain_ending) email_domain_ending,
      -- max(allows_emails) allows_emails,
      -- max(shows_emailaddress_in_profile) shows_emailaddress_in_profile,
      -- Banned --
      --max(is_banned) as is_banned, 
      --max(time_to_ban_hr) as time_to_ban_hr
      -- Other --
      --max(username_equals_email_username) as username_equals_email_username
      /* End */
      from user_agg a
      group by a.a_user_id
    )
    select *
    from agg
    """.format(install_start_date=install_start_date, install_end_date=install_end_date, n_days=n_days)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['user_id'])
    return df


def get_entry_sizes(install_start_date, install_end_date, n_days=1, connection=None):
    query = """
    /* Gets the average, max, etc., entry fee signed up for */
    with users as (
      select user_id, real_money_date
      from ww.dim_users
      where real_money_date::date between '{install_start_date}' and '{install_end_date}'
    ),
    games_played as (
      select pg.user_id, start_time, fee, purse
      from (
        select user_id, tournament_id, gametype_id, start_time
        from ww.played_games as pg
        where start_time between '{install_start_date}' and (date('{install_end_date}') + interval '{n_days} days')
      ) as pg
      /* left join tournament info */
      left join(
        select tournament_id, ttemplate_id, fee, purse
        from ww.tournaments
        ) as t
      using(tournament_id)
      where user_id is not null
    ),
    user_unagg as (
      select a.user_id, fee, start_time
      from users a
      left join games_played b
      on a.user_id::int = b.user_id::int
      and datediff('day', real_money_date::date, start_time::date) BETWEEN 0 AND ({n_days}-1)
      where fee > 0 and purse > 0
    ),
    user_agg as (
      select x.user_id, x.max_entry_fee, x.avg_entry_fee, x.var_entry_fee, y.median_entry_fee
      FROM (
        select user_id,
        MAX(fee) as max_entry_fee,
        AVG(fee) as avg_entry_fee,
        round(VARIANCE(fee), 4) as var_entry_fee
        from user_unagg
        group by user_id
      ) x
      LEFT JOIN (
        select distinct user_id, 
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY fee ASC) OVER (PARTITION BY user_id) as median_entry_fee
        from user_unagg
      ) y
      using(user_id)
    )
    select users.user_id, max_entry_fee, avg_entry_fee, var_entry_fee, median_entry_fee
    from users
    left join user_agg b
    using(user_id)
    """.format(install_start_date=install_start_date, install_end_date=install_end_date, n_days=n_days)
    df = get_dataframe_from_query(query, connection=connection)
    df = df.set_index(['user_id'])
    return df
