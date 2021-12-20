select drop_partition('airflow.snapshot_responsys_ww_pet_summary', '{{ ds }}');

INSERT   /*+ direct,LABEL ('airflow-skill-worldwinner_gsncom_daily_imports-t_prepare_responsys_ww_pet_summary')*/ INTO airflow.snapshot_responsys_ww_pet_summary
SELECT distinct  a.user_id AS CUSTOMER_ID
                ,a.createdate as CASHGAMES_REG_DATE
                ,b.first_game_played_date as FIRST_GAME_PLAYED_DATE
                ,case when b.last_game_played_date > a.real_money_date then b.last_game_played_date else null end as LAST_GAME_PLAYED_DATE
                ,i.LAST_CASH_GAME_PLAYED_DATE
                ,i.LAST_DEPOSIT_DATE
                ,gg.last_login as LAST_LOGIN_DATE
                ,a.real_money_date as REAL_MONEY_DATE
                ,a.balance + a.play_money + a.deposit_credits as TOTAL_BALANCE
                ,a.play_money as GAME_CREDIT_BALANCE
                ,a.balance as CASH_BALANCE
                ,jjjj.rp_balance as RP_BALANCE
                ,i.SIGNUP_COUNT as TOTAL_SIGNUPS
                ,i.DEPOSIT_COUNT as TOTAL_DEPOSITS
                ,total_deposits + total_withdrawals as NET_DEPOSITS
                ,i.TOTAL_ENTRY_FEES as TOTAL_ENTRY_FEES
                ,i.CASH_ENTRY_FEES
                ,i.PLAY_MONEY_ENTRY_FEES
                ,ll.PREMIER_LEVEL_CURRENT_MONTH
                ,mm.PREMIER_LEVEL_PREVIOUS_MONTH
                ,ll.PREMIER_POINTS_CURRENT_MONTH --- added 9/6/19
                ,j.start_date AS PREMIER_CLUB_SIGNUP_DATE
                ,a.account_state as ACCOUNT_STATE
                ,'{{ ds }}' as ds
                ,a.deposit_credits as BONUS_BUCKS_BALANCE
                ,i.BONUS_BUCKS_ENTRY_FEES --- added 9/6/19
                ,i.BONUS_BUCKS_AWARDED --- added 10/22
                ,mm.PREMIER_POINTS_PREVIOUS_MONTH --- added 9/6/19
                ,kk.PREMIER_POINTS_NEXT_MONTH --- added 10/22/19
                ,kk.PREMIER_LEVEL_NEXT_MONTH --- added 10/22/19
                ,zz.P1_Game as P1_GAME --- added 10/22
FROM ww.dim_users a
        LEFT JOIN  -- join for first/last game played date from played games
                (SELECT
                  user_id,
                  min(start_time) first_game_played_date,
                  max(start_time) last_game_played_date
                FROM ww.played_games
                GROUP BY 1) b on a.user_id = b.user_id
        LEFT JOIN ww.dim_profiles_private g ON a.user_id = g.user_id
        LEFT JOIN ww.dim_user_aux gg on a.user_id=gg.user_id ---- last login date from dim_users_aux
        LEFT JOIN (
                SELECT user_id
                        ,CASE
                                WHEN sum(email_tournament) = 1 THEN 'Y'
                                ELSE 'N'
                        END AS email_tournament
                        ,CASE
                                WHEN sum(email_win_notify) = 1 THEN 'Y'
                                ELSE 'N'
                        END AS email_win_notify
                        ,CASE
                                WHEN sum(email_promo) = 1 THEN 'Y'
                                ELSE 'N'
                        END AS email_promo
                        ,CASE
                                WHEN sum(email_in_chal) = 1 THEN 'Y'
                                ELSE 'N'
                        END AS email_in_chal
                        ,CASE
                                WHEN sum(email_in_survive) = 1 THEN 'Y'
                                ELSE 'N'
                        END AS email_in_survive
                FROM (
                        SELECT DISTINCT user_id
                                ,CASE
                                        WHEN
                                        (oi_id = '10'
                                        AND STATE = 't') THEN 1
                                        ELSE NULL
                                END AS email_tournament
                                ,CASE
                                        WHEN
                                        (oi_id = '12'
                                        AND STATE = 't') THEN 1
                                        ELSE NULL
                                END AS email_win_notify
                                ,CASE
                                        WHEN
                                        (oi_id = '2'
                                        AND STATE = 't') THEN 1
                                        ELSE NULL
                                END AS email_promo
                                ,CASE
                                        WHEN
                                        ( oi_id = '14'
                                         AND STATE = 't') THEN 1
                                        ELSE NULL
                                END AS email_in_chal
                                ,CASE
                                        WHEN
                                        (oi_id = '4'
                                        AND STATE = 't') THEN 1
                                        ELSE NULL
                                END AS email_in_survive
                                FROM ww.user_opt_ins
                                ORDER BY 1,2
                                ) a
                GROUP BY 1) jjj ON a.user_id = jjj.user_id --- used for specific email opt-ins
        LEFT JOIN
                (SELECT
                    it.user_id
                    , count(case when trans_type = 'SIGNUP' THEN int_trans_id end) as SIGNUP_COUNT
                    , count(case when (trans_type ilike '%deposit%') then int_trans_id end) DEPOSIT_COUNT
                    , max(case when trans_type='SIGNUP' then trans_date else null end) as LAST_CASH_GAME_PLAYED_DATE
                    , max(case when trans_type ilike '%deposit%' then trans_date else null end) as LAST_DEPOSIT_DATE
                    , sum(case when trans_type='SIGNUP' then amount else 0 end)*-1 as CASH_ENTRY_FEES
                    , sum(case when trans_type='SIGNUP' then play_money_amount else 0 end)*-1 as PLAY_MONEY_ENTRY_FEES
                    , ifnull(sum(case when trans_type='SIGNUP' then deposit_credits_amount else 0 end)*-1,0) as BONUS_BUCKS_ENTRY_FEES
                    , sum(case when trans_type = 'SIGNUP' then (amount + ifnull(deposit_credits_amount,0) + play_money_amount) else 0 end)* -1 TOTAL_ENTRY_FEES
                    , sum(case when trans_type = 'BNSBCKS-CR' then it.deposit_credits_amount else 0 end) as BONUS_BUCKS_AWARDED ---added 10/22
                FROM ww.internal_transactions it
                        JOIN ww.dim_users u
                        on it.user_id = u.user_id
                WHERE ((trans_type in ('SIGNUP','BNSBCKS-CR')) or (trans_type ilike '%deposit%'))
                AND trans_date >= u.real_money_date
                GROUP BY 1)i on a.user_id=i.user_id
        LEFT JOIN ww.pclub j on a.user_id=j.user_id
        LEFT JOIN
                (SELECT
                    user_id,
                    total_accrued-total_redeemed as rp_balance
                FROM ww.dim_point_account)jjjj on a.user_id=jjjj.user_id
        LEFT JOIN
                (SELECT
                    user_id,
                    (games_played+entry_fees_paid)*100 as PREMIER_POINTS_NEXT_MONTH, -- changed from current 10/22
                    case
                       when (games_played+entry_fees_paid)*100>=500000 then 'PLATINUM'
                       when (games_played+entry_fees_paid)*100>=250000 then 'GOLD'
                       when (games_played+entry_fees_paid)*100>=100000 then 'SILVER'
                    else 'MEMBER' end as PREMIER_LEVEL_NEXT_MONTH --- changed from current
                FROM
                    ww.pclub_games
                WHERE
                    year(date('{{ tomorrow_ds }}'))=year
                    AND month(date('{{ tomorrow_ds }}'))=month)kk on a.user_id=kk.user_id


        LEFT JOIN (SELECT
                      user_id,
                      (games_played+entry_fees_paid)*100 as PREMIER_POINTS_CURRENT_MONTH, -- changed from previous 10/22
                      case
                        when (games_played+entry_fees_paid)*100>=500000 then 'PLATINUM'
                        when (games_played+entry_fees_paid)*100>=250000 then 'GOLD'
                        when (games_played+entry_fees_paid)*100>=100000 then 'SILVER'
                       else 'MEMBER' end as PREMIER_LEVEL_CURRENT_MONTH --- changed from previous
                   FROM ww.pclub_games
                   WHERE
                        ((year(date('{{ tomorrow_ds }}')))=year
                        AND month(date('{{ tomorrow_ds }}'))-1=month)
                   OR
                        (year(date('{{ tomorrow_ds }}'))-1=year
                        AND month(date('{{ tomorrow_ds }}'))+10=month))ll on a.user_id=ll.user_id
        LEFT JOIN (SELECT
                      user_id,
                      (games_played+entry_fees_paid)*100 as PREMIER_POINTS_PREVIOUS_MONTH, -- 10/22
                      case
                        when (games_played+entry_fees_paid)*100>=500000 then 'PLATINUM'
                        when (games_played+entry_fees_paid)*100>=250000 then 'GOLD'
                        when (games_played+entry_fees_paid)*100>=100000 then 'SILVER'
                       else 'MEMBER' end as PREMIER_LEVEL_PREVIOUS_MONTH --- new 10/22
                   FROM ww.pclub_games
                   WHERE
                        ((year(date('{{ tomorrow_ds }}')))=year
                        AND month(date('{{ tomorrow_ds }}'))-2=month)
                   OR
                        (year(date('{{ tomorrow_ds }}'))-1=year
                        AND month(date('{{ tomorrow_ds }}'))+10=month))mm on a.user_id=mm.user_id
        LEFT JOIN (SELECT DISTINCT user_id,
                    P1_Game ---first game played, no matter if cash or free
                FROM (SELECT
                      pg.user_id as user_id,
                      min(start_time) start_time,
                      pg.gametype_id,
                      name P1_Game,
                      row_number() over (partition by pg.user_id order by start_time) game_rank
                      FROM ww.played_games pg
                        JOIN ww.dim_gametypes gt
                          ON pg.gametype_id = gt.gametype_id
                      GROUP BY pg.user_id, pg.gametype_id, name,start_time) x
                     WHERE game_rank = 1) zz ON a.user_id = zz.user_id --- P1 Game, added 10/22
WHERE a.email NOT ilike '%@deleted.local%'
        AND a.email NOT ilike '%@donotreply.gsn.com%'
        AND a.user_banned = 0
        AND a.email NOT IN
                (SELECT DISTINCT
                        email
                FROM
                        ww.email_suppression_list)
        AND gg.last_login>=(date('{{ tomorrow_ds }}')-365);
