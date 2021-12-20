select drop_partition('airflow.snapshot_responsys_ww_pet_affinity', '{{ ds }}');

insert   /*+ direct,LABEL ('airflow-skill-worldwinner_gsncom_daily_imports-t_prepare_responsys_ww_pet_affinity')*/ INTO airflow.snapshot_responsys_ww_pet_affinity
SELECT DISTINCT a.user_id AS CUSTOMER_ID,
       CASE
         WHEN e.user_id IS NULL THEN 'N'
         ELSE 'Y'
       END AS APP_INSTALLED,
       CASE
         WHEN b.gametype_id = '1' THEN 'Y'
         ELSE 'N'
       END AS SOL_RUSH_PRI_GAME_AFF,
       CASE
         WHEN b.gametype_id = '20' THEN 'Y'
         ELSE 'N'
       END AS SWAPIT_PRI_GAME_AFF,
       CASE
         WHEN b.gametype_id = '29' THEN 'Y'
         ELSE 'N'
       END AS SPADES_PRI_GAME_AFF,
       CASE
         WHEN b.gametype_id = '33' THEN 'Y'
         ELSE 'N'
       END AS LUXOR_PRI_GAME_AFF,
       CASE
         WHEN b.gametype_id = '34' THEN 'Y'
         ELSE 'N'
       END AS BEJEWELED2_PRI_GAME_AFF,
       CASE
         WHEN b.gametype_id = '35' THEN 'Y'
         ELSE 'N'
       END AS SCRABBLE_CUBES_PRI_GAME_AFF,
       CASE
         WHEN b.gametype_id = '38' THEN 'Y'
         ELSE 'N'
       END AS SPIDER_SOL_PRI_GAME_AFF,
       CASE
         WHEN b.gametype_id = '83' THEN 'Y'
         ELSE 'N'
       END AS CATCH_21_PRI_GAME_AFF,
       CASE
         WHEN b.gametype_id = '115' THEN 'Y'
         ELSE 'N'
       END AS WOF_PRI_GAME_AFF,
       CASE
         WHEN b.gametype_id = '117' THEN 'Y'
         ELSE 'N'
       END AS SUPER_PLINKO_PRI_GAME_AFF,
       CASE
         WHEN b.gametype_id = '118' THEN 'Y'
         ELSE 'N'
       END AS BIG_MONEY_PRI_GAME_AFF,
       CASE
         WHEN b.gametype_id = '121' THEN 'Y'
         ELSE 'N'
       END AS SOL_TRIPEAKS_PRI_GAME_AFF,
       CASE
         WHEN b.gametype_id = '122' THEN 'Y'
         ELSE 'N'
       END AS TRIVIAL_PURSUIT_PRI_GAME_AFF,
       CASE
         WHEN b.gametype_id = '124' THEN 'Y'
         ELSE 'N'
       END AS ABC_PRI_GAME_AFF,
       CASE
         WHEN b.gametype_id = '154' THEN 'Y'
         ELSE 'N'
       END AS DYNOMITE_PRI_GAME_AFF,
       CASE
         WHEN (b.gametype_id NOT IN ('1','20','29','33','34','35','38','83','115','117','118','121','122','124','154') OR b.gametype_id IS NULL) THEN 'Y'
         ELSE 'N'
       END AS NO_GAME_AFF,
       CASE
         WHEN c.gametype_id = '1' THEN 'Y'
         ELSE 'N'
       END AS SOL_RUSH_SEC_GAME_AFF,
       CASE
         WHEN c.gametype_id = '20' THEN 'Y'
         ELSE 'N'
       END AS SWAPIT_SEC_GAME_AFF,
       CASE
         WHEN c.gametype_id = '29' THEN 'Y'
         ELSE 'N'
       END AS SPADES_SEC_GAME_AFF,
       CASE
         WHEN c.gametype_id = '33' THEN 'Y'
         ELSE 'N'
       END AS LUXOR_SEC_GAME_AFF,
       CASE
         WHEN c.gametype_id = '34' THEN 'Y'
         ELSE 'N'
       END AS BEJEWELED2_SEC_GAME_AFF,
       CASE
         WHEN c.gametype_id = '35' THEN 'Y'
         ELSE 'N'
       END AS SCRABBLE_CUBES_SEC_GAME_AFF,
       CASE
         WHEN c.gametype_id = '38' THEN 'Y'
         ELSE 'N'
       END AS SPIDER_SOL_SEC_GAME_AFF,
       CASE
         WHEN c.gametype_id = '83' THEN 'Y'
         ELSE 'N'
       END AS CATCH_21_SEC_GAME_AFF,
       CASE
         WHEN c.gametype_id = '115' THEN 'Y'
         ELSE 'N'
       END AS WOF_SEC_GAME_AFF,
       CASE
         WHEN c.gametype_id = '117' THEN 'Y'
         ELSE 'N'
       END AS SUPER_PLINKO_SEC_GAME_AFF,
       CASE
         WHEN c.gametype_id = '118' THEN 'Y'
         ELSE 'N'
       END AS BIG_MONEY_SEC_GAME_AFF,
       CASE
         WHEN c.gametype_id = '121' THEN 'Y'
         ELSE 'N'
       END AS SOL_TRIPEAKS_SEC_GAME_AFF,
       CASE
         WHEN c.gametype_id = '122' THEN 'Y'
         ELSE 'N'
       END AS TRIVIAL_PURSUIT_SEC_GAME_AFF,
       CASE
         WHEN c.gametype_id = '124' THEN 'Y'
         ELSE 'N'
       END AS ABC_SEC_GAME_AFF,
       CASE
         WHEN c.gametype_id = '154' THEN 'Y'
         ELSE 'N'
       END AS DYNOMITE_SEC_GAME_AFF,
       CASE
         WHEN d.type_name ilike '''Limited Entry''' THEN 'Y'
         ELSE 'N'
       END AS LIMITED_ENTRY_TOURN_AFF,
       CASE
         WHEN d.type_name ilike '''Super Rewards''' THEN 'Y'
         ELSE 'N'
       END AS SUPER_REWARDS_TOURN_AFF,
       CASE
         WHEN d.type_name ilike '''Unlimited Entry''' THEN 'Y'
         ELSE 'N'
       END AS FPUE_TOURN_AFF,
       CASE
         WHEN d.type_name ilike '''Premium''' THEN 'Y'
         ELSE 'N'
       END AS PREMIUM_TOURN_AFF,
       CASE
         WHEN d.type_name ilike '''Progressive Rush''' THEN 'Y'
         ELSE 'N'
       END AS PROGRESSIVE_RUSH_TOURN_AFF,
       CASE
         WHEN d.type_name ilike '''Progressive''' THEN 'Y'
         ELSE 'N'
       END AS PROGRESSIVE_TOURN_AFF,
       CASE
         WHEN z.platform = 'Desktop' THEN 'Y'
         ELSE 'N'
       END AS DESKTOP_AFF,
       CASE
         WHEN z.platform = 'MoWeb' THEN 'Y'
         ELSE 'N'
       END AS MOWEB_AFF,
       CASE
         WHEN z.platform = 'App' THEN 'Y'
         ELSE 'N'
       END AS APP_AFF,
       '{{ ds }}' as ds,
       CASE
         WHEN b.gametype_id = '127' THEN 'Y'
          ELSE 'N'
       END AS POP_PRI_GAME_AFF,
       CASE
         WHEN b.gametype_id = '128' THEN 'Y'
         ELSE 'N'
       END AS TWODOTS_PRI_GAME_AFF,
       CASE
         WHEN b.gametype_id = '126' THEN 'Y'
         ELSE 'N'
       END AS PACMAN_PRI_GAME_AFF,
       CASE
         WHEN b.gametype_id = '125' THEN 'Y'
         ELSE 'N'
       END AS SWAP2_PRI_GAME_AFF,
        CASE
         WHEN c.gametype_id = '127' THEN 'Y'
         ELSE 'N'
       END AS POP_SEC_GAME_AFF,
       CASE
         WHEN c.gametype_id = '128' THEN 'Y'
         ELSE 'N'
       END AS TWODOTS_SEC_GAME_AFF,
       CASE
         WHEN c.gametype_id = '126' THEN 'Y'
         ELSE 'N'
       END AS PACMAN_SEC_GAME_AFF,
       CASE
         WHEN c.gametype_id = '125' THEN 'Y'
         ELSE 'N'
       END AS SWAP2_SEC_GAME_AFF
FROM ww.dim_users a --- user_id
  LEFT JOIN (SELECT DISTINCT user_id,
                    gametype_id
             FROM (SELECT user_id,
                          gametype_id,
                          gp,
                          ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY gp DESC) AS rownuma
                   FROM (SELECT user_id,
                                a.gametype_id,
                                COUNT(*) AS gp
                         FROM ww.played_games a
                           LEFT JOIN ww.tournament_games b ON a.tournament_id = b.tournament_id
                           LEFT JOIN ww.dim_gametypes c ON b.gametype_id = c.gametype_id
                           LEFT JOIN ww.tournaments d ON a.tournament_id = d.tournament_id
                         WHERE DATE (a.start_time) >= DATE ('{{ tomorrow_ds }}') - 120
                         AND   DATE (a.start_time) <= DATE ('{{ tomorrow_ds }}')
                         GROUP BY 1,
                                  2) a) a
             WHERE rownuma = '1') b ON a.user_id = b.user_id --- gametypes --- 1st affinity
  LEFT JOIN (SELECT DISTINCT user_id,
                    gametype_id
             FROM (SELECT user_id,
                          gametype_id,
                          gp,
                          ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY gp DESC) AS rownuma
                   FROM (SELECT user_id,
                                a.gametype_id,
                                COUNT(*) AS gp
                         FROM ww.played_games a
                           LEFT JOIN ww.tournament_games b ON a.tournament_id = b.tournament_id
                           LEFT JOIN ww.dim_gametypes c ON b.gametype_id = c.gametype_id
                           LEFT JOIN ww.tournaments d ON a.tournament_id = d.tournament_id
                         WHERE DATE (a.start_time) >= DATE ('{{ tomorrow_ds }}') - 120
                         AND   DATE (a.start_time) <= DATE ('{{ tomorrow_ds }}')
                         GROUP BY 1,
                                  2) a) a
             WHERE rownuma = '2') c ON a.user_id = c.user_id --- gametypes --- 2nd affinity
  LEFT JOIN
          (select distinct
                a.user_id,
                a.type_name
          from
                (select
                  a.user_id,
                  a.tournament_type,
                  a.type_name,
                  count(*) as gp,
                 row_number() over (partition by a.user_id order by count(*) desc) rownuma
                 from
                    (select
                      a.user_id,
                      a.gametype_id a_gametype,
                      e.tournament_type,
                      e.type_name
                     from ww.played_games a
                        left join ww.tournaments d
                          on a.tournament_id = d.tournament_id
                        left join ww.dim_tournament_types e
                          on d.type = e.tournament_type
                      where date(a.start_time) >= date('{{ tomorrow_ds }}') - 120
                        and date(a.start_time) <= date('{{ tomorrow_ds }}')) a
                  group by 1,2,3) a
            where rownuma = 1) d on a.user_id = d.user_id--- tournament affinty by type name, 9/6
  LEFT JOIN (SELECT DISTINCT user_id FROM phoenix.events_client) e ON a.user_id = e.user_id --- app install
  LEFT JOIN (SELECT DISTINCT
                a.user_id,
                a.platform
            FROM
                (SELECT
                  it.user_id,
                  gp.platform_id,
                  case
                    when gp.platform_id = '1' then 'Desktop'
                    when gp.platform_id = '2' then 'MoWeb'
                    else 'App' end as platform,
                row_number() over (partition by it.user_id order by
                 (case
                    when gp.platform_id = '1' then 'Desktop'
                    when gp.platform_id = '2' then 'MoWeb'
                    else 'App' end)desc) platform_rank,
                sum(it.amount) * -1 as cef
                FROM ww.internal_transactions it
                  LEFT JOIN ww.played_game_platform gp
                    ON it.user_id = gp.user_id
                  LEFT JOIN ww.platforms pp
                    ON gp.platform_id = pp.platform_id
                WHERE
                  it.trans_type = 'SIGNUP'
                  and date(it.trans_date) >= date('{{ tomorrow_ds }}') - 120
                  and date(it.trans_date) <= date('{{ tomorrow_ds }}')
                GROUP BY 1,2
                ORDER BY it.user_id) a
            WHERE a.platform_rank = '1') z on a.user_id = z.user_id -- platform affinity
     LEFT JOIN ww.dim_user_aux y ON a.user_id = y.user_id
     /*LEFT JOIN (SELECT DISTINCT user_id,
                    P1_Game ---first game played, no matter if cash or free
                FROM (SELECT
                      pg.user_id as user_id,
                      min(start_time) start_time,
                      pg.gametype_id,
                      name P1_Game,
                      row_number() over (partition by pg.user_id order by start_time) game_rank
                      FROM played_games pg
                        JOIN dim_gametypes gt
                          ON pg.gametype_id = gt.gametype_id
                      GROUP BY pg.user_id, pg.gametype_id, name,start_time) x
                     WHERE game_rank = 1) zz ON a.user_id = zz.user_id --- P1 Game, added 9/6*/ -- moved to summary table 10/22
    /*LEFT JOIN (SELECT
                it.user_id,
                it.trans_type,
                count(it.int_trans_id) bb_count,
                sum(it.deposit_credits_amount) bb_signup,
                sum(it.deposit_credits) bb_balance
              FROM internal_transactions it
              WHERE
                it.trans_type in ('SIGNUP')
                AND date(it.trans_date) >= date('{{ tomorrow_ds }}') - 120
                AND date(it.trans_date) <= date('{{ tomorrow_ds }}')
                AND it.deposit_credits_amount <> '0.00' --- helps with bb count, if not counts cash signups too
              GROUP BY 1,2) xx on a.user_id = xx.user_id --- BB Balance, BB signups, added 9/9 */ --- moved to summary table 10/22
    LEFT JOIN (SELECT DISTINCT --- for users who registered after 10/8
                a.user_id,
                case
                  when a.platform_id in ('1','2') then 'Web'
                  when a.platform_id > 2 then 'App'
                end as reg_platform
              FROM
                  (select distinct
                    user_id,
                    activity_date,
                    platform_id,
                    row_number() over (partition by user_id order by activity_date) as login_rank
                  from ww.login_activity) a
              JOIN ww.dim_users u
                  ON a.user_id = u.user_id
              WHERE a.login_rank = 1
                  AND u.createdate >= '2018-10-09'
                  AND u.email not ilike '%purge%'

              UNION

              SELECT DISTINCT --- for users who registered before 10/8
                u.user_id,
                case
                  when e.user_id is not null then 'App'
                  when e.user_id is null then 'Web'
                end as reg_platform
              FROM ww.dim_users u
                LEFT JOIN phoenix.events_client e
                  ON u.user_id = e.user_id
                  AND e.event_name in ('accountRegister','registrationSuccessful','registrationSuccessWithPromo')
              WHERE u.createdate < '2018-10-09') yy on a.user_id = yy.user_id
WHERE
    a.email NOT ilike '%@donotreply.gsn.com%'
  AND a.user_banned = 0
  AND y.last_login >= (date('{{ tomorrow_ds }}')-365);