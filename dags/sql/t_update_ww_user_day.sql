DELETE/*+ direct,LABEL ('airflow-skill-etl_ww_user_day-t_update_ww_user_day')*/
FROM ww.user_day
WHERE event_day = date('{{ds}}');

INSERT /*+ direct,LABEL ('airflow-skill-etl_ww_user_day-t_update_ww_user_day')*/INTO ww.user_day
SELECT user_id,
       event_day,
       SUM(CASE WHEN platform = 'desktop' THEN ump END) AS ump_desktop,
       SUM(CASE WHEN platform = 'moweb' THEN ump END) AS ump_moweb,
       SUM(CASE WHEN platform = 'phx' THEN ump END) AS ump_phx,
       MAX(ump) AS ump_all_platforms,
       SUM(CASE WHEN platform = 'desktop' THEN cef END) AS cef_desktop,
       SUM(CASE WHEN platform = 'moweb' THEN cef END) AS cef_moweb,
       SUM(CASE WHEN platform = 'phx' THEN cef END) AS cef_phx,
       SUM(cef) AS cef_all_platforms,
       SUM(CASE WHEN platform = 'desktop' THEN cgp END) AS cgp_desktop,
       SUM(CASE WHEN platform = 'moweb' THEN cgp END) AS cgp_moweb,
       SUM(CASE WHEN platform = 'phx' THEN cgp END) AS cgp_phx,
       SUM(cgp) AS cgp_all_platforms,
       ROUND(SUM(CASE WHEN platform = 'desktop' THEN cef END) - SUM(winnings)*SUM(CASE WHEN platform = 'desktop' THEN cef END) / nullifzero (SUM(cef))::float,2) AS nar_desktop,
       ROUND(SUM(CASE WHEN platform = 'moweb' THEN cef END) - SUM(winnings)*SUM(CASE WHEN platform = 'moweb' THEN cef END) / nullifzero (SUM(cef))::float,2) AS nar_moweb,
       ROUND(SUM(CASE WHEN platform = 'phx' THEN cef END) - SUM(winnings)*SUM(CASE WHEN platform = 'phx' THEN cef END) / nullifzero (SUM(cef))::float,2) AS nar_phx,
       ROUND((SUM(cef) - SUM(winnings))::float,2) AS nar_all_platforms,
       SUM(tef) AS tef,
       SUM(fgp) AS fgp,
       SUM(fgp) + SUM(cgp) AS tgp,
       SUM(pef) AS pef,
       SUM(winnings) AS winnings,
       SUM(deposits) AS deposits,
       SUM(withdrawals) AS withdrawals,
       MAX(reg) AS reg,
       MAX(npp) AS npp,
       MAX(nmp) AS nmp,
       MAX(ftd_date) AS ftd_date,
       MAX(ftd_amount) AS ftd_amount,
       MAX(ftd2_date) AS ftd2_date,
       MAX(ftd2_amount) AS ftd2_amount,
       MAX(pclub_tier) AS pclub_tier
FROM
(
    WITH deposits_all
    AS
    (SELECT user_id,
           trans_date AS deposit_date,
           amount AS amount_deposited,
           ROW_NUMBER() OVER (partition by user_id ORDER BY trans_date) AS rn
    FROM ww.internal_transactions
    WHERE trans_type ilike '%eposit%')

    SELECT a.user_id,
           trans_date::DATE AS event_day,
           CASE
             WHEN device IS NULL THEN 'desktop'
             WHEN platform = 'skill-app' THEN 'phx'
             WHEN (platform NOT IN ('skill-app', 'skill-solrush') or platform is null)
             AND coalesce(name,regexp_substr(a.description, 'Ladder Joining Fee: (.*) Ladder',1,1,'c',1))
             IN ('Pyramid Solitaire','Bejeweled 2','Bejeweled Blitz: Rubies and Riches','Dynomite','Cubis','Luxor','Mahjongg Dimensions','Pyramid Solitaire','SCRABBLE BOGGLE','Solitaire TriPeaks','Spades','Spider Solitaire','SwapIt!','Swipe Hype','Tetris&reg; Burst','TRIVIAL PURSUIT','Vegas Nights 2','Word Mojo','Angry Birds Champions')
             THEN 'desktop'
             WHEN (platform NOT IN ('skill-app', 'skill-solrush') or platform is null) then 'moweb'
             END as platform,
           MAX(CASE WHEN trans_type = 'SIGNUP' AND COALESCE(amount,0) <> 0 THEN 1 ELSE 0 END) AS ump,
           SUM(CASE WHEN trans_type = 'SIGNUP' AND COALESCE(amount,0) <> 0 THEN amount*-1 ELSE 0 END) AS cef,
           COUNT(CASE WHEN trans_type = 'SIGNUP' AND COALESCE(amount,0) <> 0 THEN int_trans_id END) AS cgp,
           SUM(CASE WHEN trans_type = 'SIGNUP' THEN play_money_amount*-1 ELSE 0 END) AS pef,
           SUM(CASE WHEN trans_type = 'SIGNUP' THEN amount + play_money_amount ELSE 0 END)*-1 AS tef,
           SUM(CASE WHEN trans_type = 'WINNINGS' THEN amount ELSE 0 END) AS winnings,
           COUNT(CASE WHEN trans_type = 'SIGNUP' AND COALESCE(play_money_amount,0) <> 0 THEN int_trans_id END) AS fgp,
           SUM(CASE WHEN trans_type ilike '%eposit%' THEN amount ELSE 0 END) AS deposits,
           SUM(CASE WHEN trans_type ilike '%withdraw%' THEN amount ELSE 0 END)*-1 AS withdrawals,
           MAX(e.createdate::DATE) AS reg,
           MAX(e.first_game_played_date::DATE) AS npp,
           MAX(e.real_money_date::DATE) AS nmp,
           MAX(f.deposit_date::DATE) AS ftd_date,
           MAX(f.amount_deposited) AS ftd_amount,
           MAX(i.deposit_date::DATE) AS ftd2_date,
           MAX(i.amount_deposited) AS ftd2_amount,
           MAX(COALESCE(g.pclub_tier,NULL)) AS pclub_tier
    FROM ww.internal_transactions a
      LEFT JOIN ww.clid_combos_ext b ON a.client_id = b.client_id
      LEFT JOIN ww.tournament_games c ON a.product_id = c.tournament_id
      LEFT JOIN ww.dim_gametypes d ON c.gametype_id = d.gametype_id
      LEFT JOIN ww.dim_users e ON a.user_id = e.user_id
      LEFT JOIN deposits_all f
             ON a.user_id = f.user_id
            AND f.rn = 1
            AND f.deposit_date::DATE<= a.trans_date::DATE
      LEFT JOIN deposits_all i
             ON a.user_id = i.user_id
            AND i.rn = 2
            AND i.deposit_date::DATE<= a.trans_date::DATE
      LEFT JOIN (SELECT user_id,
                        MONTH,
                        YEAR,
                        CASE
                          WHEN ((games_played + entry_fees_paid)*100) + ifnull (adjust_points,0) >= 500000 THEN 'Platinum'
                          WHEN ((games_played + entry_fees_paid)*100) + ifnull (adjust_points,0) >= 250000 THEN 'Silver'
                          WHEN ((games_played + entry_fees_paid)*100) + ifnull (adjust_points,0) >= 100000 THEN 'Gold'
                          ELSE 'Member'
                        END AS pclub_tier
                 FROM ww.pclub_games) g
             ON a.user_id = g.user_id
            AND month (date_trunc ('month',a.trans_date)::DATE -1) = g.month
            AND year (date_trunc ('month',a.trans_date)::DATE -1) = g.year
    WHERE trans_date::DATE = date('{{ ds }}')
    AND   (trans_type IN ('SIGNUP','WINNINGS') OR trans_type ilike '%eposit%' OR trans_type ilike '%withdraw%')
    GROUP BY 1,
             2,
             3) a
GROUP BY 1,
         2
ORDER BY event_day,
         user_id;

COMMIT;

--SELECT PURGE_TABLE('ww.user_day');

