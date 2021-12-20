SELECT /*+ LABEL ('{label}')*/
   createdate,
   a.user_id,
   a.username,
   advertiser_id,
   c.mkt_partnertag ,
   count(
   CASE
             WHEN trans_type ilike '%eposit%' THEN amount
             ELSE NULL
   END) AS deposits ,
   sum(
   CASE
             WHEN trans_type ilike '%eposit%' THEN amount
             ELSE 0
   END) AS deposit_amt ,
   user_banned_date ,
   email ,
   ww_rmd
FROM ww.dim_users a
LEFT JOIN gsncom.dim_user_platform_id b
    ON a.user_id=b.ww_id
LEFT JOIN gsncom.dim_user_mkt_partnertag c
    ON b.gsn_id=c.user_id
LEFT JOIN ww.internal_transactions d
    ON a.user_id=d.user_id
WHERE a.user_id IN
    (
    SELECT DISTINCT user_id
    FROM (
        SELECT    a.user_id,
                  count(
                  CASE
                            WHEN datediff('d',date(real_money_date), date(start_time)) BETWEEN 0 AND       10 THEN game_id
                            ELSE NULL
                  END)
        FROM      ww.dim_users a
        LEFT JOIN ww.played_games b
        ON        a.user_id=b.user_id
        WHERE     date(real_money_date)=DATE('{today}')-11
        GROUP BY  1 )a
    WHERE           count=0 )
    AND date(trans_date)=DATE('{today}')-11
GROUP BY 1, 2, 3, 4, 5, 8, 9, 10;
