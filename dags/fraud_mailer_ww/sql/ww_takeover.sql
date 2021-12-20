SELECT /*+ LABEL ('{label}')*/
    a.user_id
    ,b.username
    ,create_date
    ,'' AS password
    ,email
    ,to_char(thirty_day_avg, '$999,990.00') AS thirty_day_avg
    ,to_char(yday, '$999,990.00') AS yday
    ,to_char((yday / thirty_day_avg) * 100, '99990.99%') AS '% increase'
    ,to_char(yday - thirty_day_avg, '$999,990.00') AS '$ diff'
    ,real_money_date AS rmd
    ,a.advertiser_id
FROM (
    SELECT a.user_id
           ,DATE (create_date) AS create_date
           ,advertiser_id
           ,thirty_day_avg
           ,yday
    FROM (
           SELECT c.user_id
                   ,createdate AS create_date
                   ,advertiser_id
                   ,avg(amount) AS thirty_day_avg
           FROM (
                   SELECT user_id
                           ,DATE (trans_date)
                           ,sum(amount) AS amount
                   FROM ww.internal_transactions
                   WHERE trans_type ilike '%eposi%'
                           AND DATE (trans_date) BETWEEN (DATE('{today}') - 3)
                                  AND (DATE('{today}') - 2)
                   GROUP BY 1
                           ,2
                   ) c
           INNER JOIN ww.dim_users d ON c.user_id = d.user_id
           WHERE datediff('d', DATE (createdate), DATE('{today}')) > 30
           GROUP BY 1
                   ,2
                   ,3
           ) a
    INNER JOIN (
           SELECT DISTINCT user_id
                   ,sum(amount) AS yday
           FROM ww.internal_transactions
           WHERE trans_type ilike '%eposi%'
                   AND DATE (trans_date) = (DATE('{today}') - 1)
           GROUP BY 1
            ) b ON a.user_id = b.user_id
    ) a
LEFT JOIN ww.dim_users b ON a.user_id = b.user_id
LEFT JOIN ww.dim_user_aux c ON a.user_id = c.user_id
WHERE (
       yday > thirty_day_avg + 500
       AND yday > thirty_day_avg * 2
       )
AND (
       (real_money_date > (DATE('{today}') - 180))
       OR (
               (real_money_date < (DATE('{today}') - 180))
               AND (total_deposits <= 15000)
               )
       );
