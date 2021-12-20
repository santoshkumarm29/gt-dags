select /*+ LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_validate_bash_fbc_orders')*/
case when sum(case when (app_name in ('Bingo Bash Canvas','Bingo Bash Mobile') and hr_ct = 24)
 or (app_name = 'G2 - Bash' and hr_ct >= 20) then 1
 else 0 end) = 3 then 1 else 0 end as valid
from
(select
         CASE
         WHEN device in ('pc', 'windowst','windowsp') THEN 'Bingo Bash Canvas'
         WHEN device IN ('kindle','kindlep','androidp','androidap','androidt','androidat','iphone','ipad') THEN 'Bingo Bash Mobile'
         WHEN device = 'gsn_pc' THEN 'G2 - Bash'
         ELSE 'Bingo Bash Other'
         END AS app_name,
         count(distinct date_trunc('hour',created_at)) as hr_ct        FROM bash.b_fbc_orders
        WHERE status = 'settled'
        AND   IFNULL (item_id,'') != '-1'
        AND   fbcredits > 0
        AND   device IN ('kindle','kindlep','androidp','androidap','androidt','androidat','iphone','ipad','pc','windowst','gsn_pc','windowsp')
        AND   date(created_at) = date('{{ ds }}')
        group by 1) x;