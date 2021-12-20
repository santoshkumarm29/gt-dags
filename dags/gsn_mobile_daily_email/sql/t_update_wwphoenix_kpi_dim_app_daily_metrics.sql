insert /*+ direct,LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_update_wwphoenix_kpi_dim_app_daily_metrics')*/  into kpi.dim_app_daily_metrics_historical(event_day,app_name,transactional_bookings,daily_active_users,monthly_active_users,installs,advertising_revenue,
            ds,aggregation_time,payers,day1_retained,day7_retained,day30_retained,day1_installs,day7_installs,day30_installs,update_job)
select
  d.event_day,
  'WorldWinner App' as app_name,
  ifnull(r.transactional_bookings,0) as transactional_bookings,
  ifnull(u.daily_active_users,0) as daily_active_users,
  ifnull(m.monthly_active_users,0) as monthly_active_users,
  ifnull(i.installs,0) as installs,
  0 as ad_revenue,
  '{{ ds }}' as ds,
  sysdate as aggregation_time,
  payers,
  day1_retained,
  day7_retained,
  day30_retained,
  day1_installs,
  day7_installs,
  day30_installs,
  '{{ params.update_job }}' as update_job
from (select thedate as event_day from newapi.dim_date where thedate between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')) d
left join (
        SELECT DATE (trans_date) AS event_day,
           SUM(CASE WHEN device IS NOT NULL AND trans_type = 'SIGNUP' AND platform in ('skill-app', 'skill-solrush') THEN amount*- 1 ELSE 0 END)*((SUM(CASE WHEN trans_type = 'SIGNUP' THEN amount*- 1 ELSE 0 END)) -(SUM(CASE WHEN trans_type = 'WINNINGS' THEN amount ELSE 0 END))) / SUM(CASE WHEN trans_type = 'SIGNUP' THEN amount*- 1 ELSE 0 END) AS transactional_bookings,
           count(distinct(case WHEN device IS NOT NULL AND trans_type = 'SIGNUP' AND platform in ('skill-app', 'skill-solrush') then user_id else null end)) as payers
        FROM ww.internal_transactions a
        LEFT JOIN ww.clid_combos_ext b ON a.client_id = b.client_id
        WHERE DATE (trans_date) between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
        GROUP BY 1
        ORDER BY 1 DESC
) r using(event_day)
left join (
        select
          event_day,
          count(distinct idfv) as daily_active_users
        from (select event_day, idfv from phoenix.device_day
        union select event_day, idfv from phoenix.solrush_device_day) x
        where event_day between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
        group by 1
        order by 1 desc
) u using(event_day)
left join (
        select
          d2.event_day,
          count(distinct idfv) as monthly_active_users
        from (select thedate as event_day from newapi.dim_date where thedate between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')) d2
        join (select event_day, idfv from phoenix.device_day
        union select event_day, idfv from phoenix.solrush_device_day) a on a.event_day between d2.event_day - 29 and d2.event_day
        group by 1
) m using(event_day)
left join (
    select
      install_day as event_day,
      count(distinct idfv) as installs
    from (select install_day, idfv from phoenix.devices
        union select install_day, idfv from phoenix.solrush_devices) x
    where install_day between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
    group by 1
    order by 1
) i using(event_day)
left join (
SELECT thedate AS event_day,
       app,
       MAX(CASE WHEN install_day + 1 = thedate THEN installs ELSE NULL END) AS day1_installs,
       MAX(CASE WHEN install_day + 7 = thedate THEN installs ELSE NULL END) AS day7_installs,
       MAX(CASE WHEN install_day + 30 = thedate THEN installs ELSE NULL END) AS day30_installs,
       MAX(CASE WHEN install_day + 1 = thedate THEN day1_retained ELSE NULL END) AS day1_retained,
       MAX(CASE WHEN install_day + 7 = thedate THEN day7_retained ELSE NULL END) AS day7_retained,
       MAX(CASE WHEN install_day + 30 = thedate THEN day30_retained ELSE NULL END) AS day30_retained
FROM
(
SELECT x.event_day AS install_day,
       x.app,
       COUNT(DISTINCT x.user_id) AS installs,
       COUNT(DISTINCT CASE WHEN x.event_day + 1 = y.event_day THEN x.user_id ELSE NULL END) AS day1_retained,
       COUNT(DISTINCT CASE WHEN x.event_day + 7 = y.event_day THEN x.user_id ELSE NULL END) AS day7_retained,
       COUNT(DISTINCT CASE WHEN x.event_day + 30 = y.event_day THEN x.user_id ELSE NULL END) AS day30_retained
FROM (SELECT 'WorldWinner App' AS app,
             idfv AS user_id,
             DATE (install_day) AS event_day
      FROM (select install_day, idfv from phoenix.devices
        union select install_day, idfv from phoenix.solrush_devices) x
      WHERE DATE (install_day) between date('{{ ds }}') - cast('{{ params.days_back }}' as int) - 34 and date('{{ ds }}')) x
  LEFT OUTER JOIN (SELECT 'WorldWinner App' AS app,
                          idfv AS user_id,
                          DATE (event_day) AS event_day
                   FROM (select event_day, idfv from phoenix.device_day
                          union select event_day, idfv from phoenix.solrush_device_day) x
                   WHERE DATE (event_day) between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
                   GROUP BY 1,
                            2,
                            3) y ON x.user_id = y.user_id
GROUP BY 1,
         2) rr1
CROSS JOIN newapi.dim_date
WHERE thedate BETWEEN date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
GROUP BY 1,
         2
) rr using(event_day)
order by 1 desc
;