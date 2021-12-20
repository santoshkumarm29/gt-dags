CREATE local temp TABLE temp_dau ON COMMIT PRESERVE ROWS DIRECT AS
/*+ direct */
(
select /*+ LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_update_wwdesktop_kpi_dim_app_daily_metrics') */
distinct 'WorldWinner Web' as app,
activity_date::date as event_day,
user_id::varchar(80) as user_id
 from ww.login_activity
where date(activity_date) between date('{{ ds }}') - cast('{{ params.days_back }}' as int) - 34
and date('{{ ds }}')
and (platform_id = 1 or platform_id = 2)
);

CREATE local temp TABLE temp_installs ON COMMIT PRESERVE ROWS DIRECT AS
/*+ direct */
(
SELECT /*+ LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_update_wwdesktop_kpi_dim_app_daily_metrics') */
'WorldWinner Web' AS app,
       a.user_id::VARCHAR(80) as user_id,
       DATE (createdate) AS event_day
FROM ww.dim_users a
  LEFT JOIN (SELECT DISTINCT user_id
             FROM (SELECT a.user_id,
                          ww_cd,
                          wwapp_min_event,
                          CASE
                            WHEN wwapp_min_event BETWEEN ww_cd AND timestampadd ('minute',1,ww_cd) THEN 'new'
                            ELSE 'legacy'
                          END AS tenure
                   FROM (SELECT a.user_id,
                                min(createdate) AS ww_cd,
                                min(wwapp_min_event) as wwapp_min_event
                         FROM
                         (select user_id, MIN(timestampadd ('hour',3,event_time)) as wwapp_min_event
                          from
                          (select id as user_id, event_time from phoenix.dim_device_mapping
                          where id_type = 'user_id') a
                          group by 1) a
                           LEFT JOIN ww.dim_users b ON a.user_id = b.user_id
                         GROUP BY 1) a) a
             WHERE tenure = 'new') x ON x.user_id = a.user_id
WHERE DATE (createdate) between date('{{ ds }}') - cast('{{ params.days_back }}' as int) - 34 and date('{{ ds }}')
AND   x.user_id IS NULL
GROUP BY 1,
         2,
         3
);

insert  /*+ direct,LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_update_wwdesktop_kpi_dim_app_daily_metrics')*/
into kpi.dim_app_daily_metrics_historical(event_day,app_name,transactional_bookings,daily_active_users,monthly_active_users,installs,advertising_revenue,
            ds,aggregation_time,payers,day1_retained,day7_retained,day30_retained,day1_installs,day7_installs,day30_installs,update_job)
select
  d.event_day,
  'WorldWinner Web' as app_name,
  ifnull(r.transactional_bookings,0) as transactional_bookings,
  ifnull(u.daily_active_users,0) as daily_active_users,
  ifnull(m.monthly_active_users,0) as monthly_active_users,
  ifnull(i.installs,0) as installs,
  ifnull(a2.ad_rev,0) as ad_revenue,
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
           SUM(CASE WHEN trans_type = 'SIGNUP' and ((platform not in('skill-app','skill-solrush'))
           or (platform is null)) then amount*- 1 ELSE 0 END)
           *((SUM(CASE WHEN trans_type = 'SIGNUP' THEN amount*- 1 ELSE 0 END))
            -(SUM(CASE WHEN trans_type = 'WINNINGS' THEN amount ELSE 0 END)))
            / SUM(CASE WHEN trans_type = 'SIGNUP' THEN amount*- 1 ELSE 0 END) AS transactional_bookings,
           COUNT(DISTINCT CASE WHEN device IS NULL AND trans_type='SIGNUP' THEN user_id else null END)
           + COUNT(DISTINCT CASE WHEN device IS NOT NULL AND trans_type = 'SIGNUP'
                   AND (platform not in('skill-app','skill-solrush') OR platform IS NULL) THEN user_id ELSE NULL END) as payers
        FROM ww.internal_transactions a
        LEFT JOIN ww.clid_combos_ext b ON a.client_id = b.client_id
        WHERE DATE (trans_date) between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
        GROUP BY 1
        ORDER BY 1 DESC
) r using(event_day)
left join (
        select
          event_day,
          count(distinct user_id) as daily_active_users
        from temp_dau
        where app = 'WorldWinner Web'
        and event_day between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
        group by 1
        order by 1 desc
) u using(event_day)
left join (
        select
          d2.event_day,
          count(distinct user_id) as monthly_active_users
        from (select thedate as event_day from newapi.dim_date where thedate between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')) d2
        join temp_dau a on a.event_day between d2.event_day - 29 and d2.event_day and a.app = 'WorldWinner Web'
        group by 1
) m using(event_day)
left join (
    select
      event_day,
      count(distinct user_id) as installs
    from temp_installs
    group by 1
) i using(event_day)
left join (
        select
          event_date::date as event_day,
          sum(revenue) as ad_rev
        from ads.daily_aggregation
        where event_date::date between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
        and vendor <> 'UnityAds'
        and app = 'worldwinner.com'
        group by 1
        --order by 1 desc
) a2 using(event_day)
left join
(SELECT thedate AS event_day,
       app,
       MAX(CASE WHEN install_day + 1 = thedate THEN installs ELSE NULL END) AS day1_installs,
       MAX(CASE WHEN install_day + 7 = thedate THEN installs ELSE NULL END) AS day7_installs,
       MAX(CASE WHEN install_day + 30 = thedate THEN installs ELSE NULL END) AS day30_installs,
       MAX(CASE WHEN install_day + 1 = thedate THEN day1_retained ELSE NULL END) AS day1_retained,
       MAX(CASE WHEN install_day + 7 = thedate THEN day7_retained ELSE NULL END) AS day7_retained,
       MAX(CASE WHEN install_day + 30 = thedate THEN day30_retained ELSE NULL END) AS day30_retained
FROM
(SELECT
x.event_day AS install_day,
       x.app,
       COUNT(DISTINCT x.user_id) AS installs,
       COUNT(DISTINCT CASE WHEN x.event_day + 1 = y.event_day THEN x.user_id ELSE NULL END) AS day1_retained,
       COUNT(DISTINCT CASE WHEN x.event_day + 7 = y.event_day THEN x.user_id ELSE NULL END) AS day7_retained,
       COUNT(DISTINCT CASE WHEN x.event_day + 30 = y.event_day THEN x.user_id ELSE NULL END) AS day30_retained
FROM temp_installs x
left join temp_dau y
on x.user_id = y.user_id
group by 1,2
) rr1
  CROSS JOIN newapi.dim_date
WHERE thedate BETWEEN date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
GROUP BY 1,
         2
) rr using(event_day)
order by 1 desc
;