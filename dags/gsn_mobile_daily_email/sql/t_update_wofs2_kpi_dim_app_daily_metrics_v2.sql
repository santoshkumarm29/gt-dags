CREATE local temp TABLE temp_dau ON COMMIT PRESERVE ROWS AS
/*+ direct */
(
select /*+ LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_update_wofs2_kpi_dim_app_daily_metrics_v2') */
app, event_day, synthetic_id::varchar(80) as user_id
from gsnmobile.events_dau
where app in ('WoF App')
and event_day between date('{{ ds }}') - cast('{{ params.days_back }}' as int) - 34 and date('{{ ds }}')
and synthetic_id NOT IN (SELECT hold_back_id FROM app_wofs.blacklist)
group by 1,2,3
);


insert  /*+ direct,LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_update_wofs2_kpi_dim_app_daily_metrics_v2')*/ into kpi.dim_app_daily_metrics_historical(event_day,app_name,transactional_bookings,daily_active_users,monthly_active_users,installs,
            ds,aggregation_time,advertising_revenue,payers,day1_retained,day7_retained,day30_retained,day1_installs,day7_installs,day30_installs,update_job)
select
    a.event_day,
    'Wheel of Fortune Slots 2.0' as app_name,
    ifnull(p.bookings,0) as transactional_bookings,
    ifnull(u.daily_active_users,0) as daily_active_users,
    ifnull(m.monthly_active_users,0) as monthly_active_users,
    case when date(a.event_day) >= '2017-12-15' then ifnull(i.installs,0) else 0 end as installs,
  '{{ ds }}' as ds,
  sysdate as aggregation_time,
  ifnull(a2.ad_rev,0) as ad_revenue,
  payers,
  day1_retained,
  day7_retained,
  day30_retained,
  day1_installs,
  day7_installs,
  day30_installs,
  '{{ params.update_job }}' as update_job
from (select thedate as event_day from newapi.dim_date where thedate between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')) a
left join (
select
  event_day,
  count(distinct user_id) as daily_active_users
  from temp_dau
  where event_day between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
  group by 1
) u using(event_day)
left join (
select
  d2.event_day,
  count(distinct user_id) as monthly_active_users
from (select thedate as event_day from newapi.dim_date where thedate between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')) d2
join temp_dau a on a.event_day between d2.event_day - 29 and d2.event_day
group by 1
) m using(event_day)
left join (
    select
      event_day,
      sum(amount_paid_usd) as bookings,
      count(distinct synthetic_id) as payers
    from gsnmobile.events_payments
    where app='WoF App'
    and event_day between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
    and synthetic_id NOT IN (SELECT hold_back_id FROM app_wofs.blacklist)
    group by 1
) p using(event_day)
left join (
    select
      first_seen as event_day,
      count(distinct synthetic_id) as installs
    from gsnmobile.dim_app_installs
    where app='WoF App'
    and synthetic_id NOT IN (SELECT hold_back_id FROM app_wofs.blacklist)
    and first_seen between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
    group by 1
) i using(event_day)
left join (
 select
  event_date::date as event_day,
  sum(revenue) as ad_rev
 from ads.daily_aggregation
 where event_date::date between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
 and vendor <> 'UnityAds'
        and app = 'wof slots'
group by 1
) a2 using(event_day)
left join (
SELECT thedate AS event_day,
       MAX(CASE WHEN install_day + 1 = thedate THEN installs ELSE NULL END) AS day1_installs,
       MAX(CASE WHEN install_day + 7 = thedate THEN installs ELSE NULL END) AS day7_installs,
       MAX(CASE WHEN install_day + 30 = thedate THEN installs ELSE NULL END) AS day30_installs,
       MAX(CASE WHEN install_day + 1 = thedate THEN day1_retained ELSE NULL END) AS day1_retained,
       MAX(CASE WHEN install_day + 7 = thedate THEN day7_retained ELSE NULL END) AS day7_retained,
       MAX(CASE WHEN install_day + 30 = thedate THEN day30_retained ELSE NULL END) AS day30_retained from
(SELECT DATE (first_seen) as install_day,
       COUNT(DISTINCT x.synthetic_id) AS installs,
       COUNT(DISTINCT CASE WHEN DATE (first_seen) + 1 = y.event_day THEN x.synthetic_id ELSE NULL END) AS day1_retained,
       COUNT(DISTINCT CASE WHEN DATE (first_seen) + 7 = y.event_day THEN x.synthetic_id ELSE NULL END) AS day7_retained,
       COUNT(DISTINCT CASE WHEN DATE (first_seen) + 30 = y.event_day THEN x.synthetic_id ELSE NULL END) AS day30_retained
       from gsnmobile.dim_app_installs x
       left outer join temp_dau y
       on x.synthetic_id = y.user_id
       where date(first_seen) between date('{{ ds }}') - cast('{{ params.days_back }}' as int) - 34 and date('{{ ds }}')
       and x.app='WoF App'
       and x.synthetic_id NOT IN (SELECT hold_back_id FROM app_wofs.blacklist)
       group by 1) rr1
       CROSS JOIN newapi.dim_date
WHERE thedate BETWEEN date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
       group by 1
) rr using(event_day)
order by 1 desc;

