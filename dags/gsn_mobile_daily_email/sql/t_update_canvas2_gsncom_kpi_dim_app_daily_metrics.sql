CREATE local temp TABLE temp_dau ON COMMIT PRESERVE ROWS AS
/*+ direct, LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_update_canvas2_gsncom_kpi_dim_app_daily_metrics')*/
(
select
distinct
case when platform = 'web' then 'GSN Canvas'
 when platform = 'gsncom' then 'G2 - Casino'
 end as app_name,
 event_day_pst as event_day,
 e.user_id
FROM  gsncasino.r_events e
left join gsncasino.users using(user_id)
where platform in ('web', 'gsncom')
	and event_group='STARTUP'
	and ifnull(test_user, false) is false
	and event_day_pst between date('{{ ds }}') - cast('{{ params.days_back }}' as int) - 34 and date('{{ ds }}')
);

CREATE local temp TABLE temp_installs ON COMMIT PRESERVE ROWS AS
/*+ direct,LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_update_canvas2_gsncom_kpi_dim_app_daily_metrics')*/
(
SELECT
        case when y.platform = 'web' then 'GSN Canvas'
         when y.platform = 'gsncom' then 'G2 - Casino' end as app_name,
        date(created_at at timezone 'America/Los_Angeles') as event_day,
        x.user_id
      FROM gsncasino.users x
      left join gsncasino.first_user_platform y
      using(user_id)
      WHERE timestampadd('mi', -5, created_at) <= first_created_at -- filters mesmo users migrated from c1
      AND   date(created_at at timezone 'America/Los_Angeles') between date('{{ ds }}') - cast('{{ params.days_back }}' as int) - 34
      and date('{{ ds }}')
      and nvl(y.platform, 'null') in ('web', 'gsncom')
);



insert /*+ direct, LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_update_canvas2_gsncom_kpi_dim_app_daily_metrics')*/
into kpi.dim_app_daily_metrics_historical(event_day,app_name,transactional_bookings,daily_active_users,monthly_active_users,installs,
            advertising_revenue,ds,aggregation_time,payers,day1_retained,day7_retained,day30_retained,day1_installs,day7_installs,day30_installs,update_job)
select
  d.event_day,
  d.app_name,
  ifnull(cr.transactional_bookings,0) as transactional_bookings,
  ifnull(u.daily_active_users,0) as daily_active_users,
  ifnull(m.monthly_active_users,0) as monthly_active_users,
  ifnull(i.installs,0) as installs,
  ifnull(a.ad_rev,0) as advertising_revenue,
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
from (
        select 'GSN Canvas' as app_name, thedate as event_day from newapi.dim_date where thedate between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
        UNION
        select 'G2 - Casino', thedate as event_day from newapi.dim_date where thedate between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
) d
left join (
	select
	case when platform = 'web' then 'GSN Canvas'
	 when platform = 'gsncom' then 'G2 - Casino'
	  end as app_name,
	  event_day_pst as event_day,
sum(amount_usd) as transactional_bookings,
count(distinct t.user_id) as payers
from gsncasino.transactions t
left join gsncasino.users using(user_id)
where platform in ('web','gsncom')
	and status='COMPLETED'
	and ifnull(test_user, false) is false
	and event_day_pst between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
group by 1,2
order by 1,2
) cr using(app_name, event_day)
left join (
	select
	  app_name,
	  event_day,
	  count(distinct user_id) as daily_active_users
	from temp_dau
	where event_day between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
	group by 1,2
	order by 1,2
) u using(app_name, event_day)
left join (
	select
	  a.app_name,
	  d2.event_day,
	  count(distinct user_id) as monthly_active_users
	from (select thedate as event_day from newapi.dim_date where thedate between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')) d2
	join temp_dau a on a.event_day between d2.event_day - 29 and d2.event_day
	group by 1,2
	order by 1,2
) m using(app_name, event_day)
left join (select
      app_name,
      event_day,
      count(distinct user_id) as installs
    from temp_installs
    where event_day between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
    group by 1,2
    order by 1,2
) i using(app_name, event_day)
left join (
	select
	  case when app = 'gsn casino' then 'GSN Canvas'
	   else 'G2 - Casino' end as app_name,
	  event_date::date as event_day,
	  sum(revenue) as ad_rev
	from ads.daily_aggregation
	where event_date::date between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
	and vendor <> 'UnityAds'
        and ((app = 'gsn casino'
        and platform = 'facebook') or app = 'g2 - casino')
	group by 1,2
	order by 1,2
) a using(app_name, event_day)
left join (
SELECT thedate AS event_day,
       app_name,
       MAX(CASE WHEN install_day + 1 = thedate THEN installs ELSE NULL END) AS day1_installs,
       MAX(CASE WHEN install_day + 7 = thedate THEN installs ELSE NULL END) AS day7_installs,
       MAX(CASE WHEN install_day + 30 = thedate THEN installs ELSE NULL END) AS day30_installs,
       MAX(CASE WHEN install_day + 1 = thedate THEN day1_retained ELSE NULL END) AS day1_retained,
       MAX(CASE WHEN install_day + 7 = thedate THEN day7_retained ELSE NULL END) AS day7_retained,
       MAX(CASE WHEN install_day + 30 = thedate THEN day30_retained ELSE NULL END) AS day30_retained
FROM
(SELECT x.event_day AS install_day,
        x.app_name,
       COUNT(DISTINCT x.user_id) AS installs,
       COUNT(DISTINCT CASE WHEN x.event_day + 1 = y.event_day THEN x.user_id ELSE NULL END) AS day1_retained,
       COUNT(DISTINCT CASE WHEN x.event_day + 7 = y.event_day THEN x.user_id ELSE NULL END) AS day7_retained,
       COUNT(DISTINCT CASE WHEN x.event_day + 30 = y.event_day THEN x.user_id ELSE NULL END) AS day30_retained
FROM temp_installs x
left outer join temp_dau y
on x.user_id = y.user_id
and x.app_name = y.app_name
group by 1,2) rr1
CROSS JOIN newapi.dim_date
WHERE thedate BETWEEN date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
GROUP BY 1,2) rr using(app_name, event_day)
order by 1,2
;

