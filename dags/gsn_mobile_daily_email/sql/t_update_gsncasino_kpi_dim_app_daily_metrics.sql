CREATE local temp TABLE temp_dau ON COMMIT PRESERVE ROWS AS
/*+ direct,LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_update_gsncasino_kpi_dim_app_daily_metrics')*/
(
select
distinct app, event_day, case when app = 'GSN Casino' then synthetic_id else user_id end as user_id
from gsnmobile.events_dau
where app in ('GSN Casino', 'GSN Casino 2')
and event_day between date('{{ ds }}') - cast('{{ params.days_back }}' as int) - 34 and date('{{ ds }}')
);

CREATE local temp TABLE temp_dad_c2 ON COMMIT PRESERVE ROWS AS
/*+ direct,LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_update_gsncasino_kpi_dim_app_daily_metrics')*/
(
select
distinct app, event_day, synthetic_id as user_id
from gsnmobile.events_dau
where app in ('GSN Casino 2')
and event_day between date('{{ ds }}') - cast('{{ params.days_back }}' as int) - 34 and date('{{ ds }}')
);


CREATE local temp TABLE temp_installs ON COMMIT PRESERVE ROWS AS
/*+ direct,LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_update_gsncasino_kpi_dim_app_daily_metrics')*/
(
SELECT
        'GSN Casino 2' as app,
        date(created_at at timezone 'America/Los_Angeles') as event_day,
        x.user_id
      FROM gsncasino.users x
      left join gsncasino.first_user_platform y
      using(user_id)
      WHERE timestampadd('mi', -5, created_at) <= first_created_at -- filters mesmo users migrated from c1
      AND   date(created_at at timezone 'America/Los_Angeles') between date('{{ ds }}') - cast('{{ params.days_back }}' as int) - 34
      and date('{{ ds }}')
      and nvl(y.platform, 'null') <> 'web'
      union
      select
      distinct 'GSN Casino' as app,
      date(first_seen) as event_day,
      synthetic_id as user_id
    from gsnmobile.dim_app_installs
    where app='GSN Casino'
    and first_seen between date('{{ ds }}') - cast('{{ params.days_back }}' as int) - 34 and date('{{ ds }}')
);

insert  /*+ direct,LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_update_gsncasino_kpi_dim_app_daily_metrics')*/ into kpi.dim_app_daily_metrics_historical(event_day,app_name,transactional_bookings,daily_active_users,monthly_active_users,installs,advertising_revenue,
            ds,aggregation_time,payers,day1_retained,day7_retained,day30_retained,day1_installs,day7_installs,day30_installs,update_job)
select
  d.event_day,
  'GSN Casino' as app_name,
  ifnull(p.bookings,0) as transactional_bookings,
  ifnull(u.daily_active_users,0) as daily_active_users,
  ifnull(m.monthly_active_users,0) as monthly_active_users,
  ifnull(i.installs,0) as installs,
  ifnull(a.ad_rev,0) as ad_revenue,
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
    select
      event_day,
      sum(amount_paid_usd) as bookings,
      count(distinct user_id) as payers
    from (select * from gsnmobile.events_payments
    where app in ('GSN Casino 2','GSN Casino')
    and event_day between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
    union all
    select * from gsncasino_staging.events_payments_null_synthetic_id
    where app='GSN Casino 2'
    and event_day between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')) x
    group by 1
) p using(event_day)
left join (
	select
	  event_day,
	  count(distinct user_id) as daily_active_users
	from temp_dau
	where event_day between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
	group by 1
	order by 1 desc
) u using(event_day)
left join (
	select
	  d2.event_day,
	  count(distinct user_id) as monthly_active_users
	from (select thedate as event_day from newapi.dim_date where thedate between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')) d2
	join temp_dau a on a.event_day between d2.event_day - 29 and d2.event_day
	group by 1
) m using(event_day)
left join (select
      event_day,
      count(distinct user_id) as installs
    from temp_installs
    where event_day between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
    group by 1
) i using(event_day)
left join (
	select
	  event_date::date as event_day,
	  sum(revenue) as ad_rev
	from ads.daily_aggregation
	where event_date::date between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
	and vendor <> 'UnityAds'
        and app = 'gsn casino'
        and platform <> 'facebook'
	group by 1
	--order by 1 desc
) a using(event_day)
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
(SELECT x.event_day AS install_day,
       'GSN Casino' AS app,
       COUNT(DISTINCT x.user_id) AS installs,
       COUNT(DISTINCT CASE WHEN x.event_day + 1 = y.event_day THEN x.user_id ELSE NULL END) AS day1_retained,
       COUNT(DISTINCT CASE WHEN x.event_day + 7 = y.event_day THEN x.user_id ELSE NULL END) AS day7_retained,
       COUNT(DISTINCT CASE WHEN x.event_day + 30 = y.event_day THEN x.user_id ELSE NULL END) AS day30_retained
FROM temp_installs x
left outer join (select * from temp_dau union select * from temp_dad_c2) y
on x.user_id = y.user_id
group by 1, 2) rr1
CROSS JOIN newapi.dim_date
WHERE thedate BETWEEN date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
GROUP BY 1,
         2
) rr using(event_day)
order by 1 desc
;

