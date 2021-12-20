CREATE local temp TABLE temp_dau ON COMMIT PRESERVE ROWS AS
/*+ direct */
(
select  /*+ LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_update_tripeaks_kpi_dim_app_daily_metrics') */
distinct app, event_day, synthetic_id::varchar(80) as user_id
from gsnmobile.events_dau
where app in ('TriPeaks Solitaire')
and event_day between date('{{ ds }}') - cast('{{ params.days_back }}' as int) - 34 and date('{{ ds }}')
);

insert  /*+ direct,LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_update_tripeaks_kpi_dim_app_daily_metrics')*/ into kpi.dim_app_daily_metrics_historical(event_day,app_name,transactional_bookings,daily_active_users,monthly_active_users,installs,advertising_revenue,
            ds,aggregation_time,payers,day1_retained,day7_retained,day30_retained,day1_installs,day7_installs,day30_installs,update_job)
select
  a.event_day,
  'TriPeaks Solitaire' as app_name,
  ifnull(p.bookings,0) as transactional_bookings,
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
from (select thedate as event_day from newapi.dim_date where thedate between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')) a
left join (
	select
	  event_day,
	  count(distinct user_id) as daily_active_users
	from temp_dau
	where app = 'TriPeaks Solitaire'
	and event_day between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
	group by 1
	order by 1 desc
) u using(event_day)
left join (
	select
	  d2.event_day,
	  count(distinct user_id) as monthly_active_users
	from (select thedate as event_day from newapi.dim_date where thedate between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')) d2
	join temp_dau a on a.event_day between d2.event_day - 29 and d2.event_day and a.app = 'TriPeaks Solitaire'
	group by 1
) m using(event_day)
left join (
    select
      event_day,
      sum(amount_paid_usd) as bookings,
      count(distinct synthetic_id) as payers
    from gsnmobile.events_payments
    where app='TriPeaks Solitaire'
    and event_day between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
    group by 1
) p using(event_day)
left join (
    select
      first_seen as event_day,
      count(distinct synthetic_id) as installs
    from gsnmobile.dim_app_installs
    where app='TriPeaks Solitaire'
    and first_seen between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
    group by 1
) i using(event_day)
left join (
	select
		event_day,
		sum(case when split_part(value_varchar2, '|', 4) = 'Fullscreen' then (case when split_part(value_varchar5,'|',3) = '' or split_part(value_varchar5,'|',3)::float > 2 then 0 else split_part(value_varchar5,'|',3)::float end) else 0 end) +
		sum(case when split_part(value_varchar2, '|', 4) = 'Rewarded Video' then (case when split_part(value_varchar5,'|',3) = '' or split_part(value_varchar5,'|',3)::float > 2 then 0 else split_part(value_varchar5,'|',3)::float end) else 0 end) +
		sum(case when split_part(value_varchar2, '|', 4) = 'Banner' then (case when split_part(value_varchar5,'|',3) = '' then 0 else split_part(value_varchar5,'|',3)::float end) else 0 end) as ad_rev
	from gsnmobile.events_tripeaks a
	where action='mopub_irld'
	and split_part(value_varchar2, '|', 4) in ('Fullscreen','Rewarded Video','Banner')
	and tuid is not null
	and event_day between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
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
       and x.app='TriPeaks Solitaire'
       group by 1) rr1
       CROSS JOIN newapi.dim_date
WHERE thedate BETWEEN date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
       group by 1
) rr using(event_day)
order by 1 desc
;
