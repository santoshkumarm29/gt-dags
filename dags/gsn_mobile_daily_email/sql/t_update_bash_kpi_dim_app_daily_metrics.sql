CREATE local temp TABLE temp_dau ON COMMIT PRESERVE ROWS DIRECT AS
select /*+ LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_update_bash_kpi_dim_app_daily_metrics')*/
        distinct case when device_platform in ('pc','windowst','windowsp') then 'Bingo Bash Canvas'
             when device_platform = 'gsn_pc' then 'G2 - Bash'
             else 'Bingo Bash Mobile' end as app,
        active_on::date as event_day,
        coalesce(first_bingo_user_id, a.user_id) as user_id
from bash.bingo_dau a
left join bingo.user_id_mapping b on a.user_id = b.user_id
where active_on::date between date('{{ ds }}')- cast('{{ params.days_back }}' as int) - 34 and date('{{ ds }}')
;

CREATE local temp TABLE temp_installs ON COMMIT PRESERVE ROWS DIRECT AS
select /*+ LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_update_bash_kpi_dim_app_daily_metrics')*/  app, first_user_id as user_id, event_day
from
(
        SELECT CASE WHEN device_platform in ('pc','windowst', 'windowsp') THEN 'Bingo Bash Canvas'
                    WHEN device_platform = 'gsn_pc' then 'G2 - Bash'
                    ELSE 'Bingo Bash Mobile'
                     END AS app,
               coalesce(first_bingo_user_id, a.user_id) as first_user_id,
               active_on AS event_day,
               row_number() over(partition by coalesce(first_bingo_user_id, a.user_id) order by active_on) as rn
        FROM bash.bingo_dau a
        left join bingo.user_id_mapping b on a.user_id = b.user_id
) c
where event_day between date('{{ ds }}') - cast('{{ params.days_back }}' as int) - 34 and date('{{ ds }}')
and rn = 1
;

insert /*+ direct, LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_update_bash_kpi_dim_app_daily_metrics')*/
into kpi.dim_app_daily_metrics_historical(event_day,app_name,transactional_bookings,daily_active_users,monthly_active_users,installs,ds,aggregation_time,
            advertising_revenue,payers,day1_retained,day7_retained,day30_retained,day1_installs,day7_installs,day30_installs,update_job)
select
    d.event_day,
    d.app_name,
    ifnull(r.transactional_bookings,0) as transactional_bookings,
    ifnull(u.daily_active_users,0) as daily_active_users,
    ifnull(m.monthly_active_users,0) as monthly_active_users,
    ifnull(i.installs,0) as installs,
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
from
(
        select 'Bingo Bash Canvas' as app_name, thedate as event_day from newapi.dim_date where thedate between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
        UNION
        select 'Bingo Bash Mobile', thedate as event_day from newapi.dim_date where thedate between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
        UNION
        select 'G2 - Bash', thedate as event_day from newapi.dim_date where thedate between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
) d
left join
(
    select
         CASE
         WHEN device in ('pc', 'windowst','windowsp') THEN 'Bingo Bash Canvas'
         WHEN device IN ('kindle','kindlep','androidp','androidap','androidt','androidat','iphone','ipad') THEN 'Bingo Bash Mobile'
         WHEN device = 'gsn_pc' THEN 'G2 - Bash'
         ELSE 'Bingo Bash Other'
         END AS app_name,
         date(created_at) as event_day,
         SUM(fbcredits) / 10 as transactional_bookings,
         count(distinct user_id) as payers
        FROM bash.b_fbc_orders
        WHERE status = 'settled'
        AND   IFNULL (item_id,'') != '-1'
        AND   fbcredits > 0
        AND   device IN ('kindle','kindlep','androidp','androidap','androidt','androidat','iphone','ipad','pc','windowst','gsn_pc','windowsp')
        AND   date(created_at) between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
        group by 1,2
) r using(event_day, app_name)
left join   (
	select event_day,
      app as app_name,
      count(distinct user_id) as installs
    from temp_installs
    where date(event_day) between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
    group by 1,2
) i using(event_day, app_name)
left join (
	select
	  app as app_name,
	  event_day,
	  count(distinct user_id) as daily_active_users
	from temp_dau
	where app in ('Bingo Bash Canvas', 'Bingo Bash Mobile', 'G2 - Bash')
	and event_day between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
	group by 1,2
	--order by 1 desc
) u using(event_day, app_name)
left join (
	select
	  a.app as app_name,
	  d2.event_day,
	  count(distinct user_id) as monthly_active_users
	from (select thedate as event_day from newapi.dim_date where thedate between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')) d2
	join temp_dau a on a.event_day between d2.event_day - 29 and d2.event_day and a.app in ('Bingo Bash Canvas', 'Bingo Bash Mobile','G2 - Bash')
	group by 1,2
) m using(event_day, app_name)
left join (
	select
	  'G2 - Bash' as app_name,
	  event_date::date as event_day,
	  sum(revenue) as ad_rev
	from ads.daily_aggregation
	where event_date::date between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
	and vendor <> 'UnityAds'
        and app = 'g2 - bash'
	group by 1,2

	UNION ALL

	select
	  'Bingo Bash Mobile' as app_name,
	  event_day_pst as event_day,
	  sum(publisher_revenue) as ad_rev
	from bingo.events_other_data_json_v
	where event_day_pst between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
	group by 1,2
) a2 using(event_day, app_name)
left join (
SELECT thedate AS event_day,
       app as app_name,
       MAX(CASE WHEN install_day + 1 = thedate THEN installs ELSE NULL END) AS day1_installs,
       MAX(CASE WHEN install_day + 7 = thedate THEN installs ELSE NULL END) AS day7_installs,
       MAX(CASE WHEN install_day + 30 = thedate THEN installs ELSE NULL END) AS day30_installs,
       MAX(CASE WHEN install_day + 1 = thedate THEN day1_retained ELSE NULL END) AS day1_retained,
       MAX(CASE WHEN install_day + 7 = thedate THEN day7_retained ELSE NULL END) AS day7_retained,
       MAX(CASE WHEN install_day + 30 = thedate THEN day30_retained ELSE NULL END) AS day30_retained from
(SELECT x.event_day as install_day,
       x.app,
       COUNT(DISTINCT x.user_id) AS installs,
       COUNT(DISTINCT CASE WHEN x.event_day + 1 = y.event_day THEN x.user_id ELSE NULL END) AS day1_retained,
       COUNT(DISTINCT CASE WHEN x.event_day + 7 = y.event_day THEN x.user_id ELSE NULL END) AS day7_retained,
       COUNT(DISTINCT CASE WHEN x.event_day + 30 = y.event_day THEN x.user_id ELSE NULL END) AS day30_retained
       from temp_installs x
       left outer join temp_dau y
       on x.user_id = y.user_id
       and x.app = y.app
       group by 1,2) rr1
       CROSS JOIN newapi.dim_date
       where thedate BETWEEN date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
       group by 1,2
) rr using(event_day, app_name)
order by 1 desc,2
;
