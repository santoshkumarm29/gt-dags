insert  /*+ direct,LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_update_gsncom_ads_kpi_dim_app_daily_metrics')*/ into
kpi.dim_app_daily_metrics_historical(event_day,app_name,advertising_revenue,
            ds,aggregation_time,update_job)
select
    d.event_day,
    'G2 - Ads' as app_name,
    ifnull(a.advertising_revenue,0) as advertising_revenue,
  '{{ ds }}' as ds,
  sysdate as aggregation_time,
  '{{ params.update_job }}' as update_job
from (select thedate as event_day from newapi.dim_date where thedate between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')) d
join (
	select event_date as event_day, sum(revenue) as advertising_revenue
from ads.daily_aggregation
where event_date between date('{{ ds }}') - cast('{{ params.days_back }}' as int) and date('{{ ds }}')
and app = 'gsn.com'
and platform='web'
group by 1
) a using(event_day);