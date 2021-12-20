select  /*+ LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_validate_ads_aggregation')*/
case when
    sum(case when event_date::date = '{{ ds }}' then revenue else 0 end) /
    sum(case when event_date::date = '{{ yesterday_ds }}' then revenue else 0 end) > 0.4
    then 1 else 0 end as valid
from ads.daily_aggregation
where event_date::date between '{{ yesterday_ds }}' and '{{ ds }}'
and vendor <> 'UnityAds';

