select /*+ LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_validate_fdp2_events_dau')*/
case when (count(distinct case when event_day = '{{ ds }}' then user_id else null end ) /
count(distinct case when event_day = '{{ yesterday_ds }}' then user_id else null end ) >= 0.9)
and count(distinct case when event_day = '{{ tomorrow_ds }}' then user_id else null end ) > 1
then 1 else 0 end as valid
from blackdeck.events_dau
where event_day between '{{ yesterday_ds }}' and '{{ tomorrow_ds }}'
;