select /*+ LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_validate_fdp2_events_payments')*/
1 as valid
from blackdeck.events_payments
where event_day='{{ tomorrow_ds }}'
limit 1;