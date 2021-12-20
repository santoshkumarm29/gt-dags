select  /*+ LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_validate_phoenix_device_day')*/  1 as valid
from phoenix.device_day
where event_day = '{{ ds }}'
limit 1;