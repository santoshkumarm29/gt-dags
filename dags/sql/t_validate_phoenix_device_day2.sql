select  /*+ LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_validate_phoenix_device_day')*/  1 as valid
from phoenix.device_day
where event_day = '2021-10-15'
limit 1;