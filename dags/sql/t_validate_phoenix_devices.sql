select  /*+ LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_validate_phoenix_devices')*/  1 as valid
from phoenix.devices
where install_day = '{{ ds }}'
limit 1;