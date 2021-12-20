select /*+ LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_validate_gsncasino2_users')*/
1 as valid
from gsncasino.users
where date(etl_insert_time at timezone 'America/Los_Angeles') = '{{ tomorrow_ds }}'
limit 1;