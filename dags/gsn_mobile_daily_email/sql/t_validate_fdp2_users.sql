select  /*+ LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_validate_fdp2_users')*/  1 as valid
from blackdeck.users
where date(greatest(etl_insert_time, etl_update_time) at timezone 'America/Los_Angeles')='{{ tomorrow_ds }}'
limit 1;