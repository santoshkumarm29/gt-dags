-- this is for web only (canvas) but running on whole table to ensure table is up to date

select  /*+ LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_validate_gsncasino_transactions')*/  1 as valid
from gsncasino.transactions
where date(etl_insert_time at timezone 'America/Los_Angeles')='{{ tomorrow_ds }}'
limit 1;