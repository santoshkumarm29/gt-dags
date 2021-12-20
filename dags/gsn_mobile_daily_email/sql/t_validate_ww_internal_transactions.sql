select  /*+ LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_validate_ww_internal_transactions')*/  1 as valid
from ww.internal_transactions
where DATE (trans_date) = '{{ ds }}'
limit 1;
