select  /*+ LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_validate_ww_dim_users')*/  1 as valid
from ww.dim_users
where date(createdate) = '{{ ds }}'
limit 1;
