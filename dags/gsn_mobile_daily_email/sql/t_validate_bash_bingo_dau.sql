select /*+ LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_validate_bash_bingo_dau')*/  1 as valid
from bash.bingo_dau
where active_on = date('{{ tomorrow_ds }}')
limit 1;
