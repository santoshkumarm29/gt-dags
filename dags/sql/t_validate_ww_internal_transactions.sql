-- This is loaded once a day with a clean break at midnight

SELECT /*+ LABEL ('airflow-skill-worldwinner_gsncom_daily_imports-t_validate_ww_internal_transactions')*/
1 AS valid
FROM ww.internal_transactions
where date(trans_date) = date('{{ ds }}')
limit 1;
