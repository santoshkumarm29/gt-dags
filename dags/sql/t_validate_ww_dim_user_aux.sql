-- Worldwinner table is moved over to Vertica daily, but does not have a clean break at midnight. Some records from
-- early morning of the current day are sent over as well. So, make sure we have records from the afternoon.
SELECT /*+ LABEL ('airflow-skill-worldwinner_gsncom_daily_imports-t_validate_ww_dim_user_aux')*/
1 AS valid
FROM ww.dim_user_aux
WHERE date(last_login) = '{{ ds }}'
AND last_login >= timestampadd(hh, 12, '{{ ds }}')
limit 1;
