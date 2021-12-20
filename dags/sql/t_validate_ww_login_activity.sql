-- Worldwinner table is moved over to Vertica daily, but does not have a clean break at midnight. Some records from
-- early morning of the current day are sent over as well. So, make sure we have records from the afternoon.
SELECT /*+ LABEL ('airflow-skill-worldwinner_gsncom_daily_imports-t_validate_ww_login_activity')*/
1 AS valid
FROM ww.login_activity
WHERE date(activity_date) = '{{ tomorrow_ds }}'
limit 1;

