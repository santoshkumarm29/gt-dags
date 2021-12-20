-- Worldwinner table is moved over to Vertica daily, but does not have a clean break at midnight. Some records from
-- early morning of the current day are sent over as well. So, make sure we have records from the afternoon.
SELECT /*+ LABEL ('airflow-skill-worldwinner_gsncom_daily_imports-t_validate_ww_dim_profiles_private')*/
1 AS valid
FROM ww.dim_profiles_private
WHERE date(last_updated) = '{{ ds }}'
AND last_updated >= timestampadd(hh, 12, '{{ ds }}')
limit 1;
