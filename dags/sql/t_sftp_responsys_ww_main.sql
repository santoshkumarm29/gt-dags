-- Casting to varchar in this query is for integer fields that may be null - pandas promotes these to floats otherwise.

SELECT  /*+ LABEL ('airflow-skill-worldwinner_gsncom_daily_imports-t_sftp_responsys_ww_main')*/ CUSTOMER_ID_,
       EMAIL_ADDRESS_,
       EMAIL_FORMAT_,
       EMAIL_PERMISSION_STATUS_,
       MOBILE_NUMBER_,
       MOBILE_COUNTRY_,
       MOBILE_PERMISSION_STATUS_,
       POSTAL_STREET_1_,
       POSTAL_STREET_2_,
       CITY_,
       STATE_,
       POSTAL_CODE_,
       COuNTRY_,
       POSTAL_PERMISSION_STATUS_,
       FIRST_NAME,
       LAST_NAME,
       BIRTHDATE,
       WARMUP,
       COBRAND_ID::varchar,
       ADVERTISER_ID,
       USER_NAME_CASH,
       USER_ID_GSN::varchar,
       PLATFORM_ID,
       CGWEB_OPT_IN_TOURN,
       CGWEB_OPT_IN_WIN_NOTIFY,
       CGWEB_OPT_IN_PROMO,
       CGWEB_OPT_IN_CHAL,
       CGWEB_OPT_IN_SURVIVE,
       CC_OPT_IN
FROM airflow.snapshot_responsys_ww_main
where ds = '{{ ds }}'
EXCEPT
SELECT CUSTOMER_ID_,
       EMAIL_ADDRESS_,
       EMAIL_FORMAT_,
       EMAIL_PERMISSION_STATUS_,
       MOBILE_NUMBER_,
       MOBILE_COUNTRY_,
       MOBILE_PERMISSION_STATUS_,
       POSTAL_STREET_1_,
       POSTAL_STREET_2_,
       CITY_,
       STATE_,
       POSTAL_CODE_,
       COuNTRY_,
       POSTAL_PERMISSION_STATUS_,
       FIRST_NAME,
       LAST_NAME,
       BIRTHDATE,
       WARMUP,
       COBRAND_ID::varchar,
       ADVERTISER_ID,
       USER_NAME_CASH,
       USER_ID_GSN::varchar,
       PLATFORM_ID,
       CGWEB_OPT_IN_TOURN,
       CGWEB_OPT_IN_WIN_NOTIFY,
       CGWEB_OPT_IN_PROMO,
       CGWEB_OPT_IN_CHAL,
       CGWEB_OPT_IN_SURVIVE,
       CC_OPT_IN
FROM airflow.snapshot_responsys_ww_main
where ds = '{{ yesterday_ds }}'