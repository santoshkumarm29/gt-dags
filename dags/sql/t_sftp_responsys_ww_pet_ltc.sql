SELECT   /*+ LABEL ('airflow-skill-worldwinner_gsncom_daily_imports-t_sftp_responsys_ww_pet_ltc')*/ CUSTOMER_ID,
        LOW_LTC,
        MED_LTC,
        HIGH_LTC
FROM airflow.snapshot_responsys_ww_pet_ltc
where ds =  '{{ ds }}'
EXCEPT
SELECT  CUSTOMER_ID,
        LOW_LTC,
        MED_LTC,
        HIGH_LTC
FROM airflow.snapshot_responsys_ww_pet_ltc
where ds =  '{{ yesterday_ds }}';