SELECT  /*+ LABEL ('airflow-ua-ua_master_main_dag.phoenix_subdag-t_validate_kochava_data')*/
case when count(*) > 100 then 1 else 0 end AS valid
FROM gsnmobile.ad_partner_installs
WHERE (ad_partner = 'kochava')
AND   (platform IN ('ios','android','amazon'))
AND   (COALESCE(synthetic_id,idfa,idfv,android_id,adid,'') <> '')
AND   DATE (install_date) = '{{ prev_ds }}'
AND   app_name = 'PHOENIX'
AND   network_name NOT ilike 'unattributed'
;
