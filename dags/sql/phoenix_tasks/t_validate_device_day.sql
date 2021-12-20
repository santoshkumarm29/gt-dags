SELECT  /*+ LABEL ('airflow-ua-ua_master_main_dag.phoenix_subdag-t_validate_device_day')*/ 1 AS valid
FROM phoenix.device_day
WHERE DATE (event_day) = '{{ prev_ds }}'
limit 1;
