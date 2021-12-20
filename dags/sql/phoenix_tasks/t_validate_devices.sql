SELECT  /*+ LABEL ('airflow-ua-ua_master_main_dag.phoenix_subdag-t_validate_devices')*/ 1 AS valid
FROM phoenix.devices
WHERE DATE (install_day) = '{{ prev_ds }}'
limit 1;
