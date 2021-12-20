SELECT  /*+ LABEL ('airflow-ua-ua_master_main_dag.phoenix_subdag-t_validate_deposits_withdrawals')*/ 1 AS valid
FROM phoenix.deposits_withdrawals
WHERE DATE (event_day) = '{{ prev_ds }}'
limit 1;
