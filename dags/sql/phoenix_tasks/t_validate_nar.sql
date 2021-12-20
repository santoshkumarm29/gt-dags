SELECT  /*+ LABEL ('airflow-ua-ua_master_main_dag.phoenix_subdag-t_validate_nar')*/ 1 AS valid
FROM phoenix.tournament_entry_nar
WHERE DATE (tournament_close_day) = '{{ prev_ds }}'
limit 1;
