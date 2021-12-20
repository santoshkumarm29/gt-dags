SELECT /*+ LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}') */
  install_day
  FROM gsnmobile.kochava_device_summary_phoenix
 WHERE install_day = '{{ prev_ds }}'
 and network_name <> 'unattributed'
 LIMIT 1
;
