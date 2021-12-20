SELECT  /*+ LABEL ('airflow-ua-{{ dag.dag_id }}-{{ task.task_id }}')*/
       1 AS valid
FROM ww.app_acquisition
WHERE DATE(day) = '{{ (macros.datetime.now() - macros.timedelta(1)).date().isoformat() }}'
limit 1;
