SELECT /*+ LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}') */
       CASE
       WHEN num_completed_apps < 1 THEN 0
       ELSE 1
       END
FROM (SELECT count(DISTINCT singular_app_clean) AS num_completed_apps
      FROM gsnmobile.tableau_ua_singular_data
      WHERE singular_start_date = '{{ prev_ds }}'
        AND singular_app_clean = 'WORLDWINNER') T;
