{% set column_names = get_column_names(params) %}

DELETE /*+ DIRECT, LABEL('airflow-{{ dag.dag_id }}-{{ task.task_id }}') */
FROM {{ params.schema }}.{{ params.table_name }}
WHERE (file_name) IN (SELECT DISTINCT file_name FROM {{ params.schema }}.{{ params.table_name }}_ods);

INSERT /*+ DIRECT, LABEL('airflow-{{ dag.dag_id }}-{{ task.task_id }}') */
INTO {{ params.schema }}.{{ params.table_name }}
    ({{ column_names|join(', ') }})
SELECT {{ column_names|join(', ') }}
  FROM {{ params.schema }}.{{ params.table_name }}_ods
;
