/*
Perform an insert on history table
 */

INSERT /*+ DIRECT,LABEL('airflow-{{ params.studio }}-{{ params.dag_id }}-{{ params.task_id }}') */
INTO {{ params.schema_name }}.{{ params.final_table_name }}_history
SELECT *, now()
FROM {{ params.schema_name }}.{{ params.ods_table_name }};


commit;
