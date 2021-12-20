/*
Perform an upsert on target table
 */

{% set column_names = get_column_names(params) %}
CREATE LOCAL TEMP TABLE dedupe_{{ params.ods_table_name }}
ON COMMIT PRESERVE ROWS DIRECT AS
      -- subquery deduplicates using table's primary key
    SELECT {{ column_names|join(', ') }}
      FROM (SELECT {{ column_names|join(', ') }},
          {# first column is always autoincrementing integer primary key  #}
          ROW_NUMBER() OVER (PARTITION BY {{ column_names[0] }} ORDER BY s3_key DESC) AS row_num
          FROM {{ params.schema_name }}.{{ params.ods_table_name }} -- use ods table
           ) T
      WHERE T.row_num = 1
      ORDER BY {{ column_names[0] }}
;

DELETE /*+ DIRECT,LABEL('airflow-{{ params.studio }}-{{ params.dag_id }}-{{ params.task_id }}') */
 FROM {{ params.schema_name }}.{{ params.final_table_name }}
    WHERE ({{ column_names[0] }}) IN (SELECT {{ column_names[0] }} FROM dedupe_{{ params.ods_table_name }})
      AND {{ params.etl_update_time }} > DATE('{{ params.ds }}') - {{ params.days_to_dedupe }}
;

INSERT /*+ DIRECT,LABEL('airflow-{{ params.studio }}-{{ params.dag_id }}-{{ params.task_id }}') */
  INTO {{ params.schema_name }}.{{ params.final_table_name }}
    SELECT * FROM dedupe_{{ params.ods_table_name }};

commit;

-- SELECT PURGE_TABLE('{{ params.schema_name }}.{{ params.final_table_name }}');

-- SELECT ANALYZE_STATISTICS('{{ params.schema_name }}.{{ params.final_table_name }}');
