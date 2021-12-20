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

MERGE /*+ DIRECT,LABEL('airflow-{{ params.studio }}-{{ params.dag_id }}-{{ params.task_id }}') */
  INTO {{ params.schema_name }}.{{ params.final_table_name }} AS target
 USING dedupe_{{ params.ods_table_name }} AS source
 ON source.{{ column_names[0] }} = target.{{ column_names[0] }}
WHEN MATCHED THEN UPDATE SET
    {% for column_name in column_names %}{{ column_name }} = source.{{ column_name }}{% if not loop.last %},{% endif %}{% endfor %}
WHEN NOT MATCHED THEN
INSERT ({{ column_names|join(', ') }})
VALUES ({% for column_name in column_names %} source.{{ column_name }}{% if not loop.last %},{% endif %}  {% endfor %});



-- SELECT PURGE_TABLE('{{ params.schema_name }}.{{ params.final_table_name }}');

-- SELECT ANALYZE_STATISTICS('{{ params.schema_name }}.{{ params.final_table_name }}');