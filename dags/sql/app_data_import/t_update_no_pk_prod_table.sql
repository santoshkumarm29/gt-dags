/*
Perform an upsert on target table for tables that do not have PRIMARY KEY , USE THIS if the Number of rows are thousands and columns numbers are reasonable.
THis SQL will merge with all columns of ODS with actual table
 */

{% set column_names = get_column_names(params) %}
MERGE /*+ DIRECT,LABEL('airflow-{{ params.studio }}-{{ params.dag_id }}-{{ params.task_id }}') */
  INTO {{ params.schema_name }}.{{ params.final_table_name }} AS target
 USING (
  -- subquery deduplicates using table's primary key
  WITH dedupe AS (
    SELECT {{ column_names|join(', ') }},
           {# first column is always autoincrementing integer primary key  #}
           ROW_NUMBER() OVER(PARTITION BY {% for column_name in column_names %} {{ column_name }}{% if not loop.last %},{% endif %}  {% endfor %} ORDER BY s3_key DESC ) AS row_num
      FROM {{ params.schema_name }}.{{ params.ods_table_name }} -- use ods table
  )
  SELECT {{ column_names|join(', ') }}
    FROM dedupe
   WHERE row_num = 1
 ) source
 ON {% for column_name in column_names %} source.{{ column_name }} = target.{{ column_name }}{% if not loop.last %} and {% endif %}{% endfor %}
WHEN MATCHED THEN UPDATE SET
    {% for column_name in column_names %}{{ column_name }} = source.{{ column_name }}{% if not loop.last %},{% endif %}{% endfor %}
WHEN NOT MATCHED THEN
INSERT ({{ column_names|join(', ') }})
VALUES ({% for column_name in column_names %} source.{{ column_name }}{% if not loop.last %},{% endif %}  {% endfor %});

TRUNCATE TABLE  {{ params.schema_name }}.{{ params.ods_table_name }};

-- SELECT PURGE_TABLE('{{ params.schema_name }}.{{ params.final_table_name }}');

-- SELECT ANALYZE_STATISTICS('{{ params.schema_name }}.{{ params.final_table_name }}');
