delete /*+ direct, LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_clear_auto_annotations')*/ from
 kpi.daily_email_annotations where report_date = date('{{ ds }}')
 and app_name = '{{ params.app_name }}'
 and annotation = '{{ params.nonblocking_annotation }}'
and is_auto = true;

commit;