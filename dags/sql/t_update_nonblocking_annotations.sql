-- KPI overall
insert /*+ direct, LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_update_auto_annotations')*/ into
 kpi.daily_email_annotations ( report_date, created_time, app_name, annotation, is_auto, update_job)

select
        '{{ ds }}' as report_date,
        sysdate as created_time,
        '{{ params.app_name }}' as app_name,
        '{{ params.nonblocking_annotation }}' as annotation,
        true as is_auto,
        '{{ params.update_job }}' as update_job;