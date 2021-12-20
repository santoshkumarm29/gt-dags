-- KPI overall
insert /*+ direct, LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_update_auto_annotations')*/ into
 kpi.daily_email_annotations ( report_date, app_name, annotation, is_auto, update_job)

select
        report_date,
        app_name,
        left(annotation, length(annotation)-1)||'.' as annotation,
        is_auto,
        update_job
from
(
 select
        event_day as report_date,
        app_name,
        case when perc_curr <= {{ params.kpi_ovr_pct }} and (perc_7d > {{ params.kpi_wk_pct }} or perc_14d > {{ params.kpi_wk_pct }} or perc_21d > {{ params.kpi_wk_pct }} or perc_28d > {{ params.kpi_wk_pct }}) then
                '% drop reflects High IAP' ||
                coalesce(case when perc_7d > {{ params.kpi_wk_pct }} then ' (' || to_char((rev_7d/1000)::int, 'FML999G999G999') || 'K) on ' || left(TO_CHAR(event_day - 7, 'Month'),3) || ' ' || day(event_day - 7)::varchar || ',' end,'') ||
                coalesce(case when perc_14d > {{ params.kpi_wk_pct }} then ' (' || to_char((rev_14d/1000)::int, 'FML999G999G999') || 'K) on ' || left(TO_CHAR(event_day - 14, 'Month'),3) || ' ' || day(event_day - 14)::varchar || ',' end,'') ||
                coalesce(case when perc_21d > {{ params.kpi_wk_pct }} then ' (' || to_char((rev_21d/1000)::int, 'FML999G999G999') || 'K) on ' || left(TO_CHAR(event_day - 21, 'Month'),3) || ' ' || day(event_day - 21)::varchar || ',' end,'') ||
                coalesce(case when perc_28d > {{ params.kpi_wk_pct }} then ' (' || to_char((rev_28d/1000)::int, 'FML999G999G999') || 'K) on ' || left(TO_CHAR(event_day - 28, 'Month'),3) || ' ' || day(event_day - 28)::varchar || ',' end,'')
                end as annotation,
        true as is_auto,
        '{{ params.update_job }}' as update_job
from
(
select *, rev/((rev_7d + rev_14d + rev_21d + rev_28d)/4) - 1 as perc_curr, rev_7d/((rev_14d+rev_21d+rev_28d)/3) - 1 as perc_7d, rev_14d/((rev_7d+rev_21d+rev_28d)/3) - 1 as perc_14d, rev_21d/((rev_14d+rev_7d+rev_28d)/3) - 1 as perc_21d, rev_28d/((rev_14d+rev_21d+rev_7d)/3) - 1 as perc_28d
from
(
                select *,
                lag(rev) over (partition by app_name, thedate order by event_day) as rev_7d,
                lag(rev,2) over (partition by app_name, thedate order by event_day) as rev_14d,
                lag(rev,3) over (partition by app_name, thedate order by event_day) as rev_21d,
                lag(rev,4) over (partition by app_name, thedate order by event_day) as rev_28d
                from
                (
                        select
                                thedate,
                                event_day,
                                'GSN Games' as app_name,
                                sum(transactional_bookings)::int as rev
                        from kpi.dim_app_daily_metrics_latest a
                        join newapi.dim_date b on a.event_day in (thedate - 28, thedate - 21, thedate - 14, thedate - 7, thedate)
                        where thedate = '{{ ds }}'
                        group by 1,2,3
                        order by app_name, thedate, event_day

                ) x
                order by app_name, thedate, event_day
) z
where thedate = event_day
) a
) b
where annotation is not null
order by report_date, app_name;

