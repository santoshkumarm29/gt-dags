-- see code in https://github.com/gsnsocial/airflow-library-gsn-common/blob/master/gsn_common/kpi_email_utils.py#L591-L606
-- function get_mtd_kpi_df

select /*+ LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_validate_forecasts')*/
case when (count(budget_ad_rev) + count(budget_iap_rev)) / count(*) = 2 then 1 else 0 end as valid from (
select
        d.thedate as event_day,
        d.app_name,
        budget_ad_rev,
        budget_iap_rev
    from
    (
        select thedate, app_name from newapi.dim_date x
            cross join
        (
            select distinct app_name from kpi.dim_app_daily_metrics_latest
             where event_day between date('{{ ds }}') and date('{{ ds }}') + 7
        ) y
        where thedate between date('{{ ds }}') and date('{{ ds }}') + 7
    ) d
    left join
    (
        select
        event_day,
        case
               when app_name = 'TriPeaks' then 'TriPeaks Solitaire'
               when app_name = 'WorldWinner' then 'GSN Cash Games'
               when app_name = 'Casino Mobile' then 'GSN Casino'
               when app_name = 'Casino Canvas' then 'GSN Canvas'
               when app_name = 'Wheel of Fortune Slots' then 'Wheel of Fortune Slots 2.0'
               else app_name
        end as app_name,
        forecast_iap as budget_iap_rev,
        forecast_ad as budget_ad_rev
        from kpi.dim_forecast_by_app
        where event_day between date('{{ ds }}') and date('{{ ds }}') + 7) b
    on d.thedate = b.event_day and d.app_name = b.app_name) x
    -- include any apps ramping up here
    where app_name not in ('Grand Casino 2')
;

