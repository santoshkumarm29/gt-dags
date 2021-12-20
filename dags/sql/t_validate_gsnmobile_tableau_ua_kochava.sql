SELECT /*LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_validate_gsnmobile_tableau_ua_kochava')*/
       CASE
       WHEN count(case when today_installs / yesterday_installs < .4 THEN 1 ELSE NULL END) > 1
         THEN 0
       ELSE 1
       END
FROM (select
kochava_app_name_clean,
    sum(case when kochava_install_date = '{{ ds }}' then kochava_installs else 0 end) today_installs,
    sum(case when kochava_install_date = '{{ yesterday_ds }}' then kochava_installs else 0 end) yesterday_installs
from gsnmobile.tableau_ua_kochava
where kochava_install_date between '{{ yesterday_ds }}' and '{{ ds }}'
group by 1) T
WHERE yesterday_installs > 500
;