SELECT /*LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_validate_gsnmobile_tableau_ua_singular_data')*/
       CASE
       WHEN num_completed_apps < 4 THEN 0
       ELSE 1
       END
FROM (SELECT count(DISTINCT singular_app_clean) AS num_completed_apps
      FROM gsnmobile.tableau_ua_singular_data
      WHERE singular_start_date = '{{ ds }}'
        AND singular_app_clean IN ('SOLITAIRETRIPEAKS', 'BINGOBASH', 'WORLDWINNER', 'GSNCASINO')
      ) T;
