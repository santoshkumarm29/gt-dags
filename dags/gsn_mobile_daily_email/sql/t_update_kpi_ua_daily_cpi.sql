DELETE /*+direct, LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_update_kpi_ua_installs_cpi')*/
FROM kpi.ua_daily_cpi WHERE event_day = '{{ ds }}';

INSERT /*+direct, LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_update_kpi_ua_installs_cpi')*/
INTO kpi.ua_daily_cpi
SELECT
  kochava_install_date,
  CASE
    WHEN kochava_app_name_clean = 'WORLDWINNER' THEN 'WorldWinner App'
    WHEN kochava_app_name_clean = 'BINGOBASH' THEN 'Bingo Bash Mobile'
    WHEN kochava_app_name_clean = 'WOFSLOTS' THEN 'Wheel of Fortune Slots 2.0'
    WHEN kochava_app_name_clean = 'SOLITAIRETRIPEAKS' THEN 'TriPeaks Solitaire'
    WHEN kochava_app_name_clean = 'GSNCASINO' THEN 'GSN Casino'
    WHEN kochava_app_name_clean = 'SPARCADE' THEN 'Sparcade'
    WHEN kochava_app_name_clean = 'FRESHDECKPOKER' THEN 'Fresh Deck Poker'
    WHEN kochava_app_name_clean = 'GSNGRANDCASINO' THEN 'Grand Casino'
    WHEN kochava_app_name_clean = 'WWSOLRUSH' THEN 'Solitaire Rush'
    ELSE kochava_app_name_clean
  END as app_name,
  sum(kochava_installs)  AS kochava_installs,
  sum(singular_adn_cost) AS singular_cost
FROM gsnmobile.tableau_ua_kochava a
  JOIN gsnmobile.tableau_ua_singular_data b ON a.join_field_kochava_to_spend = b.join_field_singular_to_kochava
WHERE kochava_install_date = '{{ ds }}'
      AND a.kochava_network_name_clean <> 'UNATTRIBUTED'
GROUP BY 1, 2;
