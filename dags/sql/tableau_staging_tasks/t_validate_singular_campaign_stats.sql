SELECT /*+ LABEL ('airflow-ua-ua_master_main_dag.tableau_subdag-t_validate_singular_campaign_stats')*/ 1 AS valid
FROM apis.singular_campaign_stats
WHERE start_date >=  CASE WHEN date('{{ ds }}') = date(SYSDATE) THEN date('{{ ds }}')-1 ELSE date('{{ ds }}') END
limit 1;