SELECT /*+ LABEL ('airflow-all-ua_master_main_dag.ua_attribution_subdag-t_validate_kochava_data')*/
case when sum(case when app_name = 'SOLITAIRE TRIPEAKS' and pct_over_wk_avg >= 0.5 then 1
when app_name = 'GSN CASINO' and pct_over_wk_avg >= 0.5 then 1
when app_name = 'BINGO BASH' and pct_over_wk_avg >= 0.5 then 1
else 0 end) = 3 then 1 else 0 end as valid from (
select
app_name, count(case when DATE (install_date) = date('{{ ds }}') then synthetic_id else null end) /
(count(case when DATE (install_date) between date('{{ ds }}') - 7 and date('{{ ds }}') - 1 then synthetic_id else null end) / 7) as pct_over_wk_avg
FROM gsnmobile.ad_partner_installs
WHERE (ad_partner = 'kochava')
AND   (platform IN ('ios','android','amazon'))
AND   (COALESCE(synthetic_id,idfa,idfv,android_id,adid,'') <> '')
AND   DATE (install_date) between date('{{ ds }}') - 7 and date('{{ ds }}')
AND   network_name NOT ilike 'unattributed'
AND   app_name in ('SOLITAIRE TRIPEAKS', 'GSN CASINO','BINGO BASH')
group by 1) x
;
