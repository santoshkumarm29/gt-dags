SELECT /*+ LABEL ('airflow-all-ua_master_main_dag.ua_attribution_subdag-t_validate_client_data')*/
case when count(*) = 3 then 1 else 0 end AS valid
from (
SELECT case when app in ('GSN Casino', 'GSN Casino 2') then 'Casino'
when app in ('GSN Grand Casino', 'GSN Grand Casino 2') then 'Grand Casino'
else app end,
count(distinct date_trunc('hour',first_seen_ts)) as hr_ct
FROM gsnmobile.dim_app_installs
WHERE DATE (first_seen_ts) = date('{{ ds }}')
and app in ('TriPeaks Solitaire','GSN Grand Casino', 'GSN Grand Casino 2','GSN Casino', 'GSN Casino 2')
group by 1) x
where hr_ct >= 24;
