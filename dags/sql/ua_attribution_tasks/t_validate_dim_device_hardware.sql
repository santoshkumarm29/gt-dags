SELECT /*+ LABEL ('airflow-all-ua_master_main_dag.ua_attribution_subdag-t_validate_dim_device_hardware' )*/
case when count(*) = 2 then 1 else 0 end as valid from (
select case when app = 'GSN Casino 2' then 'GSN Casino' else app end,
count(distinct date_trunc('hour',y.first_seen_ts)) as ct
 from gsnmobile.dim_app_installs x
 left join gsnmobile.dim_device_hardware y
 using (synthetic_id, first_seen)
 where x.first_seen = '{{ ds }}'
 and app in ('TriPeaks Solitaire','GSN Casino','GSN Casino 2')
 group by 1) x
 where ct = 24;