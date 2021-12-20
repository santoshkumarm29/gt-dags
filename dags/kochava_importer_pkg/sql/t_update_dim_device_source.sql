drop table if exists gsnmobile.dim_device_source_ods;

select /*+ LABEL ('airflow-ua-ua_master_main_dag.kochava_importer_subdag-t_update_dim_device_source')*/ * into gsnmobile.dim_device_source_ods
from gsnmobile.dim_device_source limit 0;


insert /*+ direct,LABEL ('airflow-ua-ua_master_main_dag.kochava_importer_subdag-t_update_dim_device_source')*/ into  gsnmobile.dim_device_source_ods
  (synthetic_id, ad_partner, platform, inserted_on, bid_id, bid_price, campaign_id, campaign_name,
  click_date, country_code, creative, install_date, network_name, app_name, app_id, site_name, site_id)
    SELECT DISTINCT
      synthetic_id, ad_partner, platform, inserted_on, bid_id, bid_price, campaign_id, campaign_name,
      click_date, country_code, creative, install_date, network_name, app_name, null, site_name, site_id
FROM (
    SELECT
      synthetic_id,
      ad_partner,
      platform,
      inserted_on,
      bid_id,
      bid_price,
      campaign_id,
      campaign_name,
      click_date,
      country_code,
      creative,
      MIN(install_date) OVER (PARTITION BY synthetic_id order by install_date) as min_install_date,
      install_date,
      network_name,
      CASE
        WHEN app_name = 'GSN CASINO' THEN 'GSN Casino'
        WHEN app_name = 'GSN BINGO' THEN 'GSN Bingo'
	ELSE app_name
      END AS app_name,
      null,
      site_name,
      site_id
    FROM gsnmobile.ad_partner_installs
    WHERE
      synthetic_id IS NOT NULL
) z
WHERE min_install_date = install_date;

truncate table gsnmobile.dim_device_source;
insert /*+ direct,LABEL ('airflow-ua-ua_master_main_dag.kochava_importer_subdag-t_update_dim_device_source')*/ into  gsnmobile.dim_device_source
select * from gsnmobile.dim_device_source_ods;
commit;
truncate table gsnmobile.dim_device_source_ods;
