truncate table gsnmobile.ad_roas_kochava_ods;

insert /*+direct, LABEL ('airflow-all-ua_master_main_dag.ua_attribution_subdag-t_update_ad_roas_kochava')*/ into gsnmobile.ad_roas_kochava_ods
SELECT platform,
       install_day,
       app_name,
       campaign_name,
       campaign_id,
       site_id,
       network_name,
       creative,
       country_code,
       request_campaign_id,
       request_adgroup,
       request_keyword,
       request_matchtype,
       model,
       device,
       NULL as ww_registered,
       request_bundle_id,
       COUNT(DISTINCT synthetic_id) AS installs,
       SUM(todate_bookings) AS todate_bookings,
       SUM(payer) AS todate_payers,
       SUM(day1_bookings) AS day1_bookings,
       SUM(day2_bookings) AS day2_bookings,
       SUM(day3_bookings) AS day3_bookings,
       SUM(day7_bookings) AS day7_bookings,
       SUM(day14_bookings) AS day14_bookings,
       SUM(day30_bookings) AS day30_bookings,
       SUM(day60_bookings) AS day60_bookings,
       SUM(day90_bookings) AS day90_bookings,
       SUM(day180_bookings) AS day180_bookings,
       SUM(day360_bookings) AS day360_bookings,
       SUM(day540_bookings) AS day540_bookings,
       SUM(day1_payer) AS day1_payers,
       SUM(day2_payer) AS day2_payers,
       SUM(day3_payer) AS day3_payers,
       SUM(day7_payer) AS day7_payers,
       SUM(day14_payer) AS day14_payers,
       SUM(day30_payer) AS day30_payers,
       SUM(day60_payer) AS day60_payers,
       SUM(day90_payer) AS day90_payers,
       SUM(day180_payer) AS day180_payers,
       SUM(day360_payer) AS day360_payers,
       SUM(day540_payer) AS day540_payers,
       SUM(day1_retained) AS day1_retained,
       SUM(day2_retained) AS day2_retained,
       SUM(day3_retained) AS day3_retained,
       SUM(day7_retained) AS day7_retained,
       SUM(day14_retained) AS day14_retained,
       SUM(day30_retained) AS day30_retained,
       SUM(day60_retained) AS day60_retained,
       SUM(day90_retained) AS day90_retained,
       SUM(day180_retained) AS day180_retained,
       SUM(day360_retained) AS day360_retained,
       SUM(day540_retained) AS day540_retained,
       MAX(last_active_day) as last_active_day --- added part of UA tableau SQL rewrite

FROM gsnmobile.kochava_device_summary
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17;

truncate table gsnmobile.ad_roas_kochava;
insert /*+direct,LABEL ('airflow-all-ua_master_main_dag.ua_attribution_subdag-t_update_ad_roas_kochava')*/ into  gsnmobile.ad_roas_kochava select * from gsnmobile.ad_roas_kochava_ods;
truncate table gsnmobile.ad_roas_kochava_ods;

commit;
