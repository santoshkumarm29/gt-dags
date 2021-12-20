-- Cleanup the table before load
Truncate table gsnmobile.ad_partner_clicks_staging;

Insert /*+ direct,LABEL ('airflow-ua-ua_master_main_dag.kochava_click_importer_subdag_000-t_apply_synthetic_ids') */
into gsnmobile.ad_partner_clicks_staging
select
a.*,
       CASE
            WHEN a.platform ='ios'
            THEN a.idfa
            WHEN a.platform ='android'
            THEN a.adid
        END AS id1 ,
        CASE
            WHEN a.platform ='ios'
            THEN a.idfv
            WHEN a.platform ='android' or a.platform='amazon'
            THEN a.android_id
        END id2
from gsnmobile.ad_partner_clicks a
where date(a.click_date) >= date('{{ ds }}') - 15
and a.synthetic_id is null;

CREATE LOCAL TEMPORARY TABLE ad_partner_clicks_temp_1 ON COMMIT PRESERVE ROWS DIRECT
        AS
        SELECT
        /*+ LABEL ('airflow-ua-ua_master_main_dag.kochava_click_importer_subdag_000-t_apply_synthetic_ids') */
         a.*,
         m.synthetic_id as synthetic_id1
        FROM
             gsnmobile.ad_partner_clicks_staging a
        LEFT JOIN
            gsnmobile.dim_device_mapping AS m
        ON
            a.id1 = M.id
        AND id_type IN ('idfa','googleAdId')
         ORDER BY
          a.app_name,
          a.campaign,
          a.platform,
          a.tracker_id,
          a.id,
          a.kochava_click_id,
          a.click_date
SEGMENTED BY hash(id2)  ALL NODES KSAFE 1;


CREATE LOCAL TEMPORARY TABLE ad_partner_clicks_temp_2 ON COMMIT PRESERVE ROWS DIRECT
        AS
        SELECT
        /*+ LABEL ('airflow-ua-ua_master_main_dag.kochava_click_importer_subdag_000-t_apply_synthetic_ids') */
        a.ad_partner,
        a.platform,
        a.inserted_on,
        a.app_name,
        a.app_id,
        a.account,
        a.click_date,
        a.campaign,
        a.campaign_name,
        a.creative,
        a.network,
        a.network_name,
        a.site_id,
        a.tracker,
        a.tracker_id,
        a.country_code,
        a.click_status,
        a.device_version,
        a.idfa,
        a.idfv,
        a.adid,
        a.android_id,
        a.origination_ip,
        a.partition,
        a.id,
        a.original_request,
        a.clickgeo_continent_code,
        a.country_name,
        a.kochava_click_id,
        a.partner_click_id,
        a.latitude,
        a.longitude,
        NVL( a.synthetic_id1 , m2.synthetic_id) as synthetic_id,
        row_number() over(partition by a.app_name, a.platform, a.campaign, nvl(a.tracker_id,''), nvl(a.kochava_click_id,''), a.click_date,
                                       nvl(a.site_id,''), nvl(a.country_code,''), nvl(idfa,''), nvl(idfv,''), nvl(adid,''), nvl(android_id,'')
                          order by a.app_name, a.platform, a.campaign, nvl(a.tracker_id,''), nvl(a.kochava_click_id,''), a.click_date,
                                   nvl(a.site_id,''), nvl(a.country_code,''), nvl(idfa,''), nvl(idfv,''), nvl(adid,''), nvl(android_id,'')) as rn
FROM
    ad_partner_clicks_temp_1 AS a
LEFT JOIN
    gsnmobile.dim_device_mapping AS m2
ON
    a.id2 = m2.id
and a.synthetic_id1 is NULL
AND m2.id_type IN ('android_id','idfv')
ORDER BY
          a.app_name,
          a.platform,
          a.click_date
SEGMENTED BY hash(campaign, tracker_id, kochava_click_id, synthetic_id) ALL NODES KSAFE 1;

DELETE
    /*+ direct,LABEL ('airflow-ua-ua_master_main_dag.kochava_click_importer_subdag_000-t_apply_synthetic_ids') */
FROM
    gsnmobile.ad_partner_clicks
WHERE
    DATE(click_date) >= DATE('{{ ds }}') - 15
AND ( app_name, platform, campaign, NVL(tracker_id,''), NVL(kochava_click_id,''), click_date,
      NVL(site_id,''), NVL(country_code,''), NVL(idfa,''), NVL(idfv,''), NVL(adid,''), NVL(android_id,''))
      IN
    (SELECT
            a.app_name,
            a.platform,
            a.campaign,
            NVL(a.tracker_id,''),
            NVL(a.kochava_click_id,''),
            a.click_date,
            NVL(a.site_id,''),
            NVL(a.country_code,''),
            NVL(a.idfa,''),
            NVL(a.idfv,''),
            NVL(a.adid,''),
            NVL(a.android_id,'')
        FROM
            ad_partner_clicks_temp_2 a
        WHERE a.rn = 1
          AND a.synthetic_id IS NOT NULL);

Insert /*+ direct,LABEL ('airflow-ua-ua_master_main_dag.kochava_click_importer_subdag_000-t_apply_synthetic_ids') */
  into gsnmobile.ad_partner_clicks
   (ad_partner,
    platform,
    inserted_on,
    app_name,
    app_id,
    account,
    click_date,
    campaign,
    campaign_name,
    creative,
    network,
    network_name,
    site_id,
    tracker,
    tracker_id,
    country_code,
    click_status,
    device_version,
    idfa,
    idfv,
    adid,
    android_id,
    origination_ip,
    partition,
    synthetic_id,
    id,
    original_request,
    clickgeo_continent_code,
    country_name,
    kochava_click_id,
    partner_click_id,
    latitude,
    longitude)
 Select
    ad_partner,
    platform,
    inserted_on,
    app_name,
    app_id,
    account,
    click_date,
    campaign,
    campaign_name,
    creative,
    network,
    network_name,
    site_id,
    tracker,
    tracker_id,
    country_code,
    click_status,
    device_version,
    idfa,
    idfv,
    adid,
    android_id,
    origination_ip,
    partition,
    synthetic_id,
    id,
    original_request,
    clickgeo_continent_code,
    country_name,
    kochava_click_id,
    partner_click_id,
    latitude,
    longitude
 from ad_partner_clicks_temp_2
 where rn = 1
 and synthetic_id is not null;

COMMIT;
