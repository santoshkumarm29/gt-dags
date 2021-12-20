-- insert all into one table
truncate table temp.ah_2105_ad_partner_installs_staging;

insert /*+direct,LABEL ('airflow-ua-ua_master_main_dag.kochava_importer_subdag-t_apply_synthetic_ids_1')*/ into
temp.ah_2105_ad_partner_installs_staging
select a.*,
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
        END id2 from temp.ah_2105_ad_partner_installs_gamesnetwork_ods a;

insert /*+direct,LABEL ('airflow-ua-ua_master_main_dag.kochava_importer_subdag-t_apply_synthetic_ids_1')*/ into
temp.ah_2105_ad_partner_installs_staging
select a.*,
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
        END id2 from temp.ah_2105_ad_partner_installs_bitrhymes_ods a;

insert /*+direct,LABEL ('airflow-ua-ua_master_main_dag.kochava_importer_subdag-t_apply_synthetic_ids_1')*/ into
temp.ah_2105_ad_partner_installs_staging
select a.*,
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
        END id2 from temp.ah_2105_ad_partner_installs_idlegames_ods a;

select analyze_statistics('temp.ah_2105_ad_partner_installs_staging');

-- lighten load for applying synthetic IDs
delete /*+direct,LABEL ('airflow-ua-ua_master_main_dag.kochava_importer_subdag-t_apply_synthetic_ids_1')*/
from temp.ah_2105_ad_partner_installs_staging
WHERE (ad_partner_id, app_name, tracker_id) IN
      (select ad_partner_id, app_name, tracker_id from
      (SELECT ad_partner_id, app_name, max(tracker_id) as tracker_id, count(*) as ct
         FROM temp.ah_2105_ad_partner_installs
        WHERE nvl(synthetic_id, '') <> ''
        and date(install_date) between date('{{ ds }}') - 31 and  date('{{ ds }}') + 7
        group by 1,2) x where ct = 1)
        and upper(network_name) <> 'UNATTRIBUTED';

delete /*+direct,LABEL ('airflow-ua-ua_master_main_dag.kochava_importer_subdag-t_apply_synthetic_ids_1')*/
from temp.ah_2105_ad_partner_installs_staging
WHERE (ad_partner_id, app_name) IN
      (SELECT DISTINCT ad_partner_id, app_name
         FROM gsnmobile.kochava_unattributed_installs
        WHERE nvl(synthetic_id, '') <> ''
        and date(install_date) between date('{{ ds }}') - 30 and  date('{{ ds }}') + 7)
        and upper(network_name) = 'UNATTRIBUTED';


-- attempt to map synthetic ID using IDFA and AD ID first
CREATE LOCAL TEMPORARY TABLE ad_partner_installs_temp_1 ON COMMIT PRESERVE ROWS DIRECT
        AS
        SELECT
    /*+ direct,LABEL ('airflow-ua-ua_master_main_dag.kochava_importer_subdag-t_apply_synthetic_ids_2') */
         a.*,
         m.synthetic_id as synthetic_id1
        FROM
             temp.ah_2105_ad_partner_installs_staging a
        LEFT JOIN
            gsnmobile.dim_device_mapping AS m
        ON
            id1= M.id
        AND case when a.platform = 'ios' then 'idfa' else 'googleAdId' end = M.id_type
        AND id_type IN ('idfa',
                        'googleAdId')
         ORDER BY
          app_name,
          platform,
          install_date
SEGMENTED BY hash(id2)  ALL NODES KSAFE 1;

-- finish by mapping synthetic ID using android id and idfv second
CREATE LOCAL TEMPORARY TABLE ad_partner_installs_temp_2 ON COMMIT PRESERVE ROWS DIRECT
        AS
        SELECT
    /*+ direct,LABEL ('airflow-ua-ua_master_main_dag.kochava_importer_subdag-t_apply_synthetic_ids_3') */
    a.ad_partner,
    a.platform,
    a.inserted_on,
    a.bid_id,
    a.bid_price,
    a.campaign_id,
    a.campaign_name,
    a.click_date,
    a.country_code,
    a.install_date,
    a.network_name,
    a.app_name,
    a.app_id,
    a.site_name,
    a.site_id,
    a.sub_site_id,
    a.udid,
    a.mac_address,
    a.idfa,
    a.device_type,
    a.android_id,
    a.device_id,
    a.country_name,
    a.ad_partner_id,
    a.creative,
    a.matched_by,
    a.matched_on,
    a.account,
    a.network_id,
    a.idfv,
    a.adid,
    a.tracker_id,
    a.tracker_name,
    a.click_id,
    a.request_campaign_id,
    a.request_click_id,
    a.request_g_id,
    a.request_adgroup,
    a.request_keyword,
    a.request_matchtype,
    a.request_network,
    a.request_cp_value1,
    a.request_cp_value2,
    a.request_cp_value3,
    a.request_cp_name1,
    a.request_cp_name2,
    a.request_cp_name3,
    a.region,
    a.device_version,
    a.device_ua ,
    a.request_bundle_id,
    a.request_adgroup_name,
    a.request_campaigngroup_name,
    a.ad_squad_id,
    a.ad_squad_name,
    a.ad_id,
    a.ad_name,
    a.campaign_group_id,
    a.campaign_group_name,
    NVL( a.synthetic_id1 , m2.synthetic_id) as synthetic_id
FROM
    ad_partner_installs_temp_1 a
LEFT JOIN
    gsnmobile.dim_device_mapping AS m2
ON
    a.id2= m2.id
and a.synthetic_id1 is NULL
AND case when a.platform = 'ios' then 'idfv' else 'android_id' end = m2.id_type
AND id_type IN ('android_id',
                'idfv')
ORDER BY
          app_name,
          platform,
          install_date
SEGMENTED BY hash(ad_partner_id, app_name, synthetic_id) ALL NODES KSAFE 1;
;



DELETE /*+ direct, LABEL ('airflow-ua-ua_master_main_dag.kochava_importer_subdag-t_apply_synthetic_ids_4') */
 FROM temp.ah_2105_ad_partner_installs_raw
WHERE (ad_partner_id, app_name) IN
      (SELECT ad_partner_id, app_name FROM ad_partner_installs_temp_2 WHERE upper(network_name) <> 'UNATTRIBUTED')
;

insert /*+ direct, LABEL ('airflow-ua-ua_master_main_dag.kochava_importer_subdag-t_apply_synthetic_ids_4') */
into temp.ah_2105_ad_partner_installs_raw
select distinct ad_partner,
platform,
inserted_on,
bid_id,
bid_price,
campaign_id,
campaign_name,
click_date,
country_code,
install_date,
network_name,
app_name,
app_id,
site_name,
site_id,
sub_site_id,
udid,
mac_address,
idfa,
device_type,
android_id,
device_id,
country_name,
ad_partner_id,
creative,
matched_by,
matched_on,
account,
network_id,
idfv,
adid,
tracker_id,
tracker_name,
click_id,
request_campaign_id,
request_click_id,
request_g_id,
request_adgroup,
request_keyword,
request_matchtype,
request_network,
request_cp_value1,
request_cp_value2,
request_cp_value3,
request_cp_name1,
request_cp_name2,
request_cp_name3,
region,
device_version,
device_ua,
synthetic_id,
request_bundle_id,
request_adgroup_name,
request_campaigngroup_name,
ad_squad_id,
ad_squad_name,
ad_id,
ad_name,
campaign_group_id,
campaign_group_name,
sysdate as updated_at from ad_partner_installs_temp_2 x WHERE upper(network_name) <> 'UNATTRIBUTED';


TRUNCATE TABLE temp.ah_2105_ad_partner_installs_ods;

INSERT /*+ direct, LABEL('airflow-ua-ua_master_main_dag.kochava_importer_subdag-t_apply_synthetic_ids_5') */
INTO temp.ah_2105_ad_partner_installs_ods
SELECT
    *
    , row_number() OVER (
        PARTITION BY app_name,
            ad_partner_id
        ORDER BY case when matched_on like '%claim' then 2 else 1 end) as app_device_claim_winner
    , row_number() OVER (
        PARTITION BY app_name,
            synthetic_id
        ORDER BY case when matched_on like '%claim' then 2 else 1 end, install_date) as app_sid_claim_winner
FROM temp.ah_2105_ad_partner_installs_raw;


COMMIT;

ALTER TABLE temp.ah_2105_ad_partner_installs, temp.ah_2105_ad_partner_installs_ods
RENAME TO ah_2105_ad_partner_installs_old, ah_2105_ad_partner_installs;

alter table temp.ah_2105_ad_partner_installs_old rename to ah_2105_ad_partner_installs_ods;
TRUNCATE TABLE temp.ah_2105_ad_partner_installs_ods;

