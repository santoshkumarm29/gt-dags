DROP TABLE IF EXISTS mapping;
CREATE LOCAL TEMP TABLE mapping ON COMMIT PRESERVE ROWS DIRECT AS
/*+direct,LABEL('airflow-ua-ua_master_main_dag.kochava_importer_subdag-t_update_bingo_network_mapping')*/
SELECT  network_name,
        case when network_name = 'Facebook'                 then 'AdQuant'
             when network_name = 'Instagram'                then 'AdQuant'
             when network_name = 'Amazon - Fire'            then 'Amazon Media Group'
             when network_name ilike '%aarki%'              then 'Aarki'
             when network_name ilike '%adcolo%'             then 'AdColony'
             when network_name ilike 'adquant%'             then 'AdQuant'
             when network_name ilike '%adword%'             then 'AdWords'
             when network_name ilike '%apple%sear%'         then 'Apple Search Ads'
             when network_name ilike '%applike%'            then 'AppLike'
             when network_name ilike '%ferret%'             then 'Blind Ferret Media'
             when network_name ilike '%turbine%'            then 'Digital Turbine'
             when network_name ilike '%fyber%'              then 'Fyber'
             when network_name ilike '%iron%source%'        then 'ironSource'
             when network_name ilike 'mediaforce%'          then 'Mediaforce'
             when network_name ilike 'mistp%'               then 'Mistplay'
             when network_name ilike '%pikoya%'             then 'Pikoya'
             when network_name ilike '%similar%'            then 'Similarbid'
             when network_name ilike '%snapchat%'           then 'Snapchat'
             when network_name ilike '%tapjoy%'             then 'Tapjoy'
             when network_name ilike '%unity%ads%'          then 'Unity Ads'
             when network_name ilike '%gsn%'                then 'GSN'
             when network_name ilike '%remerge%'            then 'REMERGE'
             when network_name ilike '%appreciate%'         then 'Appreciate'
             when network_name ilike '%liftoff%'            then 'Liftoff'
             when network_name ilike '%point2web%'          then 'Point2Web'
             when network_name ilike '%adjoe%'              then 'Adjoe'
             when network_name ilike '%adMobo%'             then 'adMobo'
             when network_name ilike '%bigabid%'            then 'BigaBid'
             when network_name ilike '%blast%'              then 'Blast'
             when network_name ilike '%chartboost%'         then 'Chartboost'
             when network_name ilike '%point2web%'          then 'Point2Web'
             when network_name ilike '%vungle%'             then 'Vungle'
             when network_name ilike '%applovin%'           then 'AppLovin'
             when network_name ilike '%lifestreet%'         then 'LifeStreet'
             when network_name ilike '%Mundo%'              then 'Mundo Media'
             when network_name ilike '%taboola%'            then 'Taboola'
             when network_name ilike '%spotad%'             then 'Spotad'
             when network_name ilike '%yeahmobi%'           then 'Yeahmobi'
             when network_name ilike '%target%circle%'      then 'Target Circle'
             when network_name ilike '%ndrs%'               then 'NDRS'
             when network_name ilike '%hang%my%ads%'        then 'Hang My Ads'
             when network_name ilike '%yntm%'               then 'YNTM'
             when network_name ilike '%cheetah%'            then 'Cheetah Mobile'
             when network_name ilike '%grapheffect%'        then 'GraphEffect'
             when network_name ilike '%cheetah%'            then 'Cheetah Mobile'
             when network_name ilike '%smartlinks%'         then 'SmartLinks'
             when network_name ilike '%long%tail%pilot%'    then 'Long Tail Pilot'
             when network_name ilike '%restricted%'         then 'Restricted'
             when network_name ilike '%volo%'               then 'Volo Media'
             when network_name ilike '%Pinsight%'           then 'Pinsight Media'
             when network_name ilike '%Liquid%PCH%'         then 'PCH Liquid Wireless'             
        else network_name end singular_media_sorce,
        min(install_date) as date_added
FROM    gsnmobile.ad_partner_installs
WHERE   ad_partner = 'kochava'
AND     install_date >= date('{{ ds }}')
AND     app_name in ('BINGO BASH','GSN CASINO')
group by 1,2;



insert /*+direct,LABEL ('airflow-ua-ua_master_main_dag.kochava_importer_subdag-t_update_bingo_network_mapping')*/ into
bingo.kochava_singular_mapping
select x.* from mapping x
left join bingo.kochava_singular_mapping y
using(network_name,singular_media_sorce)
where y.network_name is null;

commit;
