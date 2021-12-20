DROP TABLE
    IF EXISTS airflow.temp_tableau_ua_kochava;
CREATE TEMPORARY TABLE airflow.temp_tableau_ua_kochava ( join_field_kochava_to_multipliers VARCHAR
(200), join_field_kochava_to_spend VARCHAR(200), join_field_kochava_financialmodel VARCHAR (200),
kochava_app_name VARCHAR(60), kochava_network_name VARCHAR (80), kochava_platform VARCHAR (80),
kochava_install_date_epoch VARCHAR(50), kochava_install_date DATE, kochava_existing_user VARCHAR
(30), kochava_unattributed_preload BOOLEAN , kochava_installs INTEGER , kochava_day1_bookings FLOAT, kochava_day2_bookings FLOAT,
kochava_day3_bookings FLOAT, kochava_day7_bookings FLOAT, kochava_day14_bookings FLOAT,
kochava_day30_bookings FLOAT, kochava_day60_bookings FLOAT, kochava_day90_bookings FLOAT,
kochava_day180_bookings FLOAT, kochava_day360_bookings FLOAT, kochava_day540_bookings FLOAT,
kochava_ltd_bookings FLOAT, kochava_day1_payers INTEGER , kochava_day2_payers INTEGER ,
kochava_day3_payers INTEGER , kochava_day7_payers INTEGER , kochava_day14_payers INTEGER ,
kochava_day30_payers INTEGER , kochava_day60_payers INTEGER , kochava_day90_payers INTEGER ,
kochava_day180_payers INTEGER , kochava_day360_payers INTEGER , kochava_day540_payers INTEGER ,
kochava_ltd_payers INTEGER , kochava_day1_retained INTEGER , kochava_day2_retained INTEGER ,
kochava_day3_retained INTEGER , kochava_day7_retained INTEGER , kochava_day14_retained INTEGER ,
kochava_day30_retained INTEGER , kochava_day60_retained INTEGER , kochava_day90_retained INTEGER ,
kochava_day180_retained INTEGER , kochava_day360_retained INTEGER , kochava_day540_retained INTEGER
, kochava_day1_cef FLOAT , kochava_day2_cef FLOAT , kochava_day3_cef FLOAT , kochava_day7_cef FLOAT
, kochava_day14_cef FLOAT , kochava_day30_cef FLOAT , kochava_day60_cef FLOAT , kochava_day90_cef
FLOAT , kochava_day180_cef FLOAT , kochava_day360_cef FLOAT , kochava_day540_cef FLOAT ,
kochava_todate_cef FLOAT , kochava_registered_cnt INTEGER ,kochava_day1_ad_rev FLOAT , kochava_day2_ad_rev FLOAT , kochava_day3_ad_rev FLOAT , kochava_day7_ad_rev FLOAT 
, kochava_day14_ad_rev  FLOAT , kochava_day30_ad_rev FLOAT  , kochava_day60_ad_rev FLOAT,  kochava_day90_ad_rev  FLOAT, kochava_day180_ad_rev  FLOAT, kochava_day360_ad_rev  FLOAT
,kochava_day540_ad_rev FLOAT,  kochava_todate_ad_rev FLOAT
,join_field_kochava_to_ml  VARCHAR(200)
) ON COMMIT PRESERVE ROWS;

INSERT /*+ direct,LABEL ('airflow-ua-ua_master_main_dag.tableau_subdag-t_update_tableau_ua_kochava')*/
INTO
    airflow.temp_tableau_ua_kochava
SELECT final.*,final.join_field_kochava_to_spend ||'||'||CASE WHEN kochava_unattributed_preload = true THEN 'true' ELSE  'false' END  AS join_field_kochava_to_ml
FROM (
SELECT
    REGEXP_REPLACE(UPPER(kochava.app_name),'[^A-Z0-9]','') ||'||'||
    CASE
        WHEN((kochava.network_name ilike '%google%') AND (kochava.model ilike '%IPAD%')) 
        THEN 'IPHONE' 
	WHEN(((kochava.network_name ilike '%facebook%') or (kochava.network_name ilike '%instagram%'))
	AND (kochava.model ilike '%IPAD%')) 
        THEN 'IPHONE' 						  
        WHEN(kochava.model ilike '%ANDROID%')
        THEN 'ANDROID'
        WHEN(kochava.model ilike '%IPHONE%')
        THEN 'IPHONE'
        WHEN(kochava.model ilike '%IPAD%')
        THEN 'IPAD'
        WHEN((kochava.model ilike '%AMAZON%')
            OR  (kochava.model ilike '%KINDLE%'))
        THEN 'AMAZON'
        WHEN(kochava.model ilike '%IPOD%')
        THEN 'IPHONE'
        WHEN(kochava.platform ilike '%android%')
        THEN 'ANDROID'
        WHEN((kochava.platform ilike '%amazon%')
            OR  (kochava.platform ilike '%kindle%'))
        THEN 'AMAZON'
        WHEN(kochava.platform ilike '%iphone%')
        THEN 'IPHONE'
        WHEN(kochava.platform ilike '%ipad%')
        THEN 'IPAD'
        WHEN(kochava.platform ilike '%ipod%')
        THEN 'IPHONE'
        WHEN(kochava.campaign_id ilike 'kobingobashandroid%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'kobingobashhdios%')
        THEN('IPAD')
        WHEN(kochava.campaign_id ilike 'kobingobashios%')
        THEN('IPHONE')
        WHEN(kochava.campaign_id ilike 'kofreshdeckpokerandroid%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'kofreshdeckpokerios%')
        THEN('IPHONE')
        WHEN(kochava.campaign_id ilike 'kofreshdeckpokerkindle%')
        THEN('AMAZON')
        WHEN(kochava.campaign_id ilike 'kogo-poker-android%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'kogo-poker-kindle%')
        THEN('AMAZON')
        WHEN(kochava.campaign_id ilike 'kogsn-grand-casino-ios%')
        THEN('IPHONE')
        WHEN(kochava.campaign_id ilike 'kogsn-grand-casino%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'kogsncasinoamazon%')
        THEN('AMAZON')
        WHEN(kochava.campaign_id ilike 'kogsncasinoandroid%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'kogsncasinoios%')
        THEN('IPHONE')
        WHEN(kochava.campaign_id ilike 'komybingobash%')
        THEN('AMAZON')
        WHEN(kochava.campaign_id ilike 'koslotsbash%')
        THEN('IPAD')
        WHEN(kochava.campaign_id ilike 'koslotsbashandroid%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'koslotsoffunandroid%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'koslotsoffunios%')
        THEN('IPHONE')
        WHEN(kochava.campaign_id ilike 'koslotsoffunkindle%')
        THEN('AMAZON')
        WHEN(kochava.campaign_id ilike 'kosolitaire-tripeaks-amazon%')
        THEN('AMAZON')
        WHEN(kochava.campaign_id ilike 'kosolitaire-tripeaks-ios%')
        THEN('IPHONE')
        WHEN(kochava.campaign_id ilike 'kosolitairetripeaks%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'kowheel-of-fortune-slots-amazon%')
        THEN('AMAZON')
        WHEN(kochava.campaign_id ilike 'kowheel-of-fortune-slots-ios%')
        THEN('IPHONE')
        WHEN(kochava.campaign_id ilike 'kowheel-of-fortune-slots-android%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'kosmash-ios%')
        THEN('IPHONE')
        WHEN(kochava.campaign_id ilike 'kosmash-android%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'kosmash-amazon%')
        THEN('AMAZON')
        WHEN(kochava.model ilike '%IOS%')
        THEN 'IPHONE'
        WHEN(kochava.platform ilike '%ios%')
        THEN 'IPHONE'
        WHEN(LOWER(kochava.model) IN ('pc',
                                      'desktop',
                                      'web',
                                      'canvas',
                                      'fb_browser'))
        THEN('FBCANVAS')
        WHEN(LOWER(kochava.platform) IN ('pc',
                                         'fb',
                                         'desktop',
                                         'web',
                                         'canvas'))
        THEN('FBCANVAS')
        ELSE 'UNKNOWN'
    END AS join_field_kochava_to_multipliers,
    REGEXP_REPLACE(UPPER(kochava.app_name),'[^A-Z0-9]','') ||'||'||
    CASE
        WHEN((kochava.network_name ilike '%smartlink%')
            AND (campaign_id IN ('kosolitairetripeaks179552cdd213d1b1ec8dadb11e078f',
                                 'kosolitairetripeaks179552cdd213d1b1eff27d8be17f25',
                                 'kosolitaire-tripeaks-ios53a8762dc3a45f7f36992c7065',
                                 'kosolitaire-tripeaks-amazon542c346dd4e8d62409345f40e6',
                                 'kosolitairetripeaks179552cdd213d1b1e2ed740d2e8bfa',
                                 'kosolitairetripeaks179552cdd213d1b1e8c45df91957da',
                                 'kosolitaire-tripeaks-ios53a8762dc3a4559d6e0f025582',
                                 'kosolitaire-tripeaks-amazon542c346dd4e8d2134a9d909d97')))
        THEN('PLAYANEXTYAHOO')
        WHEN(kochava.network_name ilike '%amazon%fire%')
        THEN('AMAZONMEDIAGROUP')
        WHEN(kochava.network_name ilike '%wake%screen%')
        THEN('AMAZONMEDIAGROUP')
        WHEN((kochava.app_name ilike '%bingo%bash%')
            AND (kochava.network_name ilike '%amazon%')
            AND (kochava.campaign_name ilike '%wakescreen%'))
        THEN('AMAZONMEDIAGROUP')
        WHEN(kochava.network_name ilike '%mobilda%')
        THEN('SNAPCHAT')
        WHEN(kochava.network_name ilike '%snap%')
        THEN('SNAPCHAT')
        WHEN(kochava.network_name ilike '%applovin%')
        THEN('APPLOVIN')
        WHEN(kochava.network_name ilike '%bing%')
        THEN('BINGSEARCH')
        WHEN(kochava.network_name ilike '%aura%ironsource%')
        THEN('IRONSOURCEPRELOAD')
        WHEN(kochava.network_name ilike '%supersonic%')
        THEN('IRONSOURCE')
        WHEN(kochava.network_name ilike '%iron%source%')
        THEN('IRONSOURCE')
        WHEN((kochava.network_name ilike '%pch%')
            OR  (kochava.network_name ilike '%liquid%'))
        THEN('LIQUIDWIRELESS')
        WHEN(kochava.network_name ilike '%tresensa%')
        THEN('TRESENSA')
        WHEN(kochava.network_name ilike '%digital%turbine%')
        THEN('DIGITALTURBINE')
        WHEN(((kochava.network_name ilike '%facebook%') or (kochava.network_name ilike '%instagram%'))
	    AND (kochava.site_id ilike '%fbcoupon%')
	    AND (DATE(kochava.install_day) <= '2018-12-30'))	
        THEN('FACEBOOKCOUPON')
	WHEN((kochava.network_name ilike '%facebook%')
            AND (LOWER(kochava.site_id) ilike '%creativetest%'))
        THEN('CREATIVETESTFB')
        WHEN(kochava.network_name ilike '%smartly%')
        THEN('SMARTLY')
	WHEN(kochava.network_name ilike '%Bidalgo%')
        THEN('BIDALGO')
        WHEN(kochava.network_name ilike '%moloco%')
        THEN('MOLOCO')
        WHEN(kochava.network_name ilike '%adaction%')
        THEN('ADACTION')
        WHEN(kochava.network_name ilike '%google%')
        THEN('GOOGLE')
        WHEN(kochava.network_name ilike '%admob%')
        THEN('GOOGLE')
        WHEN(kochava.network_name ilike '%adwords%')
        THEN('GOOGLE')
        WHEN(kochava.network_name ilike '%dauup%facebook%')
        THEN('DAUUPFACEBOOK')
        WHEN(kochava.network_name ilike '%dauup%android%')
        THEN('DAUUPNETWORK')
        WHEN(kochava.network_name ilike '%motive%')
        THEN('MOTIVEINTERACTIVE')
        WHEN(kochava.network_name ilike '%aarki%')
        THEN('AARKI')
        WHEN(kochava.network_name ilike '%unity%')
        THEN('UNITYADS')
        WHEN ((kochava.network_name ilike '%chartboost%')
            AND (kochava.campaign_name ilike '%_dd_%'))
        THEN('CHARTBOOSTDIRECTDEAL')
        WHEN (kochava.network_name ilike '%sprinklr%instagram%')
        THEN('SPRINKLRINSTAGRAM')
        WHEN (kochava.network_name ilike '%instagram%sprinklr%')
        THEN('SPRINKLRINSTAGRAM')
        WHEN (kochava.network_name ilike '%sprinklr%facebook%')
        THEN('SPRINKLRFACEBOOK')
        WHEN (kochava.network_name ilike '%facebook%sprinklr%')
        THEN('SPRINKLRFACEBOOK')
        WHEN (kochava.network_name ilike '%sprinklr%yahoo%')
        THEN('SPRINKLRYAHOO')
        WHEN (kochava.network_name ilike '%yahoo%sprinklr%')
        THEN('SPRINKLRYAHOO')
        WHEN((kochava.network_name ilike '%facebook%')
            AND (LOWER(kochava.site_id) ilike 'svl%'))
        THEN('STEALTHVENTURELABSFACEBOOK')
        WHEN ((kochava.network_name ilike '%facebook%')
            AND (LOWER(kochava.site_id) LIKE '[conacq]%'))
        THEN('CONSUMERACQUISITIONFACEBOOK')
        WHEN (kochava.network_name ilike '%sprinklr%')
        THEN('SPRINKLRFACEBOOK')
        WHEN ((kochava.network_name ilike '%instagram%')
            AND (kochava.app_name ilike '%wof%'))
        THEN('FACEBOOK')
        WHEN ((kochava.network_name ilike '%instagram%')
            AND (DATE(kochava.install_day)>='2015-12-10'))
        THEN('CONSUMERACQUISITIONINSTAGRAM')
        WHEN ((kochava.network_name ilike '%instagram%')
            AND (DATE(kochava.install_day)<'2015-12-10'))
        THEN('INSTAGRAM')
        WHEN(kochava.network_name ilike '%glispaandroid%')
        THEN('GLISPA')
        WHEN(kochava.network_name ilike '%apple%search%')
        THEN('APPLESEARCHADS')
        ELSE TRIM(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(kochava.network_name),
            '- ANDROID|- IOS|- FACEBOOK|- AMAZON|- ANROID|– ANDROID|– IOS|– FACEBOOK|– AMAZON|– ANROID|-ANDROID|-IOS|-FACEBOOK|-AMAZON|-ANROID|–ANDROID|–IOS|–FACEBOOK|–AMAZON|–ANROID'
            ,''),'[^A-Z0-9]',''))
    END ||'||'||
    CASE
        WHEN((kochava.network_name ilike '%google%') AND (kochava.model ilike '%IPAD%')) 
        THEN 'IPHONE' 
	WHEN(((kochava.network_name ilike '%facebook%') or (kochava.network_name ilike '%instagram%'))
	AND (kochava.model ilike '%IPAD%')) 
        THEN 'IPHONE'
        WHEN(kochava.model ilike '%ANDROID%')
        THEN 'ANDROID'
        WHEN(kochava.model ilike '%IPHONE%')
        THEN 'IPHONE'
        WHEN(kochava.model ilike '%IPAD%')
        THEN 'IPAD'
        WHEN((kochava.model ilike '%AMAZON%')
            OR  (kochava.model ilike '%KINDLE%'))
        THEN 'AMAZON'
        WHEN(kochava.model ilike '%IPOD%')
        THEN 'IPHONE'
        WHEN(kochava.platform ilike '%android%')
        THEN 'ANDROID'
        WHEN((kochava.platform ilike '%amazon%')
            OR  (kochava.platform ilike '%kindle%'))
        THEN 'AMAZON'
        WHEN(kochava.platform ilike '%iphone%')
        THEN 'IPHONE'
        WHEN(kochava.platform ilike '%ipad%')
        THEN 'IPAD'
        WHEN(kochava.platform ilike '%ipod%')
        THEN 'IPHONE'
        WHEN(kochava.campaign_id ilike 'kobingobashandroid%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'kobingobashhdios%')
        THEN('IPAD')
        WHEN(kochava.campaign_id ilike 'kobingobashios%')
        THEN('IPHONE')
        WHEN(kochava.campaign_id ilike 'kofreshdeckpokerandroid%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'kofreshdeckpokerios%')
        THEN('IPHONE')
        WHEN(kochava.campaign_id ilike 'kofreshdeckpokerkindle%')
        THEN('AMAZON')
        WHEN(kochava.campaign_id ilike 'kogo-poker-android%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'kogo-poker-kindle%')
        THEN('AMAZON')
        WHEN(kochava.campaign_id ilike 'kogsn-grand-casino-ios%')
        THEN('IPHONE')
        WHEN(kochava.campaign_id ilike 'kogsn-grand-casino%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'kogsncasinoamazon%')
        THEN('AMAZON')
        WHEN(kochava.campaign_id ilike 'kogsncasinoandroid%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'kogsncasinoios%')
        THEN('IPHONE')
        WHEN(kochava.campaign_id ilike 'komybingobash%')
        THEN('AMAZON')
        WHEN(kochava.campaign_id ilike 'koslotsbash%')
        THEN('IPAD')
        WHEN(kochava.campaign_id ilike 'koslotsbashandroid%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'koslotsoffunandroid%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'koslotsoffunios%')
        THEN('IPHONE')
        WHEN(kochava.campaign_id ilike 'koslotsoffunkindle%')
        THEN('AMAZON')
        WHEN(kochava.campaign_id ilike 'kosolitaire-tripeaks-amazon%')
        THEN('AMAZON')
        WHEN(kochava.campaign_id ilike 'kosolitaire-tripeaks-ios%')
        THEN('IPHONE')
        WHEN(kochava.campaign_id ilike 'kosolitairetripeaks%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'kowheel-of-fortune-slots-amazon%')
        THEN('AMAZON')
        WHEN(kochava.campaign_id ilike 'kowheel-of-fortune-slots-ios%')
        THEN('IPHONE')
        WHEN(kochava.campaign_id ilike 'kowheel-of-fortune-slots-android%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'kosmash-ios%')
        THEN('IPHONE')
        WHEN(kochava.campaign_id ilike 'kosmash-android%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'kosmash-amazon%')
        THEN('AMAZON')
        WHEN(kochava.model ilike '%IOS%')
        THEN 'IPHONE'
        WHEN(kochava.platform ilike '%ios%')
        THEN 'IPHONE'
        WHEN(LOWER(kochava.model) IN ('pc',
                                      'desktop',
                                      'web',
                                      'canvas',
                                      'fb_browser'))
        THEN('FBCANVAS')
        WHEN(LOWER(kochava.platform) IN ('pc',
                                         'fb',
                                         'desktop',
                                         'web',
                                         'canvas'))
        THEN('FBCANVAS')
        ELSE 'UNKNOWN'
    END ||'||'|| CAST(CAST(EXTRACT(EPOCH FROM DATE_TRUNC('day',kochava.install_day)) AS INTEGER) AS
    VARCHAR) AS join_field_kochava_to_spend,
    REGEXP_REPLACE(UPPER(kochava.app_name),'[^A-Z0-9]','') ||'||'||
    CASE
        WHEN((kochava.network_name ilike '%smartlink%')
            AND (campaign_id IN ('kosolitairetripeaks179552cdd213d1b1ec8dadb11e078f',
                                 'kosolitairetripeaks179552cdd213d1b1eff27d8be17f25',
                                 'kosolitaire-tripeaks-ios53a8762dc3a45f7f36992c7065',
                                 'kosolitaire-tripeaks-amazon542c346dd4e8d62409345f40e6',
                                 'kosolitairetripeaks179552cdd213d1b1e2ed740d2e8bfa',
                                 'kosolitairetripeaks179552cdd213d1b1e8c45df91957da',
                                 'kosolitaire-tripeaks-ios53a8762dc3a4559d6e0f025582',
                                 'kosolitaire-tripeaks-amazon542c346dd4e8d2134a9d909d97')))
        THEN('PLAYANEXTYAHOO')
        WHEN(kochava.network_name ilike '%amazon%fire%')
        THEN('AMAZONMEDIAGROUP')
        WHEN(kochava.network_name ilike '%wake%screen%')
        THEN('AMAZONMEDIAGROUP')
        WHEN((kochava.app_name ilike '%bingo%bash%')
            AND (kochava.network_name ilike '%amazon%')
            AND (kochava.campaign_name ilike '%wakescreen%'))
        THEN('AMAZONMEDIAGROUP')
        WHEN(kochava.network_name ilike '%mobilda%')
        THEN('SNAPCHAT')
        WHEN(kochava.network_name ilike '%snap%')
        THEN('SNAPCHAT')
        WHEN(kochava.network_name ilike '%applovin%')
        THEN('APPLOVIN')
        WHEN(kochava.network_name ilike '%bing%')
        THEN('BINGSEARCH')
        WHEN(kochava.network_name ilike '%aura%ironsource%')
        THEN('IRONSOURCEPRELOAD')
        WHEN(kochava.network_name ilike '%supersonic%')
        THEN('IRONSOURCE')
        WHEN(kochava.network_name ilike '%iron%source%')
        THEN('IRONSOURCE')
        WHEN((kochava.network_name ilike '%pch%')
            OR  (kochava.network_name ilike '%liquid%'))
        THEN('LIQUIDWIRELESS')
        WHEN(kochava.network_name ilike '%tresensa%')
        THEN('TRESENSA')
        WHEN(kochava.network_name ilike '%digital%turbine%')
        THEN('DIGITALTURBINE')
        WHEN(kochava.network_name ilike '%moloco%')
        THEN('MOLOCO')
        WHEN(kochava.network_name ilike '%adaction%')
        THEN('ADACTION')
        WHEN(kochava.network_name ilike '%google%')
        THEN('GOOGLE')
        WHEN(kochava.network_name ilike '%admob%')
        THEN('GOOGLE')
        WHEN(kochava.network_name ilike '%adwords%')
        THEN('GOOGLE')
        WHEN(kochava.network_name ilike '%dauup%android%')
        THEN('DAUUPNETWORK')
        WHEN(kochava.network_name ilike '%motive%')
        THEN('MOTIVEINTERACTIVE')
        WHEN(kochava.network_name ilike '%aarki%')
        THEN('AARKI')
        WHEN(kochava.network_name ilike '%unity%')
        THEN('UNITYADS')
        WHEN ((kochava.network_name ilike '%chartboost%')
            AND (kochava.campaign_name ilike '%_dd_%'))
        THEN('CHARTBOOSTDIRECTDEAL')
        WHEN(((kochava.network_name ilike '%facebook%') or (kochava.network_name ilike '%instagram%')) 
	    AND (kochava.site_id ilike '%fbcoupon%')
	    AND (DATE(kochava.install_day) <= '2018-12-30'))
        THEN('FACEBOOKCOUPON')
	WHEN((kochava.network_name ilike '%facebook%')
            AND (LOWER(kochava.site_id) ilike '%creativetest%'))
        THEN('CREATIVETESTFB')
        WHEN(kochava.network_name ilike '%smartly%')
        THEN('SMARTLY')
	WHEN(kochava.network_name ilike '%Bidalgo%')
        THEN('BIDALGO')
        WHEN (kochava.network_name ilike '%sprinklr%instagram%')
        THEN('SPRINKLRINSTAGRAM')
        WHEN (kochava.network_name ilike '%instagram%sprinklr%')
        THEN('SPRINKLRINSTAGRAM')
        WHEN (kochava.network_name ilike '%sprinklr%facebook%')
        THEN('SPRINKLRFACEBOOK')
        WHEN (kochava.network_name ilike '%facebook%sprinklr%')
        THEN('SPRINKLRFACEBOOK')
        WHEN (kochava.network_name ilike '%sprinklr%yahoo%')
        THEN('SPRINKLRYAHOO')
        WHEN (kochava.network_name ilike '%yahoo%sprinklr%')
        THEN('SPRINKLRYAHOO')
        WHEN((kochava.network_name ilike '%facebook%')
            AND (kochava.site_id ilike 'svl%'))
        THEN('STEALTHVENTURELABSFACEBOOK')
        WHEN ((kochava.network_name ilike '%facebook%')
            AND (kochava.site_id ilike '[conacq]%'))
        THEN('CONSUMERACQUISITIONFACEBOOK')
        WHEN (kochava.network_name ilike '%sprinklr%')
        THEN('SPRINKLRFACEBOOK')
        WHEN ((kochava.network_name ilike '%instagram%')
            AND (kochava.app_name ilike '%wof%'))
        THEN('FACEBOOK')
        WHEN ((kochava.network_name ilike '%instagram%')
            AND (DATE(kochava.install_day)>='2015-12-10'))
        THEN('CONSUMERACQUISITIONINSTAGRAM')
        WHEN ((kochava.network_name ilike '%instagram%')
            AND (DATE(kochava.install_day)<'2015-12-10'))
        THEN('INSTAGRAM')
        WHEN(kochava.network_name ilike '%dauup%facebook%')
        THEN('DAUUPFACEBOOK')
        WHEN(kochava.network_name ilike '%glispaandroid%')
        THEN('GLISPA')
        WHEN(kochava.network_name ilike '%apple%search%')
        THEN('APPLESEARCHADS')
        ELSE TRIM(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(kochava.network_name),
            '- ANDROID|- IOS|- FACEBOOK|- AMAZON|- ANROID|– ANDROID|– IOS|– FACEBOOK|– AMAZON|– ANROID|-ANDROID|-IOS|-FACEBOOK|-AMAZON|-ANROID|–ANDROID|–IOS|–FACEBOOK|–AMAZON|–ANROID'
            ,''),'[^A-Z0-9]',''))
    END ||'||'||
    CASE
        WHEN((kochava.network_name ilike '%google%') AND (kochava.model ilike '%IPAD%')) 
        THEN 'IPHONE'
	WHEN(((kochava.network_name ilike '%facebook%') or (kochava.network_name ilike '%instagram%'))
	AND (kochava.model ilike '%IPAD%')) 
        THEN 'IPHONE' 
        WHEN(kochava.model ilike '%ANDROID%')
        THEN 'ANDROID'
        WHEN(kochava.model ilike '%IPHONE%')
        THEN 'IPHONE'
        WHEN(kochava.model ilike '%IPAD%')
        THEN 'IPAD'
        WHEN((kochava.model ilike '%AMAZON%')
            OR  (kochava.model ilike '%KINDLE%'))
        THEN 'AMAZON'
        WHEN(kochava.model ilike '%IPOD%')
        THEN 'IPHONE'
        WHEN(kochava.platform ilike '%android%')
        THEN 'ANDROID'
        WHEN((kochava.platform ilike '%amazon%')
            OR  (kochava.platform ilike '%kindle%'))
        THEN 'AMAZON'
        WHEN(kochava.platform ilike '%iphone%')
        THEN 'IPHONE'
        WHEN(kochava.platform ilike '%ipad%')
        THEN 'IPAD'
        WHEN(kochava.platform ilike '%ipod%')
        THEN 'IPHONE'
        WHEN(kochava.campaign_id ilike 'kobingobashandroid%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'kobingobashhdios%')
        THEN('IPAD')
        WHEN(kochava.campaign_id ilike 'kobingobashios%')
        THEN('IPHONE')
        WHEN(kochava.campaign_id ilike 'kofreshdeckpokerandroid%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'kofreshdeckpokerios%')
        THEN('IPHONE')
        WHEN(kochava.campaign_id ilike 'kofreshdeckpokerkindle%')
        THEN('AMAZON')
        WHEN(kochava.campaign_id ilike 'kogo-poker-android%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'kogo-poker-kindle%')
        THEN('AMAZON')
        WHEN(kochava.campaign_id ilike 'kogsn-grand-casino-ios%')
        THEN('IPHONE')
        WHEN(kochava.campaign_id ilike 'kogsn-grand-casino%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'kogsncasinoamazon%')
        THEN('AMAZON')
        WHEN(kochava.campaign_id ilike 'kogsncasinoandroid%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'kogsncasinoios%')
        THEN('IPHONE')
        WHEN(kochava.campaign_id ilike 'komybingobash%')
        THEN('AMAZON')
        WHEN(kochava.campaign_id ilike 'koslotsbash%')
        THEN('IPAD')
        WHEN(kochava.campaign_id ilike 'koslotsbashandroid%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'koslotsoffunandroid%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'koslotsoffunios%')
        THEN('IPHONE')
        WHEN(kochava.campaign_id ilike 'koslotsoffunkindle%')
        THEN('AMAZON')
        WHEN(kochava.campaign_id ilike 'kosolitaire-tripeaks-amazon%')
        THEN('AMAZON')
        WHEN(kochava.campaign_id ilike 'kosolitaire-tripeaks-ios%')
        THEN('IPHONE')
        WHEN(kochava.campaign_id ilike 'kosolitairetripeaks%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'kowheel-of-fortune-slots-amazon%')
        THEN('AMAZON')
        WHEN(kochava.campaign_id ilike 'kowheel-of-fortune-slots-ios%')
        THEN('IPHONE')
        WHEN(kochava.campaign_id ilike 'kowheel-of-fortune-slots-android%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'kosmash-ios%')
        THEN('IPHONE')
        WHEN(kochava.campaign_id ilike 'kosmash-android%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'kosmash-amazon%')
        THEN('AMAZON')
        WHEN(kochava.model ilike '%IOS%')
        THEN 'IPHONE'
        WHEN(kochava.platform ilike '%ios%')
        THEN 'IPHONE'
        WHEN(LOWER(kochava.model) IN ('pc',
                                      'desktop',
                                      'web',
                                      'canvas',
                                      'fb_browser'))
        THEN('FBCANVAS')
        WHEN(LOWER(kochava.platform) IN ('pc',
                                         'fb',
                                         'desktop',
                                         'web',
                                         'canvas'))
        THEN('FBCANVAS')
        ELSE 'UNKNOWN'
    END ||'||'|| CAST(CAST(EXTRACT(EPOCH FROM DATE_TRUNC('day',kochava.install_day)) AS INTEGER) AS
    VARCHAR)                                               AS join_field_kochava_to_financialmodel,
    REGEXP_REPLACE(UPPER(kochava.app_name),'[^A-Z0-9]','') AS kochava_app_name_clean,
    CASE
        WHEN((kochava.network_name ilike '%smartlink%')
            AND (campaign_id IN ('kosolitairetripeaks179552cdd213d1b1ec8dadb11e078f',
                                 'kosolitairetripeaks179552cdd213d1b1eff27d8be17f25',
                                 'kosolitaire-tripeaks-ios53a8762dc3a45f7f36992c7065',
                                 'kosolitaire-tripeaks-amazon542c346dd4e8d62409345f40e6',
                                 'kosolitairetripeaks179552cdd213d1b1e2ed740d2e8bfa',
                                 'kosolitairetripeaks179552cdd213d1b1e8c45df91957da',
                                 'kosolitaire-tripeaks-ios53a8762dc3a4559d6e0f025582',
                                 'kosolitaire-tripeaks-amazon542c346dd4e8d2134a9d909d97')))
        THEN('PLAYANEXTYAHOO')
        WHEN(kochava.network_name ilike '%amazon%fire%')
        THEN('AMAZONMEDIAGROUP')
        WHEN(kochava.network_name ilike '%wake%screen%')
        THEN('AMAZONMEDIAGROUP')
        WHEN((kochava.app_name ilike '%bingo%bash%')
            AND (kochava.network_name ilike '%amazon%')
            AND (kochava.campaign_name ilike '%wakescreen%'))
        THEN('AMAZONMEDIAGROUP')
        WHEN(kochava.network_name ilike '%mobilda%')
        THEN('SNAPCHAT')
        WHEN(kochava.network_name ilike '%snap%')
        THEN('SNAPCHAT')
        WHEN(kochava.network_name ilike '%applovin%')
        THEN('APPLOVIN')
        WHEN(kochava.network_name ilike '%bing%')
        THEN('BINGSEARCH')
        WHEN(kochava.network_name ilike '%aura%ironsource%')
        THEN('IRONSOURCEPRELOAD')
        WHEN(kochava.network_name ilike '%supersonic%')
        THEN('IRONSOURCE')
        WHEN(kochava.network_name ilike '%iron%source%')
        THEN('IRONSOURCE')
        WHEN((kochava.network_name ilike '%pch%')
            OR  (kochava.network_name ilike '%liquid%'))
        THEN('LIQUIDWIRELESS')
        WHEN(kochava.network_name ilike '%tresensa%')
        THEN('TRESENSA')
        WHEN(kochava.network_name ilike '%digital%turbine%')
        THEN('DIGITALTURBINE')
        WHEN(kochava.network_name ilike '%moloco%')
        THEN('MOLOCO')
        WHEN(kochava.network_name ilike '%adaction%')
        THEN('ADACTION')
        WHEN(kochava.network_name ilike '%google%')
        THEN('GOOGLE')
        WHEN(kochava.network_name ilike '%admob%')
        THEN('GOOGLE')
        WHEN(kochava.network_name ilike '%adwords%')
        THEN('GOOGLE')
        WHEN(kochava.network_name ilike '%dauup%android%')
        THEN('DAUUPNETWORK')
        WHEN(kochava.network_name ilike '%motive%')
        THEN('MOTIVEINTERACTIVE')
        WHEN(kochava.network_name ilike '%aarki%')
        THEN('AARKI')
        WHEN(kochava.network_name ilike '%unity%')
        THEN('UNITYADS')
        WHEN ((kochava.network_name ilike '%chartboost%')
            AND (kochava.campaign_name ilike '%_dd_%'))
        THEN('CHARTBOOSTDIRECTDEAL')
        WHEN(((kochava.network_name ilike '%facebook%') or (kochava.network_name ilike '%instagram%'))
	    AND (kochava.site_id ilike '%fbcoupon%')
	    AND (DATE(kochava.install_day) <= '2018-12-30'))	
        THEN('FACEBOOKCOUPON')
	WHEN((kochava.network_name ilike '%facebook%')
            AND (LOWER(kochava.site_id) ilike '%creativetest%'))
        THEN('CREATIVETESTFB')
        WHEN(kochava.network_name ilike '%smartly%')
        THEN('SMARTLY')
	WHEN(kochava.network_name ilike '%Bidalgo%')
        THEN('BIDALGO')
        WHEN (kochava.network_name ilike '%sprinklr%instagram%')
        THEN('SPRINKLRINSTAGRAM')
        WHEN (kochava.network_name ilike '%instagram%sprinklr%')
        THEN('SPRINKLRINSTAGRAM')
        WHEN (kochava.network_name ilike '%sprinklr%facebook%')
        THEN('SPRINKLRFACEBOOK')
        WHEN (kochava.network_name ilike '%facebook%sprinklr%')
        THEN('SPRINKLRFACEBOOK')
        WHEN (kochava.network_name ilike '%sprinklr%yahoo%')
        THEN('SPRINKLRYAHOO')
        WHEN (kochava.network_name ilike '%yahoo%sprinklr%')
        THEN('SPRINKLRYAHOO')
        WHEN((kochava.network_name ilike '%facebook%')
            AND (kochava.site_id ilike 'svl%'))
        THEN('STEALTHVENTURELABSFACEBOOK')
        WHEN ((kochava.network_name ilike '%facebook%')
            AND (kochava.site_id ilike '[conacq]%'))
        THEN('CONSUMERACQUISITIONFACEBOOK')
        WHEN (kochava.network_name ilike '%sprinklr%')
        THEN('SPRINKLRFACEBOOK')
        WHEN ((kochava.network_name ilike '%instagram%')
            AND (kochava.app_name ilike '%wof%'))
        THEN('FACEBOOK')
        WHEN ((kochava.network_name ilike '%instagram%')
            AND (DATE(kochava.install_day)>='2015-12-10'))
        THEN('CONSUMERACQUISITIONINSTAGRAM')
        WHEN ((kochava.network_name ilike '%instagram%')
            AND (DATE(kochava.install_day)<'2015-12-10'))
        THEN('INSTAGRAM')
        WHEN(kochava.network_name ilike '%dauup%facebook%')
        THEN('DAUUPFACEBOOK')
        WHEN(kochava.network_name ilike '%glispaandroid%')
        THEN('GLISPA')
        WHEN(kochava.network_name ilike '%apple%search%')
        THEN('APPLESEARCHADS')
        ELSE TRIM(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(kochava.network_name),
            '- ANDROID|- IOS|- FACEBOOK|- AMAZON|- ANROID|– ANDROID|– IOS|– FACEBOOK|– AMAZON|– ANROID|-ANDROID|-IOS|-FACEBOOK|-AMAZON|-ANROID|–ANDROID|–IOS|–FACEBOOK|–AMAZON|–ANROID'
            ,''),'[^A-Z0-9]',''))
    END AS kochava_network_name_clean,
    CASE
        WHEN((kochava.network_name ilike '%google%') AND (kochava.model ilike '%IPAD%')) 
        THEN 'IPHONE' 
        WHEN(((kochava.network_name ilike '%facebook%') or (kochava.network_name ilike '%instagram%'))
	AND (kochava.model ilike '%IPAD%')) 
        THEN 'IPHONE' 
        WHEN(kochava.model ilike '%ANDROID%')
        THEN 'ANDROID'
        WHEN(kochava.model ilike '%IPHONE%')
        THEN 'IPHONE'
        WHEN(kochava.model ilike '%IPAD%')
        THEN 'IPAD'
        WHEN((kochava.model ilike '%AMAZON%')
            OR  (kochava.model ilike '%KINDLE%'))
        THEN 'AMAZON'
        WHEN(kochava.model ilike '%IPOD%')
        THEN 'IPHONE'
        WHEN(kochava.platform ilike '%android%')
        THEN 'ANDROID'
        WHEN((kochava.platform ilike '%amazon%')
            OR  (kochava.platform ilike '%kindle%'))
        THEN 'AMAZON'
        WHEN(kochava.platform ilike '%iphone%')
        THEN 'IPHONE'
        WHEN(kochava.platform ilike '%ipad%')
        THEN 'IPAD'
        WHEN(kochava.platform ilike '%ipod%')
        THEN 'IPHONE'
        WHEN(kochava.campaign_id ilike 'kobingobashandroid%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'kobingobashhdios%')
        THEN('IPAD')
        WHEN(kochava.campaign_id ilike 'kobingobashios%')
        THEN('IPHONE')
        WHEN(kochava.campaign_id ilike 'kofreshdeckpokerandroid%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'kofreshdeckpokerios%')
        THEN('IPHONE')
        WHEN(kochava.campaign_id ilike 'kofreshdeckpokerkindle%')
        THEN('AMAZON')
        WHEN(kochava.campaign_id ilike 'kogo-poker-android%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'kogo-poker-kindle%')
        THEN('AMAZON')
        WHEN(kochava.campaign_id ilike 'kogsn-grand-casino-ios%')
        THEN('IPHONE')
        WHEN(kochava.campaign_id ilike 'kogsn-grand-casino%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'kogsncasinoamazon%')
        THEN('AMAZON')
        WHEN(kochava.campaign_id ilike 'kogsncasinoandroid%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'kogsncasinoios%')
        THEN('IPHONE')
        WHEN(kochava.campaign_id ilike 'komybingobash%')
        THEN('AMAZON')
        WHEN(kochava.campaign_id ilike 'koslotsbash%')
        THEN('IPAD')
        WHEN(kochava.campaign_id ilike 'koslotsbashandroid%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'koslotsoffunandroid%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'koslotsoffunios%')
        THEN('IPHONE')
        WHEN(kochava.campaign_id ilike 'koslotsoffunkindle%')
        THEN('AMAZON')
        WHEN(kochava.campaign_id ilike 'kosolitaire-tripeaks-amazon%')
        THEN('AMAZON')
        WHEN(kochava.campaign_id ilike 'kosolitaire-tripeaks-ios%')
        THEN('IPHONE')
        WHEN(kochava.campaign_id ilike 'kosolitairetripeaks%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'kowheel-of-fortune-slots-amazon%')
        THEN('AMAZON')
        WHEN(kochava.campaign_id ilike 'kowheel-of-fortune-slots-ios%')
        THEN('IPHONE')
        WHEN(kochava.campaign_id ilike 'kowheel-of-fortune-slots-android%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'kosmash-ios%')
        THEN('IPHONE')
        WHEN(kochava.campaign_id ilike 'kosmash-android%')
        THEN('ANDROID')
        WHEN(kochava.campaign_id ilike 'kosmash-amazon%')
        THEN('AMAZON')
        WHEN(kochava.model ilike '%IOS%')
        THEN 'IPHONE'
        WHEN(kochava.platform ilike '%ios%')
        THEN 'IPHONE'
        WHEN(LOWER(kochava.model) IN ('pc',
                                      'desktop',
                                      'web',
                                      'canvas',
                                      'fb_browser'))
        THEN('FBCANVAS')
        WHEN(LOWER(kochava.platform) IN ('pc',
                                         'fb',
                                         'desktop',
                                         'web',
                                         'canvas'))
        THEN('FBCANVAS')
        ELSE 'UNKNOWN'
    END                                                                   AS kochava_platform_clean,
    CAST(CAST(EXTRACT(EPOCH FROM DATE_TRUNC('day',kochava.install_day)) AS INTEGER) AS VARCHAR) AS
                                                   kochava_install_date_epoch_clean,
    DATE(DATE_TRUNC('day',kochava.install_day)) AS kochava_install_date,
    existing_user,
    unattributed_preload 	 AS kochava_unattributed_preload, 
    NVL(SUM(kochava.installs),0)        AS kochava_installs,
    NVL(SUM(kochava.day1_bookings),0)   AS kochava_day1_bookings,
    NVL(SUM(kochava.day2_bookings)  ,0) AS kochava_day2_bookings,
    NVL(SUM(kochava.day3_bookings),0)   AS kochava_day3_bookings,
    NVL(SUM(kochava.day7_bookings) ,0)  AS kochava_day7_bookings,
    NVL(SUM(kochava.day14_bookings),0)  AS kochava_day14_bookings,
    NVL(SUM(kochava.day30_bookings),0)  AS kochava_day30_bookings,
    NVL(SUM(kochava.day60_bookings),0)  AS kochava_day60_bookings,
    NVL(SUM(kochava.day90_bookings) ,0) AS kochava_day90_bookings,
    NVL(SUM(kochava.day180_bookings),0) AS kochava_day180_bookings,
    NVL(SUM(kochava.day360_bookings),0) AS kochava_day360_bookings,
    NVL(SUM(kochava.day540_bookings),0) AS kochava_day540_bookings,
    NVL(SUM(kochava.todate_bookings),0) AS kochava_ltd_bookings,
    NVL(SUM(kochava.day1_payers)  ,0)   AS kochava_day1_payers,
    NVL(SUM(kochava.day2_payers) ,0)    AS kochava_day2_payers,
    NVL(SUM(kochava.day3_payers) ,0)    AS kochava_day3_payers,
    NVL(SUM(kochava.day7_payers) ,0)    AS kochava_day7_payers,
    NVL(SUM(kochava.day14_payers),0)    AS kochava_day14_payers,
    NVL(SUM(kochava.day30_payers) ,0)   AS kochava_day30_payers,
    NVL(SUM(kochava.day60_payers) ,0)   AS kochava_day60_payers,
    NVL(SUM(kochava.day90_payers) ,0)   AS kochava_day90_payers,
    NVL(SUM(kochava.day180_payers) ,0)  AS kochava_day180_payers,
    NVL(SUM(kochava.day360_payers) ,0)  AS kochava_day360_payers,
    NVL(SUM(kochava.day540_payers) ,0)  AS kochava_day540_payers,
    NVL(SUM(kochava.todate_payers) ,0)  AS kochava_ltd_payers,
    NVL(SUM(kochava.day1_retained) ,0)  AS kochava_day1_retained,
    NVL(SUM(kochava.day2_retained) ,0)  AS kochava_day2_retained,
    NVL(SUM(kochava.day3_retained) ,0)  AS kochava_day3_retained,
    NVL(SUM(kochava.day7_retained) ,0)  AS kochava_day7_retained,
    NVL(SUM(kochava.day14_retained),0)  AS kochava_day14_retained,
    NVL(SUM(kochava.day30_retained),0)  AS kochava_day30_retained,
    NVL(SUM(kochava.day60_retained),0)  AS kochava_day60_retained,
    NVL(SUM(kochava.day90_retained) ,0) AS kochava_day90_retained,
    NVL(SUM(kochava.day180_retained),0) AS kochava_day180_retained,
    NVL(SUM(kochava.day360_retained),0) AS kochava_day360_retained,
    NVL(SUM(kochava.day540_retained),0) AS kochava_day540_retained,
    NVL(SUM(
        CASE
            WHEN(kochava.app_name IN ('WORLDWINNER',
                                      'SPARCADE',
                                      'PHOENIX', 
                                      'WWSOLRUSH'))
            THEN(kochava.day1_cef)
            ELSE(kochava.day1_bookings)
        END),0) AS kochava_day1_cef,
    NVL(SUM(
        CASE
            WHEN(kochava.app_name IN ('WORLDWINNER',
                                      'SPARCADE',
                                      'PHOENIX', 
                                      'WWSOLRUSH'))
            THEN(kochava.day2_cef)
            ELSE(kochava.day2_bookings)
        END),0) AS kochava_day2_cef,
    NVL(SUM(
        CASE
            WHEN(kochava.app_name IN ('WORLDWINNER',
                                      'SPARCADE',
                                      'PHOENIX', 
                                      'WWSOLRUSH'))
            THEN(kochava.day3_cef)
            ELSE(kochava.day3_bookings)
        END) ,0) AS kochava_day3_cef,
    NVL(SUM(
        CASE
            WHEN(kochava.app_name IN ('WORLDWINNER',
                                      'SPARCADE',
                                      'PHOENIX', 
                                      'WWSOLRUSH'))
            THEN(kochava.day7_cef)
            ELSE(kochava.day7_bookings)
        END),0) AS kochava_day7_cef,
    NVL(SUM(
        CASE
            WHEN(kochava.app_name IN ('WORLDWINNER',
                                      'SPARCADE',
                                      'PHOENIX', 
                                      'WWSOLRUSH'))
            THEN(kochava.day14_cef)
            ELSE(kochava.day14_bookings)
        END),0) AS kochava_day14_cef,
    NVL(SUM(
        CASE
            WHEN(kochava.app_name IN ('WORLDWINNER',
                                      'SPARCADE',
                                      'PHOENIX', 
                                      'WWSOLRUSH'))
            THEN(kochava.day30_cef)
            ELSE(kochava.day30_bookings)
        END),0) AS kochava_day30_cef,
    NVL(SUM(
        CASE
            WHEN(kochava.app_name IN ('WORLDWINNER',
                                      'SPARCADE',
                                      'PHOENIX', 
                                      'WW SOLRUSH'))
            THEN(kochava.day60_cef)
            ELSE(kochava.day60_bookings)
        END),0) AS kochava_day60_cef,
    NVL(SUM(
        CASE
            WHEN(kochava.app_name IN ('WORLDWINNER',
                                      'SPARCADE',
                                      'PHOENIX', 
                                      'WWSOLRUSH'))
            THEN(kochava.day90_cef)
            ELSE(kochava.day90_bookings)
        END),0) AS kochava_day90_cef,
    NVL(SUM(
        CASE
            WHEN(kochava.app_name IN ('WORLDWINNER',
                                      'SPARCADE',
                                      'PHOENIX', 
                                      'WWSOLRUSH'))
            THEN(kochava.day180_cef)
            ELSE(kochava.day180_bookings)
        END),0) AS kochava_day180_cef,
    NVL(SUM(
        CASE
            WHEN(kochava.app_name IN ('WORLDWINNER',
                                      'SPARCADE',
                                      'PHOENIX', 
                                      'WWSOLRUSH'))
            THEN(kochava.day360_cef)
            ELSE(kochava.day360_bookings)
        END),0) AS kochava_day360_cef,
    NVL(SUM(
        CASE
            WHEN(kochava.app_name IN ('WORLDWINNER',
                                      'SPARCADE',
                                      'PHOENIX', 
                                      'WWSOLRUSH'))
            THEN(kochava.day540_cef)
            ELSE(kochava.day540_bookings)
        END),0) AS kochava_day540_cef,
    NVL(SUM(
        CASE
            WHEN(kochava.app_name IN ('WORLDWINNER',
                                      'SPARCADE',
                                      'PHOENIX', 
                                      'WWSOLRUSH'))
            THEN(kochava.todate_cef)
            ELSE(kochava.todate_bookings)
        END),0)                    AS kochava_todate_cef,
    NVL(SUM(kochava.registered_cnt),0) AS kochava_registered_cnt,
                NVL(SUM(kochava.day1_ad_rev),0)::FLOAT          AS day1_ad_rev,
	        NVL(SUM(kochava.day2_ad_rev),0)::FLOAT          AS day2_ad_rev,
	        NVL(SUM(kochava.day3_ad_rev),0)::FLOAT          AS day3_ad_rev,
	       NVL( SUM(kochava.day7_ad_rev),0)::FLOAT          AS day7_ad_rev,
	       NVL( SUM(kochava.day14_ad_rev),0)::FLOAT          AS day14_ad_rev,
	       NVL( SUM(kochava.day30_ad_rev),0)::FLOAT          AS day30_ad_rev,
	       NVL( SUM(kochava.day60_ad_rev),0)::FLOAT          AS day60_ad_rev,
	       NVL( SUM(kochava.day90_ad_rev),0)::FLOAT          AS day90_ad_rev,
	       NVL( SUM(kochava.day180_ad_rev),0)::FLOAT          AS day180_ad_rev,
	       NVL( SUM(kochava.day360_ad_rev),0)::FLOAT          AS day360_ad_rev,
	       NVL( SUM(kochava.day540_ad_rev),0)::FLOAT          AS day540_ad_rev,
	       NVL( SUM(kochava.todate_ad_rev),0)::FLOAT          AS todate_ad_rev
   
FROM
    (
        -- SELECT
        --     platform,
        --     app_name,
        --     model,
        --     install_day,
        --     campaign_name,
        --     campaign_id,
        --     site_id,
        --     network_name,
        --     creative,
        --     country_code,
        --     request_keyword,
        --     'new_user'        AS existing_user,
	    -- CASE 
		-- WHEN (network_name='unattributed' 
		--       AND ((last_click_network_name ilike '%digital%turbine%') 
		-- 	OR (last_click_network_name ilike '%aura%ironsource%'))
		-- ) 
		-- THEN true 
		-- ELSE false 
	    -- END AS unattributed_preload, 
        --     COUNT(DISTINCT tripeaks_kds.synthetic_id)::  INTEGER as installs,
        --     SUM(day1_bookings)::      FLOAT as day1_bookings,
        --     SUM(day2_bookings)::      FLOAT as day2_bookings,
        --     SUM(day3_bookings)::      FLOAT as day3_bookings,
        --     SUM(day7_bookings)::      FLOAT as day7_bookings,
        --     SUM(day14_bookings)::     FLOAT as day14_bookings,
        --     SUM(day30_bookings)::     FLOAT as day30_bookings,
        --     SUM(day60_bookings)::     FLOAT as day60_bookings,
        --     SUM(day90_bookings)::     FLOAT as day90_bookings,
        --     SUM(day180_bookings)::    FLOAT as day180_bookings,
        --     SUM(day360_bookings)::    FLOAT as day360_bookings,
        --     SUM(day540_bookings)::    FLOAT as day540_bookings,
        --     SUM(todate_bookings)::    FLOAT as todate_bookings,
        --     SUM(day1_payer)::        INTEGER as day1_payers,
        --     SUM(day2_payer)::        INTEGER as day2_payers,
        --     SUM(day3_payer)::        INTEGER as day3_payers,
        --     SUM(day7_payer)::        INTEGER as day7_payers,
        --     SUM(day14_payer)::       INTEGER as day14_payers,
        --     SUM(day30_payer)::       INTEGER as day30_payers,
        --     SUM(day60_payer)::       INTEGER as day60_payers,
        --     SUM(day90_payer)::       INTEGER as day90_payers,
        --     SUM(day180_payer)::      INTEGER as day180_payers,
        --     SUM(day360_payer)::      INTEGER as day360_payers,
        --     SUM(day540_payer)::      INTEGER as day540_payers,
        --     SUM(payer)::      INTEGER as todate_payers,
        --     SUM(day1_retained)::      INTEGER as day1_retained,
        --     SUM(day2_retained)::      INTEGER as day2_retained,
        --     SUM(day3_retained)::      INTEGER as day3_retained,
        --     SUM(day7_retained)::      INTEGER as day7_retained,
        --     SUM(day14_retained)::     INTEGER as day14_retained,
        --     SUM(day30_retained)::     INTEGER as day30_retained,
        --     SUM(day60_retained)::     INTEGER as day60_retained,
        --     SUM(day90_retained)::     INTEGER as day90_retained,
        --     SUM(day180_retained)::    INTEGER as day180_retained,
        --     SUM(day360_retained)::    INTEGER as day360_retained,
        --     SUM(day540_retained)::    INTEGER as day540_retained,
        --     SUM(0)::FLOAT          AS day1_cef,
        --     SUM(0)::FLOAT          AS day2_cef,
        --     SUM(0)::FLOAT          AS day3_cef,
        --     SUM(0)::FLOAT          AS day7_cef,
        --     SUM(0)::FLOAT          AS day14_cef,
        --     SUM(0)::FLOAT          AS day30_cef,
        --     SUM(0)::FLOAT          AS day60_cef,
        --     SUM(0)::FLOAT          AS day90_cef,
        --     SUM(0)::FLOAT          AS day180_cef,
        --     SUM(0)::FLOAT          AS day360_cef,
        --     SUM(0)::FLOAT          AS day540_cef,
        --     SUM(0)::FLOAT          AS todate_cef,
	    --     SUM(ads.d1_ad_rev)::FLOAT          AS day1_ad_rev,
	    --     SUM(ads.d2_ad_rev)::FLOAT          AS day2_ad_rev,
	    --     SUM(ads.d3_ad_rev)::FLOAT          AS day3_ad_rev,
	    --     SUM(ads.d7_ad_rev)::FLOAT          AS day7_ad_rev,
	    --     SUM(ads.d14_ad_rev)::FLOAT          AS day14_ad_rev,
	    --     SUM(ads.d30_ad_rev)::FLOAT          AS day30_ad_rev,
	    --     SUM(ads.d60_ad_rev)::FLOAT          AS day60_ad_rev,
	    --     SUM(ads.d90_ad_rev)::FLOAT          AS day90_ad_rev,
	    --     SUM(ads.d180_ad_rev)::FLOAT          AS day180_ad_rev,
	    --     SUM(ads.d360_ad_rev)::FLOAT          AS day360_ad_rev,
	    --     SUM(ads.d540_ad_rev)::FLOAT          AS day540_ad_rev,
	    --     SUM(ads.ltd_ad_rev)::FLOAT          AS todate_ad_rev,
        --     SUM(0)::INTEGER        AS registered_cnt
        -- FROM
        --     gsnmobile.kochava_device_summary tripeaks_kds 
        --     left join 
        --     	(select k.synthetic_id, 
		-- 			sum(case when(a.event_date between k.install_day and k.install_day+1) then(a.mills/1000) else(0.0000) end) as d1_ad_rev, 
		-- 			sum(case when(a.event_date between k.install_day and k.install_day+2) then(a.mills/1000) else(0.0000) end) as d2_ad_rev, 
		-- 			sum(case when(a.event_date between k.install_day and k.install_day+3) then(a.mills/1000) else(0.0000) end) as d3_ad_rev, 
		-- 			sum(case when(a.event_date between k.install_day and k.install_day+7) then(a.mills/1000) else(0.0000) end) as d7_ad_rev, 
		-- 			sum(case when(a.event_date between k.install_day and k.install_day+14) then(a.mills/1000) else(0.0000) end) as d14_ad_rev, 
		-- 			sum(case when(a.event_date between k.install_day and k.install_day+30) then(a.mills/1000) else(0.0000) end) as d30_ad_rev, 
		-- 			sum(case when(a.event_date between k.install_day and k.install_day+60) then(a.mills/1000) else(0.0000) end) as d60_ad_rev, 
		-- 			sum(case when(a.event_date between k.install_day and k.install_day+90) then(a.mills/1000) else(0.0000) end) as d90_ad_rev, 
		-- 			sum(case when(a.event_date between k.install_day and k.install_day+180) then(a.mills/1000) else(0.0000) end) as d180_ad_rev, 
		-- 			sum(case when(a.event_date between k.install_day and k.install_day+360) then(a.mills/1000) else(0.0000) end) as d360_ad_rev, 
		-- 			sum(case when(a.event_date between k.install_day and k.install_day+540) then(a.mills/1000) else(0.0000) end) as d540_ad_rev, 
		-- 			sum(case when(a.event_date >= k.install_day) then(a.mills/1000) else(0.0000) end) as ltd_ad_rev 
		-- 		from ads.device_revenue a 
		-- 		join gsnmobile.kochava_device_summary k 
		-- 		using (synthetic_id) 
		-- 		where a.app ilike '%tripeak%' 
		-- 			and k.app_name ilike '%tripeak%' 
		-- 		group by 1 order by 1 desc) ads 
        --     using(synthetic_id) 
        -- WHERE
        --     app_name IN ('SOLITAIRE TRIPEAKS')
        -- 		AND (platform, app_name, model, install_day, network_name) IN
		-- 			(
		-- 			SELECT DISTINCT
		-- 			  platform,
		-- 			  app_name,
		-- 			  model,
		-- 			  install_day,
		-- 			  network_name
		-- 			FROM
		-- 			  gsnmobile.ad_roas_kochava
		-- 			WHERE
		-- 			  app_name IN
		-- 			               (
		-- 			               'SOLITAIRE TRIPEAKS'
		-- 			               )

        --        					  -- ds is previous day
        --        					  -- 's date             

		-- 			)
		-- GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
        -- UNION ALL 
        SELECT
            platform,
            app_name,
            model,
            install_day,
            campaign_name,
            campaign_id,
            site_id,
            network_name,
            creative,
            country_code,
            request_keyword,
            'new_user'        AS existing_user,
            false AS unattributed_preload,
            installs::           INTEGER,
            day1_bookings::      FLOAT,
            day2_bookings::      FLOAT,
            day3_bookings::      FLOAT,
            day7_bookings::      FLOAT,
            day14_bookings::     FLOAT,
            day30_bookings::     FLOAT,
            day60_bookings::     FLOAT,
            day90_bookings::     FLOAT,
            day180_bookings::    FLOAT,
            day360_bookings::    FLOAT,
            day540_bookings::    FLOAT,
            todate_bookings::    FLOAT,
            day1_payers::        INTEGER,
            day2_payers::        INTEGER,
            day3_payers::        INTEGER,
            day7_payers::        INTEGER,
            day14_payers::       INTEGER,
            day30_payers::       INTEGER,
            day60_payers::       INTEGER,
            day90_payers::       INTEGER,
            day180_payers::      INTEGER,
            day360_payers::      INTEGER,
            day540_payers::      INTEGER,
            todate_payers::      INTEGER,
            day1_retained::      INTEGER,
            day2_retained::      INTEGER,
            day3_retained::      INTEGER,
            day7_retained::      INTEGER,
            day14_retained::     INTEGER,
            day30_retained::     INTEGER,
            day60_retained::     INTEGER,
            day90_retained::     INTEGER,
            day180_retained::    INTEGER,
            day360_retained::    INTEGER,
            day540_retained::    INTEGER,
            0::FLOAT          AS day1_cef,
            0::FLOAT          AS day2_cef,
            0::FLOAT          AS day3_cef,
            0::FLOAT          AS day7_cef,
            0::FLOAT          AS day14_cef,
            0::FLOAT          AS day30_cef,
            0::FLOAT          AS day60_cef,
            0::FLOAT          AS day90_cef,
            0::FLOAT          AS day180_cef,
            0::FLOAT          AS day360_cef,
            0::FLOAT          AS day540_cef,
            0::FLOAT          AS todate_cef,
	        0::FLOAT          AS day1_ad_rev,
	        0::FLOAT          AS day2_ad_rev,
	        0::FLOAT          AS day3_ad_rev,
	        0::FLOAT          AS day7_ad_rev,
	        0::FLOAT          AS day14_ad_rev,
	        0::FLOAT          AS day30_ad_rev,
	        0::FLOAT          AS day60_ad_rev,
	        0::FLOAT          AS day90_ad_rev,
	        0::FLOAT          AS day180_ad_rev,
	        0::FLOAT          AS day360_ad_rev,
	        0::FLOAT          AS day540_ad_rev,
	        0::FLOAT          AS todate_ad_rev,
            0::INTEGER        AS registered_cnt
        FROM
            gsnmobile.ad_roas_kochava
        WHERE
            app_name IN ('GSN GRAND CASINO',
                         'GSN CASINO',
                         'WOF SLOTS')
        AND (
                platform, app_name, model, install_day, network_name) IN
                                                                          (
                                                                          SELECT DISTINCT
                                                                              platform,
                                                                              app_name,
                                                                              model,
                                                                              install_day,
                                                                              network_name
                                                                          FROM
                                                                              gsnmobile.ad_roas_kochava
                                                                          WHERE
                                                                              app_name IN
                                                                                           (
                                                                                           'GSN GRAND CASINO'
                                                                                           ,
                                                                                           'GSN CASINO'
                                                                                           ,
                                                                                           'WOF SLOTS'
                                                                                           )

                                                                          )
        UNION ALL 
        SELECT
            platform,
            app_name,
            model,
            install_day,
            campaign_name,
            campaign_id,
            site_id,
            network_name,
            creative,
            country_code,
            request_keyword,
            'new_user'        AS existing_user,
            false AS unattributed_preload,
            installs::           INTEGER,
            day1_bookings::      FLOAT,
            day2_bookings::      FLOAT,
            day3_bookings::      FLOAT,
            day7_bookings::      FLOAT,
            day14_bookings::     FLOAT,
            day30_bookings::     FLOAT,
            day60_bookings::     FLOAT,
            day90_bookings::     FLOAT,
            day180_bookings::    FLOAT,
            day360_bookings::    FLOAT,
            day540_bookings::    FLOAT,
            todate_bookings::    FLOAT,
            day1_payers::        INTEGER,
            day2_payers::        INTEGER,
            day3_payers::        INTEGER,
            day7_payers::        INTEGER,
            day14_payers::       INTEGER,
            day30_payers::       INTEGER,
            day60_payers::       INTEGER,
            day90_payers::       INTEGER,
            day180_payers::      INTEGER,
            day360_payers::      INTEGER,
            day540_payers::      INTEGER,
            todate_payers::      INTEGER,
            day1_retained::      INTEGER,
            day2_retained::      INTEGER,
            day3_retained::      INTEGER,
            day7_retained::      INTEGER,
            day14_retained::     INTEGER,
            day30_retained::     INTEGER,
            day60_retained::     INTEGER,
            day90_retained::     INTEGER,
            day180_retained::    INTEGER,
            day360_retained::    INTEGER,
            day540_retained::    INTEGER,
            0::FLOAT          AS day1_cef,
            0::FLOAT          AS day2_cef,
            0::FLOAT          AS day3_cef,
            0::FLOAT          AS day7_cef,
            0::FLOAT          AS day14_cef,
            0::FLOAT          AS day30_cef,
            0::FLOAT          AS day60_cef,
            0::FLOAT          AS day90_cef,
            0::FLOAT          AS day180_cef,
            0::FLOAT          AS day360_cef,
            0::FLOAT          AS day540_cef,
            0::FLOAT          AS todate_cef,
	        0::FLOAT          AS day1_ad_rev,
	        0::FLOAT          AS day2_ad_rev,
	        0::FLOAT          AS day3_ad_rev,
	        0::FLOAT          AS day7_ad_rev,
	        0::FLOAT          AS day14_ad_rev,
	        0::FLOAT          AS day30_ad_rev,
	        0::FLOAT          AS day60_ad_rev,
	        0::FLOAT          AS day90_ad_rev,
	        0::FLOAT          AS day180_ad_rev,
	        0::FLOAT          AS day360_ad_rev,
	        0::FLOAT          AS day540_ad_rev,
	        0::FLOAT          AS todate_ad_rev,
            0::INTEGER        AS registered_cnt
        FROM
            gsnmobile.ad_roas_kochava_smash
        WHERE
            app_name IN ('SMASH')
        
        UNION ALL
        SELECT
            platform,
            'WORLDWINNER' AS app_name,
            model,
            install_day,
            campaign_name,
            campaign_id,
            site_id,
            network_name,
            creative,
            country_code,
            request_keyword,
            CASE
                WHEN((network_name='unattributed')
                    AND (DATE(first_ww_app_login) > DATE(first_ww_reg)))
                THEN('prior_ww_reg')
                ELSE('new_unattributed_user')
            END                           AS existing_user,
            false AS unattributed_preload,
            COUNT(DISTINCT idfv)::INTEGER AS installs,
            SUM(day1_bookings)::FLOAT     AS day1_bookings,
            SUM(day2_bookings)::FLOAT     AS day2_bookings,
            SUM(day3_bookings)::FLOAT     AS day3_bookings,
            SUM(day7_bookings)::FLOAT     AS day7_bookings,
            SUM(day14_bookings)::FLOAT    AS day14_bookings,
            SUM(day30_bookings)::FLOAT    AS day30_bookings,
            SUM(day60_bookings)::FLOAT    AS day60_bookings,
            SUM(day90_bookings)::FLOAT    AS day90_bookings,
            SUM(day180_bookings)::FLOAT   AS day180_bookings,
            SUM(day360_bookings)::FLOAT   AS day360_bookings,
            SUM(day540_bookings)::FLOAT   AS day540_bookings,
            SUM(todate_bookings)::FLOAT   AS todate_bookings,
            SUM(day1_payer)::INTEGER      AS day1_payers,
            SUM(day2_payer)::INTEGER      AS day2_payers,
            SUM(day3_payer)::INTEGER      AS day3_payers,
            SUM(day7_payer)::INTEGER      AS day7_payers,
            SUM(day14_payer)::INTEGER     AS day14_payers,
            SUM(day30_payer)::INTEGER     AS day30_payers,
            SUM(day60_payer)::INTEGER     AS day60_payers,
            SUM(day90_payer)::INTEGER     AS day90_payers,
            SUM(day180_payer)::INTEGER    AS day180_payers,
            SUM(day360_payer)::INTEGER    AS day360_payers,
            SUM(day540_payer)::INTEGER    AS day540_payers,
            SUM(payer)::INTEGER           AS todate_payers,
            SUM(day1_retained)::INTEGER   AS day1_retained,
            SUM(day2_retained)::INTEGER   AS day2_retained,
            SUM(day3_retained)::INTEGER   AS day3_retained,
            SUM(day7_retained)::INTEGER   AS day7_retained,
            SUM(day14_retained)::INTEGER  AS day14_retained,
            SUM(day30_retained)::INTEGER  AS day30_retained,
            SUM(day60_retained)::INTEGER  AS day60_retained,
            SUM(day90_retained)::INTEGER  AS day90_retained,
            SUM(day180_retained)::INTEGER AS day180_retained,
            SUM(day360_retained)::INTEGER AS day360_retained,
            SUM(day540_retained)::INTEGER AS day540_retained,
            SUM(day1_cef)::FLOAT          AS day1_cef,
            SUM(day2_cef)::FLOAT          AS day2_cef,
            SUM(day3_cef)::FLOAT          AS day3_cef,
            SUM(day7_cef)::FLOAT          AS day7_cef,
            SUM(day14_cef)::FLOAT         AS day14_cef,
            SUM(day30_cef)::FLOAT         AS day30_cef,
            SUM(day60_cef)::FLOAT         AS day60_cef,
            SUM(day90_cef)::FLOAT         AS day90_cef,
            SUM(day180_cef)::FLOAT        AS day180_cef,
            SUM(day360_cef)::FLOAT        AS day360_cef,
            SUM(day540_cef)::FLOAT        AS day540_cef,
            SUM(todate_cef)::FLOAT        AS todate_cef,
	        SUM(0)::FLOAT          AS day1_ad_rev,
	        SUM(0)::FLOAT          AS day2_ad_rev,
	        SUM(0)::FLOAT          AS day3_ad_rev,
	        SUM(0)::FLOAT          AS day7_ad_rev,
	        SUM(0)::FLOAT          AS day14_ad_rev,
	        SUM(0)::FLOAT          AS day30_ad_rev,
	        SUM(0)::FLOAT          AS day60_ad_rev,
	        SUM(0)::FLOAT          AS day90_ad_rev,
	        SUM(0)::FLOAT          AS day180_ad_rev,
	        SUM(0)::FLOAT          AS day360_ad_rev,
	        SUM(0)::FLOAT          AS day540_ad_rev,
	        SUM(0)::FLOAT          AS todate_ad_rev,
            COUNT(DISTINCT
            CASE
                WHEN(first_ww_reg IS NOT NULL)
                THEN(idfv)
                ELSE(NULL)
            END)::INTEGER AS registered_cnt
        FROM
            gsnmobile.kochava_device_summary_phoenix
        WHERE
            app_name IN ('PHOENIX',
                         'WORLDWINNER',
                         'WORLD WINNER')

        GROUP BY
            1,2,3,4,5,6,7,8,9,10,11,12 ,13
        UNION ALL
        SELECT
            platform,
            'WWSOLRUSH' AS app_name,
            model,
            install_day,
            campaign_name,
            campaign_id,
            site_id,
            network_name,
            creative,
            country_code,
            request_keyword,
            CASE
                WHEN((network_name='unattributed')
                    AND (DATE(first_ww_app_login) > DATE(first_ww_reg)))
                THEN('prior_ww_reg')
                ELSE('new_unattributed_user')
            END                           AS existing_user,
            false AS unattributed_preload,
            COUNT(DISTINCT idfv)::INTEGER AS installs,
            SUM(day1_bookings)::FLOAT     AS day1_bookings,
            SUM(day2_bookings)::FLOAT     AS day2_bookings,
            SUM(day3_bookings)::FLOAT     AS day3_bookings,
            SUM(day7_bookings)::FLOAT     AS day7_bookings,
            SUM(day14_bookings)::FLOAT    AS day14_bookings,
            SUM(day30_bookings)::FLOAT    AS day30_bookings,
            SUM(day60_bookings)::FLOAT    AS day60_bookings,
            SUM(day90_bookings)::FLOAT    AS day90_bookings,
            SUM(day180_bookings)::FLOAT   AS day180_bookings,
            SUM(day360_bookings)::FLOAT   AS day360_bookings,
            SUM(day540_bookings)::FLOAT   AS day540_bookings,
            SUM(todate_bookings)::FLOAT   AS todate_bookings,
            SUM(day1_payer)::INTEGER      AS day1_payers,
            SUM(day2_payer)::INTEGER      AS day2_payers,
            SUM(day3_payer)::INTEGER      AS day3_payers,
            SUM(day7_payer)::INTEGER      AS day7_payers,
            SUM(day14_payer)::INTEGER     AS day14_payers,
            SUM(day30_payer)::INTEGER     AS day30_payers,
            SUM(day60_payer)::INTEGER     AS day60_payers,
            SUM(day90_payer)::INTEGER     AS day90_payers,
            SUM(day180_payer)::INTEGER    AS day180_payers,
            SUM(day360_payer)::INTEGER    AS day360_payers,
            SUM(day540_payer)::INTEGER    AS day540_payers,
            SUM(payer)::INTEGER           AS todate_payers,
            SUM(day1_retained)::INTEGER   AS day1_retained,
            SUM(day2_retained)::INTEGER   AS day2_retained,
            SUM(day3_retained)::INTEGER   AS day3_retained,
            SUM(day7_retained)::INTEGER   AS day7_retained,
            SUM(day14_retained)::INTEGER  AS day14_retained,
            SUM(day30_retained)::INTEGER  AS day30_retained,
            SUM(day60_retained)::INTEGER  AS day60_retained,
            SUM(day90_retained)::INTEGER  AS day90_retained,
            SUM(day180_retained)::INTEGER AS day180_retained,
            SUM(day360_retained)::INTEGER AS day360_retained,
            SUM(day540_retained)::INTEGER AS day540_retained,
            SUM(day1_cef)::FLOAT          AS day1_cef,
            SUM(day2_cef)::FLOAT          AS day2_cef,
            SUM(day3_cef)::FLOAT          AS day3_cef,
            SUM(day7_cef)::FLOAT          AS day7_cef,
            SUM(day14_cef)::FLOAT         AS day14_cef,
            SUM(day30_cef)::FLOAT         AS day30_cef,
            SUM(day60_cef)::FLOAT         AS day60_cef,
            SUM(day90_cef)::FLOAT         AS day90_cef,
            SUM(day180_cef)::FLOAT        AS day180_cef,
            SUM(day360_cef)::FLOAT        AS day360_cef,
            SUM(day540_cef)::FLOAT        AS day540_cef,
            SUM(todate_cef)::FLOAT        AS todate_cef,
	        SUM(0)::FLOAT          AS day1_ad_rev,
	        SUM(0)::FLOAT          AS day2_ad_rev,
	        SUM(0)::FLOAT          AS day3_ad_rev,
	        SUM(0)::FLOAT          AS day7_ad_rev,
	        SUM(0)::FLOAT          AS day14_ad_rev,
	        SUM(0)::FLOAT          AS day30_ad_rev,
	        SUM(0)::FLOAT          AS day60_ad_rev,
	        SUM(0)::FLOAT          AS day90_ad_rev,
	        SUM(0)::FLOAT          AS day180_ad_rev,
	        SUM(0)::FLOAT          AS day360_ad_rev,
	        SUM(0)::FLOAT          AS day540_ad_rev,
	        SUM(0)::FLOAT          AS todate_ad_rev,
            COUNT(DISTINCT
            CASE
                WHEN(first_ww_reg IS NOT NULL)
                THEN(idfv)
                ELSE(NULL)
            END)::INTEGER AS registered_cnt
        FROM
            gsnmobile.kochava_device_summary_solrush
        WHERE
            app_name IN ('WW SOLRUSH')

        GROUP BY
            1,2,3,4,5,6,7,8,9,10,11,12,13
        UNION ALL
        SELECT
            platform,
            app_name,
            model,
            install_day,
            campaign_name,
            campaign_id,
            site_id,
            network_name,
            creative,
            country_code,
            request_keyword,
            'new_user'                               AS existing_user,
            false AS unattributed_preload,
            installs::                                    INTEGER,
            day1_bookings::                               FLOAT,
            ((day1_bookings+day3_bookings)/2)::FLOAT   AS day2_bookings,
            day3_bookings::                               FLOAT,
            day7_bookings::                               FLOAT,
            day14_bookings::                              FLOAT,
            day30_bookings::                              FLOAT,
            day60_bookings::                              FLOAT,
            day90_bookings::                              FLOAT,
            day180_bookings::                             FLOAT,
            day360_bookings::                             FLOAT,
            day360_bookings::FLOAT                     AS day540_bookings,
            todate_bookings::                             FLOAT,
            day1_payers::                                 INTEGER,
            ((day1_payers+day3_payers)/2)::INTEGER     AS day2_payers,
            day3_payers::                                 INTEGER,
            day7_payers::                                 INTEGER,
            day14_payers::                                INTEGER,
            day30_payers::                                INTEGER,
            day60_payers::                                INTEGER,
            day90_payers::                                INTEGER,
            day180_payers::                               INTEGER,
            day360_payers::                               INTEGER,
            day360_payers::INTEGER                     AS day540_payers,
            todate_payers::                               INTEGER,
            day1_retained::                               INTEGER,
            ((day1_retained+day3_retained)/2)::INTEGER AS day2_retained,
            day3_retained::                               INTEGER,
            day7_retained::                               INTEGER,
            day14_retained::                              INTEGER,
            day30_retained::                              INTEGER,
            day60_retained::                              INTEGER,
            day90_retained::                              INTEGER,
            day180_retained::                             INTEGER,
            day360_retained::                             INTEGER,
            day360_retained::INTEGER                   AS day540_retained,
            0::FLOAT                                   AS day1_cef,
            0::FLOAT                                   AS day2_cef,
            0::FLOAT                                   AS day3_cef,
            0::FLOAT                                   AS day7_cef,
            0::FLOAT                                   AS day14_cef,
            0::FLOAT                                   AS day30_cef,
            0::FLOAT                                   AS day60_cef,
            0::FLOAT                                   AS day90_cef,
            0::FLOAT                                   AS day180_cef,
            0::FLOAT                                   AS day360_cef,
            0::FLOAT                                   AS day540_cef,
            0::FLOAT                                   AS todate_cef,
	        0::FLOAT          AS day1_ad_rev,
	        0::FLOAT          AS day2_ad_rev,
	        0::FLOAT          AS day3_ad_rev,
	        0::FLOAT          AS day7_ad_rev,
	        0::FLOAT          AS day14_ad_rev,
	        0::FLOAT          AS day30_ad_rev,
	        0::FLOAT          AS day60_ad_rev,
	        0::FLOAT          AS day90_ad_rev,
	        0::FLOAT          AS day180_ad_rev,
	        0::FLOAT          AS day360_ad_rev,
	        0::FLOAT          AS day540_ad_rev,
	        0::FLOAT          AS todate_ad_rev,
            0::INTEGER                                 AS registered_cnt
        FROM
            bash.ad_roas_kochava
        WHERE
            app_name = 'BINGO BASH'
        AND (
                platform, app_name, model, install_day, network_name) IN
                                                                          (
                                                                          SELECT DISTINCT
                                                                              platform,
                                                                              app_name,
                                                                              model,
                                                                              install_day,
                                                                              network_name
                                                                          FROM
                                                                              bash.ad_roas_kochava
                                                                          WHERE
                                                                              app_name =
                                                                              'BINGO BASH'

                                                                          )
        UNION ALL
        SELECT
            platform,
            UPPER(app_name) AS app_name,
            model,
            install_day,
            campaign_name,
            campaign_id,
            site_id,
            UPPER(network_name) AS network_name,
            creative,
            country_code,
            NULL                     AS request_keyword,
            'new_user'               AS existing_user,
            false AS unattributed_preload,
            installs::                  INTEGER,
            day1_bookings::             FLOAT,
            day2_bookings::             FLOAT,
            day3_bookings::             FLOAT,
            day7_bookings::             FLOAT,
            day14_bookings::            FLOAT,
            day30_bookings::            FLOAT,
            day60_bookings::            FLOAT,
            day90_bookings::            FLOAT,
            day180_bookings::           FLOAT,
            day360_bookings::           FLOAT,
            day360_bookings::FLOAT   AS day540_bookings,
            todate_bookings::           FLOAT,
            day1_payers::               INTEGER,
            day2_payers::               INTEGER,
            day3_payers::               INTEGER,
            day7_payers::               INTEGER,
            day14_payers::              INTEGER,
            day30_payers::              INTEGER,
            day60_payers::              INTEGER,
            day90_payers::              INTEGER,
            day180_payers::             INTEGER,
            day360_payers::             INTEGER,
            day360_payers::INTEGER   AS day540_payers,
            todate_payers::             INTEGER,
            day1_retained::             INTEGER,
            day2_retained::             INTEGER,
            day3_retained::             INTEGER,
            day7_retained::             INTEGER,
            day14_retained::            INTEGER,
            day30_retained::            INTEGER,
            day60_retained::            INTEGER,
            day90_retained::            INTEGER,
            day180_retained::           INTEGER,
            day360_retained::           INTEGER,
            day360_retained::INTEGER AS day540_retained,
            0::FLOAT                 AS day1_cef,
            0::FLOAT                 AS day2_cef,
            0::FLOAT                 AS day3_cef,
            0::FLOAT                 AS day7_cef,
            0::FLOAT                 AS day14_cef,
            0::FLOAT                 AS day30_cef,
            0::FLOAT                 AS day60_cef,
            0::FLOAT                 AS day90_cef,
            0::FLOAT                 AS day180_cef,
            0::FLOAT                 AS day360_cef,
            0::FLOAT                 AS day540_cef,
            0::FLOAT                 AS todate_cef,
	        0::FLOAT          AS day1_ad_rev,
	        0::FLOAT          AS day2_ad_rev,
	        0::FLOAT          AS day3_ad_rev,
	        0::FLOAT          AS day7_ad_rev,
	        0::FLOAT          AS day14_ad_rev,
	        0::FLOAT          AS day30_ad_rev,
	        0::FLOAT          AS day60_ad_rev,
	        0::FLOAT          AS day90_ad_rev,
	        0::FLOAT          AS day180_ad_rev,
	        0::FLOAT          AS day360_ad_rev,
	        0::FLOAT          AS day540_ad_rev,
	        0::FLOAT          AS todate_ad_rev,
            0::INTEGER               AS registered_cnt
        FROM
            idle.ad_roas_kochava
        WHERE
            app_name ilike '%fresh%deck%poker%'
        AND LOWER(platform) IN ('android',
                                'ios',
                                'amazon',
                                'fb')
        AND install_day >= '2014-01-01'
        UNION ALL
        SELECT
            platform,
            UPPER(app_name) AS app_name,
            model,
            install_day,
            campaign_name,
            campaign_id,
            site_id,
            UPPER(network_name) AS network_name,
            creative,
            country_code,
            NULL              AS request_keyword,
            'new_user'        AS existing_user,
            false AS unattributed_preload,
            installs::           INTEGER,
            day1_bookings::      FLOAT,
            day2_bookings::      FLOAT,
            day3_bookings::      FLOAT,
            day7_bookings::      FLOAT,
            day14_bookings::     FLOAT,
            day30_bookings::     FLOAT,
            day60_bookings::     FLOAT,
            day90_bookings::     FLOAT,
            day180_bookings::    FLOAT,
            day360_bookings::    FLOAT,
            day540_bookings::    FLOAT,
            todate_bookings::    FLOAT,
            day1_payers::        INTEGER,
            day2_payers::        INTEGER,
            day3_payers::        INTEGER,
            day7_payers::        INTEGER,
            day14_payers::       INTEGER,
            day30_payers::       INTEGER,
            day60_payers::       INTEGER,
            day90_payers::       INTEGER,
            day180_payers::      INTEGER,
            day360_payers::      INTEGER,
            day540_payers::      INTEGER,
            todate_payers::      INTEGER,
            day1_retained::      INTEGER,
            day2_retained::      INTEGER,
            day3_retained::      INTEGER,
            day7_retained::      INTEGER,
            day14_retained::     INTEGER,
            day30_retained::     INTEGER,
            day60_retained::     INTEGER,
            day90_retained::     INTEGER,
            day180_retained::    INTEGER,
            day360_retained::    INTEGER,
            day540_retained::    INTEGER,
            0::FLOAT          AS day1_cef,
            0::FLOAT          AS day2_cef,
            0::FLOAT          AS day3_cef,
            0::FLOAT          AS day7_cef,
            0::FLOAT          AS day14_cef,
            0::FLOAT          AS day30_cef,
            0::FLOAT          AS day60_cef,
            0::FLOAT          AS day90_cef,
            0::FLOAT          AS day180_cef,
            0::FLOAT          AS day360_cef,
            0::FLOAT          AS day540_cef,
            0::FLOAT          AS todate_cef,
	        0::FLOAT          AS day1_ad_rev,
	        0::FLOAT          AS day2_ad_rev,
	        0::FLOAT          AS day3_ad_rev,
	        0::FLOAT          AS day7_ad_rev,
	        0::FLOAT          AS day14_ad_rev,
	        0::FLOAT          AS day30_ad_rev,
	        0::FLOAT          AS day60_ad_rev,
	        0::FLOAT          AS day90_ad_rev,
	        0::FLOAT          AS day180_ad_rev,
	        0::FLOAT          AS day360_ad_rev,
	        0::FLOAT          AS day540_ad_rev,
	        0::FLOAT          AS todate_ad_rev,
            0::INTEGER        AS registered_cnt
        FROM
            gsnmobile.ad_roas_kochava_sparcade_old
        WHERE
            install_day >= '2016-06-01'
        ) kochava
GROUP BY
    1,2,3,4,5,6,7,8,9,10
    ) final;
COMMIT;
MERGE
   /*+ direct,LABEL ('airflow-ua-ua_master_main_dag.tableau_subdag-t_update_tableau_ua_kochava')*/
INTO
    gsnmobile.tableau_ua_kochava main
USING
    airflow.temp_tableau_ua_kochava temp
ON
    (
        main.join_field_kochava_to_spend = temp.join_field_kochava_to_spend
    AND main.kochava_existing_user=temp.kochava_existing_user
    AND main.join_field_kochava_to_ml = temp.join_field_kochava_to_ml)
WHEN MATCHED
    THEN
UPDATE
SET
    kochava_existing_user = temp.kochava_existing_user,
    kochava_unattributed_preload = temp.kochava_unattributed_preload,
    kochava_installs = temp.kochava_installs,
    kochava_day1_bookings = temp.kochava_day1_bookings,
    kochava_day2_bookings = temp.kochava_day2_bookings,
    kochava_day3_bookings = temp.kochava_day3_bookings,
    kochava_day7_bookings = temp.kochava_day7_bookings,
    kochava_day14_bookings = temp.kochava_day14_bookings,
    kochava_day30_bookings = temp.kochava_day30_bookings,
    kochava_day60_bookings = temp.kochava_day60_bookings,
    kochava_day90_bookings = temp.kochava_day90_bookings,
    kochava_day180_bookings = temp.kochava_day180_bookings,
    kochava_day360_bookings = temp.kochava_day360_bookings,
    kochava_day540_bookings = temp.kochava_day540_bookings,
    kochava_ltd_bookings = temp.kochava_ltd_bookings,
    kochava_day1_payers = temp.kochava_day1_payers,
    kochava_day2_payers = temp.kochava_day2_payers,
    kochava_day3_payers = temp.kochava_day3_payers,
    kochava_day7_payers = temp.kochava_day7_payers,
    kochava_day14_payers = temp.kochava_day14_payers,
    kochava_day30_payers = temp.kochava_day30_payers,
    kochava_day60_payers = temp.kochava_day60_payers,
    kochava_day90_payers = temp.kochava_day90_payers,
    kochava_day180_payers = temp.kochava_day180_payers,
    kochava_day360_payers = temp.kochava_day360_payers,
    kochava_day540_payers = temp.kochava_day540_payers,
    kochava_ltd_payers = temp.kochava_ltd_payers,
    kochava_day1_retained = temp.kochava_day1_retained,
    kochava_day2_retained = temp.kochava_day2_retained,
    kochava_day3_retained = temp.kochava_day3_retained,
    kochava_day7_retained = temp.kochava_day7_retained,
    kochava_day14_retained = temp.kochava_day14_retained,
    kochava_day30_retained = temp.kochava_day30_retained,
    kochava_day60_retained = temp.kochava_day60_retained,
    kochava_day90_retained = temp.kochava_day90_retained,
    kochava_day180_retained = temp.kochava_day180_retained,
    kochava_day360_retained = temp.kochava_day360_retained,
    kochava_day540_retained = temp.kochava_day540_retained,
    kochava_day1_cef = temp.kochava_day1_cef,
    kochava_day2_cef = temp.kochava_day2_cef,
    kochava_day3_cef = temp.kochava_day3_cef,
    kochava_day7_cef = temp.kochava_day7_cef,
    kochava_day14_cef = temp.kochava_day14_cef,
    kochava_day30_cef = temp.kochava_day30_cef,
    kochava_day60_cef = temp.kochava_day60_cef,
    kochava_day90_cef = temp.kochava_day90_cef,
    kochava_day180_cef = temp.kochava_day180_cef,
    kochava_day360_cef = temp.kochava_day360_cef,
    kochava_day540_cef = temp.kochava_day540_cef,
    kochava_todate_cef = temp.kochava_todate_cef,
    kochava_day1_ad_rev = temp.kochava_day1_ad_rev,
    kochava_day2_ad_rev = temp.kochava_day2_ad_rev,
    kochava_day3_ad_rev = temp.kochava_day3_ad_rev,
    kochava_day7_ad_rev = temp.kochava_day7_ad_rev,
    kochava_day14_ad_rev = temp.kochava_day14_ad_rev,
    kochava_day30_ad_rev = temp.kochava_day30_ad_rev,
    kochava_day60_ad_rev = temp.kochava_day60_ad_rev,
    kochava_day90_ad_rev = temp.kochava_day90_ad_rev,
    kochava_day180_ad_rev = temp.kochava_day180_ad_rev,
    kochava_day360_ad_rev = temp.kochava_day360_ad_rev,
    kochava_day540_ad_rev = temp.kochava_day540_ad_rev,
    kochava_todate_ad_rev = temp.kochava_todate_ad_rev,
    kochava_registered_cnt = temp.kochava_registered_cnt
WHEN NOT MATCHED
    THEN
INSERT
    (
        join_field_kochava_to_multipliers ,
        join_field_kochava_to_spend ,
        join_field_kochava_financialmodel ,
        kochava_app_name_Clean ,
        kochava_network_name_Clean ,
        kochava_platform_clean ,
        kochava_install_date_epoch_Clean ,
        kochava_install_date ,
        kochava_existing_user ,
	kochava_unattributed_preload ,
        kochava_installs ,
        kochava_day1_bookings ,
        kochava_day2_bookings ,
        kochava_day3_bookings ,
        kochava_day7_bookings ,
        kochava_day14_bookings ,
        kochava_day30_bookings ,
        kochava_day60_bookings ,
        kochava_day90_bookings ,
        kochava_day180_bookings ,
        kochava_day360_bookings ,
        kochava_day540_bookings ,
        kochava_ltd_bookings ,
        kochava_day1_payers ,
        kochava_day2_payers ,
        kochava_day3_payers ,
        kochava_day7_payers ,
        kochava_day14_payers ,
        kochava_day30_payers ,
        kochava_day60_payers ,
        kochava_day90_payers ,
        kochava_day180_payers ,
        kochava_day360_payers ,
        kochava_day540_payers ,
        kochava_ltd_payers ,
        kochava_day1_retained ,
        kochava_day2_retained ,
        kochava_day3_retained ,
        kochava_day7_retained ,
        kochava_day14_retained ,
        kochava_day30_retained ,
        kochava_day60_retained ,
        kochava_day90_retained ,
        kochava_day180_retained ,
        kochava_day360_retained ,
        kochava_day540_retained ,
        kochava_day1_cef ,
        kochava_day2_cef ,
        kochava_day3_cef ,
        kochava_day7_cef ,
        kochava_day14_cef ,
        kochava_day30_cef ,
        kochava_day60_cef ,
        kochava_day90_cef ,
        kochava_day180_cef ,
        kochava_day360_cef ,
        kochava_day540_cef ,
        kochava_todate_cef ,
		kochava_day1_ad_rev,
		kochava_day2_ad_rev,
		kochava_day3_ad_rev,
		kochava_day7_ad_rev,
		kochava_day14_ad_rev,
		kochava_day30_ad_rev,
		kochava_day60_ad_rev,
		kochava_day90_ad_rev,
		kochava_day180_ad_rev,
		kochava_day360_ad_rev,
		kochava_day540_ad_rev,
		kochava_todate_ad_rev,
        kochava_registered_cnt,
        join_field_kochava_to_ml
    )
    VALUES
    (
        temp.join_field_kochava_to_multipliers,
        temp.join_field_kochava_to_spend,
        temp.join_field_kochava_financialmodel,
        temp.kochava_app_name,
        temp.kochava_network_name,
        temp.kochava_platform,
        temp.kochava_install_date_epoch,
        temp.kochava_install_date,
        temp.kochava_existing_user,
	temp.kochava_unattributed_preload,
        temp.kochava_installs,
        temp.kochava_day1_bookings,
        temp.kochava_day2_bookings,
        temp.kochava_day3_bookings,
        temp.kochava_day7_bookings,
        temp.kochava_day14_bookings,
        temp.kochava_day30_bookings,
        temp.kochava_day60_bookings,
        temp.kochava_day90_bookings,
        temp.kochava_day180_bookings,
        temp.kochava_day360_bookings,
        temp.kochava_day540_bookings,
        temp.kochava_ltd_bookings,
        temp.kochava_day1_payers,
        temp.kochava_day2_payers,
        temp.kochava_day3_payers,
        temp.kochava_day7_payers,
        temp.kochava_day14_payers,
        temp.kochava_day30_payers,
        temp.kochava_day60_payers,
        temp.kochava_day90_payers,
        temp.kochava_day180_payers,
        temp.kochava_day360_payers,
        temp.kochava_day540_payers,
        temp.kochava_ltd_payers,
        temp.kochava_day1_retained,
        temp.kochava_day2_retained,
        temp.kochava_day3_retained,
        temp.kochava_day7_retained,
        temp.kochava_day14_retained,
        temp.kochava_day30_retained,
        temp.kochava_day60_retained,
        temp.kochava_day90_retained,
        temp.kochava_day180_retained,
        temp.kochava_day360_retained,
        temp.kochava_day540_retained,
        temp.kochava_day1_cef,
        temp.kochava_day2_cef,
        temp.kochava_day3_cef,
        temp.kochava_day7_cef,
        temp.kochava_day14_cef,
        temp.kochava_day30_cef,
        temp.kochava_day60_cef,
        temp.kochava_day90_cef,
        temp.kochava_day180_cef,
        temp.kochava_day360_cef,
        temp.kochava_day540_cef,
        temp.kochava_todate_cef,
		temp.kochava_day1_ad_rev,
		temp.kochava_day2_ad_rev,
		temp.kochava_day3_ad_rev,
		temp.kochava_day7_ad_rev,
		temp.kochava_day14_ad_rev,
		temp.kochava_day30_ad_rev,
		temp.kochava_day60_ad_rev,
		temp.kochava_day90_ad_rev,
		temp.kochava_day180_ad_rev,
		temp.kochava_day360_ad_rev,
		temp.kochava_day540_ad_rev,
		temp.kochava_todate_ad_rev,
        temp.kochava_registered_cnt,
        temp.join_field_kochava_to_ml
    );
COMMIT;

-- SELECT PURGE_TABLE('gsnmobile.tableau_ua_kochava');
