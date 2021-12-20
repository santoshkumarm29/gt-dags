MERGE
   /*+ direct,LABEL ('airflow-ua-ua_master_main_dag.tableau_subdag-t_update_tableau_ua_payer_retention')*/
INTO
    gsnmobile.tableau_ua_payer_retention main
USING
    (
        SELECT
            REGEXP_REPLACE(UPPER(
                CASE
                    WHEN kds.app_name IN ('PHOENIX',
                                          'WORLDWINNER',
                                          'WORLD WINNER')
                    THEN 'WORLDWINNER'
                    ELSE app_name
                END),'[^A-Z0-9]','') ||'||'||
            CASE
                WHEN((kds.network_name ilike '%smartlink%') AND (campaign_id in ('kosolitairetripeaks179552cdd213d1b1ec8dadb11e078f','kosolitairetripeaks179552cdd213d1b1eff27d8be17f25','kosolitaire-tripeaks-ios53a8762dc3a45f7f36992c7065','kosolitaire-tripeaks-amazon542c346dd4e8d62409345f40e6','kosolitairetripeaks179552cdd213d1b1e2ed740d2e8bfa','kosolitairetripeaks179552cdd213d1b1e8c45df91957da','kosolitaire-tripeaks-ios53a8762dc3a4559d6e0f025582','kosolitaire-tripeaks-amazon542c346dd4e8d2134a9d909d97'))) THEN('PLAYANEXTYAHOO')
                WHEN((kds.network_name ilike 'unattributed')
                    AND ((kds.model ilike '%AMAZON%')
                        OR  (kds.model ilike '%KINDLE%')
                        OR  (kds.platform ilike '%amazon%')
                        OR  (kds.platform ilike '%kindle%')
                        OR  (kds.campaign_id ilike '%amazon%')
                        OR  (kds.campaign_id ilike '%kindle%')
                        OR  (kds.campaign_id ilike 'komybingobash%')))
                THEN('AMAZONMEDIAGROUP')
                WHEN(kds.network_name ilike '%amazon%fire%')
                THEN('AMAZONMEDIAGROUP')
                WHEN(kds.network_name ilike '%wake%screen%')
                THEN('AMAZONMEDIAGROUP')
                WHEN(kds.network_name ilike '%applovin%')
                THEN('APPLOVIN')
                WHEN(kds.network_name ilike '%supersonic%')
                THEN('IRONSOURCE')
                WHEN(kds.network_name ilike '%iron%source%')
                THEN('IRONSOURCE')
                WHEN((kds.network_name ilike '%pch%')
                    OR  (kds.network_name ilike '%liquid%'))
                THEN('LIQUIDWIRELESS')
                WHEN(kds.network_name ilike '%tresensa%')
                THEN('TRESENSA')
                WHEN(kds.network_name ilike '%digital%turbine%')
                THEN('DIGITALTURBINE')
                WHEN(kds.network_name ilike '%smartly%')
                THEN('SMARTLY')
                WHEN(kds.network_name ilike '%Bidalgo%')
                THEN('BIDALGO')
                WHEN((kds.network_name ilike '%facebook%')
                     AND (LOWER(kds.site_id) ilike '%creativetest%'))
               THEN('CREATIVETESTFB')
                WHEN(kds.network_name ilike '%moloco%')
                THEN('MOLOCO')
                WHEN(kds.network_name ilike '%google%')
                THEN('GOOGLE')
                WHEN(kds.network_name ilike '%admob%')
                THEN('GOOGLE')
                WHEN(kds.network_name ilike '%adwords%')
                THEN('GOOGLE')
                WHEN(kds.network_name ilike '%dauup%facebook%')
                THEN('DAUUPFACEBOOK')
                WHEN(kds.network_name ilike '%dauup%android%')
                THEN('DAUUPNETWORK')
                WHEN(kds.network_name ilike '%motive%')
                THEN('MOTIVEINTERACTIVE')
                WHEN(kds.network_name ilike '%aarki%')
                THEN('AARKI')
                WHEN(kds.network_name ilike '%unity%')
                THEN('UNITYADS')
                WHEN ((kds.network_name ilike '%chartboost%')
                    AND (kds.campaign_name ilike '%_dd_%'))
                THEN('CHARTBOOSTDIRECTDEAL')
                WHEN (kds.network_name ilike '%sprinklr%instagram%')
                THEN('SPRINKLRINSTAGRAM')
                WHEN (kds.network_name ilike '%instagram%sprinklr%')
                THEN('SPRINKLRINSTAGRAM')
                WHEN (kds.network_name ilike '%sprinklr%facebook%')
                THEN('SPRINKLRFACEBOOK')
                WHEN (kds.network_name ilike '%facebook%sprinklr%')
                THEN('SPRINKLRFACEBOOK')
                WHEN (kds.network_name ilike '%sprinklr%yahoo%')
                THEN('SPRINKLRYAHOO')
                WHEN (kds.network_name ilike '%yahoo%sprinklr%')
                THEN('SPRINKLRYAHOO')
                WHEN((kds.network_name ilike '%facebook%')
                    AND (LOWER(kds.site_id) ilike 'svl%'))
                THEN('STEALTHVENTURELABSFACEBOOK')
                WHEN ((kds.network_name ilike '%facebook%')
                    AND (LOWER(kds.site_id) LIKE '[conacq]%'))
                THEN('CONSUMERACQUISITIONFACEBOOK')
                WHEN (kds.network_name ilike '%sprinklr%')
                THEN('SPRINKLRFACEBOOK')
                WHEN ((kds.network_name ilike '%instagram%')
                    AND (kds.app_name ilike '%wof%'))
                THEN('FACEBOOK')
                WHEN ((kds.network_name ilike '%instagram%')
                    AND (DATE(kds.install_day)>='2015-12-10'))
                THEN('CONSUMERACQUISITIONINSTAGRAM')
                WHEN ((kds.network_name ilike '%instagram%')
                    AND (DATE(kds.install_day)<'2015-12-10'))
                THEN('INSTAGRAM')
                WHEN(kds.network_name ilike '%glispaandroid%')
                THEN('GLISPA')
                WHEN(kds.network_name ilike '%apple%search%')
                THEN('APPLESEARCHADS')
                ELSE TRIM(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(kds.network_name),
                    '- ANDROID|- IOS|- FACEBOOK|- AMAZON|- ANROID|– ANDROID|– IOS|– FACEBOOK|– AMAZON|– ANROID'
                    ,''),'[^A-Z0-9]',''))
            END ||'||'||
            CASE
                WHEN(((kds.network_name ilike '%google%') OR (kds.network_name ilike '%adwords%') OR (kds.network_name ilike '%admob%')) 
                    AND ((kds.model ilike '%IPAD%') OR (kds.model ilike '%IOS%')))
                THEN 'IPHONE'
                WHEN(kds.model ilike '%ANDROID%')
                THEN 'ANDROID'
                WHEN(kds.model ilike '%IPHONE%')
                THEN 'IPHONE'
                WHEN(kds.model ilike '%IPAD%')
                THEN 'IPAD'
                WHEN((kds.model ilike '%AMAZON%')
                    OR  (kds.model ilike '%KINDLE%'))
                THEN 'AMAZON'
                WHEN(kds.model ilike '%IPOD%')
                THEN 'IPHONE'
                WHEN(kds.platform ilike '%android%')
                THEN 'ANDROID'
                WHEN((kds.platform ilike '%amazon%')
                    OR  (kds.platform ilike '%kindle%'))
                THEN 'AMAZON'
                WHEN(kds.platform ilike '%iphone%')
                THEN 'IPHONE'
                WHEN(kds.platform ilike '%ipad%')
                THEN 'IPAD'
                WHEN(kds.platform ilike '%ipod%')
                THEN 'IPHONE'
                WHEN(kds.campaign_id ilike 'kobingobashandroid%')
                THEN('ANDROID')
                WHEN(kds.campaign_id ilike 'kobingobashhdios%')
                THEN('IPAD')
                WHEN(kds.campaign_id ilike 'kobingobashios%')
                THEN('IPHONE')
                WHEN(kds.campaign_id ilike 'kofreshdeckpokerandroid%')
                THEN('ANDROID')
                WHEN(kds.campaign_id ilike 'kofreshdeckpokerios%')
                THEN('IPHONE')
                WHEN(kds.campaign_id ilike 'kofreshdeckpokerkindle%')
                THEN('AMAZON')
                WHEN(kds.campaign_id ilike 'kogo-poker-android%')
                THEN('ANDROID')
                WHEN(kds.campaign_id ilike 'kogo-poker-kindle%')
                THEN('AMAZON')
                WHEN(kds.campaign_id ilike 'kogsn-grand-casino-ios%')
                THEN('IPHONE')
                WHEN(kds.campaign_id ilike 'kogsn-grand-casino%')
                THEN('ANDROID')
                WHEN(kds.campaign_id ilike 'kogsncasinoamazon%')
                THEN('AMAZON')
                WHEN(kds.campaign_id ilike 'kogsncasinoandroid%')
                THEN('ANDROID')
                WHEN(kds.campaign_id ilike 'kogsncasinoios%')
                THEN('IPHONE')
                WHEN(kds.campaign_id ilike 'komybingobash%')
                THEN('AMAZON')
                WHEN(kds.campaign_id ilike 'koslotsbash%')
                THEN('IPAD')
                WHEN(kds.campaign_id ilike 'koslotsbashandroid%')
                THEN('ANDROID')
                WHEN(kds.campaign_id ilike 'koslotsoffunandroid%')
                THEN('ANDROID')
                WHEN(kds.campaign_id ilike 'koslotsoffunios%')
                THEN('IPHONE')
                WHEN(kds.campaign_id ilike 'koslotsoffunkindle%')
                THEN('AMAZON')
                WHEN(kds.campaign_id ilike 'kosolitaire-tripeaks-amazon%')
                THEN('AMAZON')
                WHEN(kds.campaign_id ilike 'kosolitaire-tripeaks-ios%')
                THEN('IPHONE')
                WHEN(kds.campaign_id ilike 'kosolitairetripeaks%')
                THEN('ANDROID')
                WHEN(kds.campaign_id ilike 'kowheel-of-fortune-slots-amazon%')
                THEN('AMAZON')
                WHEN(kds.campaign_id ilike 'kowheel-of-fortune-slots-ios%')
                THEN('IPHONE')
                WHEN(kds.campaign_id ilike 'kowheel-of-fortune-slots-android%')
                THEN('ANDROID')
                WHEN(kds.model ilike '%IOS%')
                THEN 'IPHONE'
                WHEN(kds.platform ilike '%ios%')
                THEN 'IPHONE'
                WHEN(LOWER(kds.model) IN ('pc',
                                          'desktop',
                                          'web',
                                          'canvas',
                                          'fb_browser'))
                THEN('FBCANVAS')
                WHEN(LOWER(kds.platform) IN ('pc',
                                             'fb',
                                             'desktop',
                                             'web',
                                             'canvas'))
                THEN('FBCANVAS')
                ELSE 'UNKNOWN'
            END ||'||'|| CAST(CAST(EXTRACT(EPOCH FROM DATE_TRUNC('day',kds.install_day)) AS INTEGER
            ) AS VARCHAR)                     AS join_field_kds_to_kochava,
            COUNT(DISTINCT kds.synthetic_id)  AS kds_payers,
            SUM(kds.day1_retained)::INTEGER   AS kds_day1_payers_retained,
            SUM(kds.day2_retained)::INTEGER   AS kds_day2_payers_retained,
            SUM(kds.day3_retained)::INTEGER   AS kds_day3_payers_retained,
            SUM(kds.day7_retained)::INTEGER   AS kds_day7_payers_retained,
            SUM(kds.day14_retained)::INTEGER  AS kds_day14_payers_retained,
            SUM(kds.day30_retained)::INTEGER  AS kds_day30_payers_retained,
            SUM(kds.day60_retained)::INTEGER  AS kds_day60_payers_retained,
            SUM(kds.day90_retained)::INTEGER  AS kds_day90_payers_retained,
            SUM(kds.day180_retained)::INTEGER AS kds_day180_payers_retained,
            SUM(kds.day360_retained)::INTEGER AS kds_day360_payers_retained,
            SUM(kds.day540_retained)::INTEGER AS kds_day540_payers_retained
        FROM
            gsnmobile.kochava_device_summary kds
        WHERE
        payer=1
        AND (kds.synthetic_id,kds.app_name,
          kds.install_day,
          kds.platform    ) IN (SELECT DISTINCT synthetic_id,app_name,
          install_day,
          platform
          FROM gsnmobile.kochava_device_summary
          WHERE app_name IN ('SOLITAIRE TRIPEAKS',
                         'GSN GRAND CASINO',
                         'GSN CASINO',
                         'WOF SLOTS',
                         'PHOENIX',
                         'WORLDWINNER',
                         'WORLD WINNER',
                         'WW SOLRUSH')
        AND last_Active_Day >= date('{{ ds }}') -- ds is previous day's date
        AND payer=1
        )
        GROUP BY
            1) temp
ON
    (
        main.join_field_kds_to_kochava = temp.join_field_kds_to_kochava)
WHEN MATCHED
    THEN
UPDATE
SET
    kds_payers = temp.kds_payers,
    kds_day1_payers_retained = temp.kds_day1_payers_retained,
    kds_day2_payers_retained = temp.kds_day2_payers_retained,
    kds_day3_payers_retained = temp.kds_day3_payers_retained,
    kds_day7_payers_retained = temp.kds_day7_payers_retained,
    kds_day14_payers_retained = temp.kds_day14_payers_retained,
    kds_day30_payers_retained = temp.kds_day30_payers_retained,
    kds_day60_payers_retained = temp.kds_day60_payers_retained,
    kds_day90_payers_retained = temp.kds_day90_payers_retained,
    kds_day180_payers_retained = temp.kds_day180_payers_retained,
    kds_day360_payers_retained = temp.kds_day360_payers_retained,
    kds_day540_payers_retained = temp.kds_day540_payers_retained
WHEN NOT MATCHED
    THEN
INSERT
    (
        join_field_kds_to_kochava,
        kds_payers ,
        kds_day1_payers_retained ,
        kds_day2_payers_retained ,
        kds_day3_payers_retained ,
        kds_day7_payers_retained ,
        kds_day14_payers_retained ,
        kds_day30_payers_retained ,
        kds_day60_payers_retained ,
        kds_day90_payers_retained ,
        kds_day180_payers_retained ,
        kds_day360_payers_retained ,
        kds_day540_payers_retained
    )
    VALUES
    (
        temp.join_field_kds_to_kochava,
        temp.kds_payers,
        temp.kds_day1_payers_retained,
        temp.kds_day2_payers_retained,
        temp.kds_day3_payers_retained,
        temp.kds_day7_payers_retained,
        temp.kds_day14_payers_retained,
        temp.kds_day30_payers_retained,
        temp.kds_day60_payers_retained,
        temp.kds_day90_payers_retained,
        temp.kds_day180_payers_retained,
        temp.kds_day360_payers_retained,
        temp.kds_day540_payers_retained
    );

COMMIT;

-- SELECT PURGE_TABLE('gsnmobile.tableau_ua_payer_retention');
