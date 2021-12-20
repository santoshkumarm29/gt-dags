MERGE
    /*+ direct,LABEL ('airflow-ua-ua_master_main_dag.tableau_subdag-t_update_tableau_us_singular_data')*/
INTO
    gsnmobile.tableau_ua_singular_data MAIN
USING
    (
        SELECT
            REGEXP_REPLACE(UPPER(
                CASE
                    WHEN(singular.app ilike '%wheel%of%fortune%slots%')
                    THEN('WOF SLOTS')
                    WHEN(singular.app ilike '%wof%')
                    THEN('WOF SLOTS')
                    WHEN(singular.app ilike '%world%winner%')
                    THEN('WORLDWINNER')
                    WHEN(singular.app ilike '%phoenix%')
                    THEN('WORLDWINNER')
                    WHEN(singular.app ilike '%animal%heroes%blast%')
                    THEN('SMASH')
                    ELSE(singular.app)
                END),'[^A-Z0-9]','') ||'||'||
            CASE
                WHEN((singular.source ilike '%amazon%media%')
                    OR  (singular.source ilike '%amazon%wake%screen%'))
                THEN('AMAZONMEDIAGROUP')
                WHEN((singular.adn_account_id = '1922818028024038') 
                     AND (singular.adn_campaign_name ilike '%fbcoupon%')
                     AND (DATE(singular.start_date) <= '2018-12-30'))
                THEN('FACEBOOKCOUPON')
                 WHEN((lower(singular.source) in ('facebook','smartly.io'))
                     AND (singular.adn_campaign_name ilike '%creativetest%')
                     )
                THEN('CREATIVETESTFB')
                WHEN(singular.adn_account_id = '275053936659451')
                THEN('STEALTHVENTURELABSFACEBOOK')
                WHEN((singular.adn_account_id = '1922818028024038')
                    AND (DATE(singular.start_date) <= '2018-12-30'))
                THEN('STEALTHVENTURELABSFACEBOOK')
                WHEN((singular.adn_account_id = '1922818028024038')
                    AND (DATE(singular.start_date) > '2018-12-30'))
                THEN('SMARTLY')
                WHEN((singular.adn_account_id = '452423075594349')
                    OR (singular.adn_account_id = '325870488289336'))
                THEN('BIDALGO')
                WHEN(singular.adn_account_id = '1374598489457433')
                THEN('SPRINKLRFACEBOOK')
                WHEN(singular.adn_account_id = '1218987018194054')
                THEN('FACEBOOKRETARGETING')
                WHEN(singular.adn_account_id = '1330741603685261')
                THEN('NANIGANS')
                WHEN((singular.adn_account_id = '1337828676309887')
                    AND (DATE(singular.start_date)<='2017-12-12'))
                THEN('NANIGANS')
                WHEN((singular.adn_account_id = '1337828676309887')
                    AND (DATE(singular.start_date)>'2017-12-12'))
                THEN('SMARTLY')
                WHEN(singular.adn_account_id = '1547198355372917')
                THEN('SMARTLY')
                WHEN(singular.adn_account_id = '1582304278528991')
                THEN('SMARTLY')
                WHEN(singular.source ilike '%smartly%')
                THEN('SMARTLY')
                WHEN(singular.source ilike '%mobilda%')
                THEN('SNAPCHAT')
                WHEN(singular.source ilike '%snap%')
                THEN('SNAPCHAT')
                WHEN(singular.source ilike '%manage.com%')
                THEN('MANAGE')
                WHEN(singular.source ilike '%applovin%')
                THEN('APPLOVIN')
                WHEN(singular.source ilike '%bing%')
                THEN('BINGSEARCH')
                WHEN(singular.source ilike '%aura%ironsource%')
                THEN('IRONSOURCEPRELOAD')
                WHEN(singular.source ilike '%super%sonic%')
                THEN('IRONSOURCE')
                WHEN(singular.source ilike '%iron%source%')
                THEN('IRONSOURCE')
                WHEN(singular.source ilike '%liquid%')
                THEN('LIQUIDWIRELESS')
                WHEN(singular.source ilike '%tresensa%')
                THEN('TRESENSA')
                WHEN(singular.source ilike '%digital%turbine%')
                THEN('DIGITALTURBINE')
                WHEN(singular.source ilike '%moloco%')
                THEN('MOLOCO')
                WHEN(singular.source ilike '%adaction%')
                THEN('ADACTION')
                WHEN(singular.tracker_name ilike '%google%')
                THEN('GOOGLE')
                WHEN(singular.tracker_name ilike '%admob%')
                THEN('GOOGLE')
                WHEN(singular.tracker_name ilike '%adwords%')
                THEN('GOOGLE')
                WHEN(singular.source ilike '%google%')
                THEN('GOOGLE')
                WHEN(singular.source ilike '%admob%')
                THEN('GOOGLE')
                WHEN(singular.source ilike '%adwords%')
                THEN('GOOGLE')
                WHEN(singular.tracker_name ilike '%dauup%facebook%')
                THEN('DAUUPFACEBOOK')
                WHEN(singular.tracker_name ilike '%dauup%android%')
                THEN('DAUUPNETWORK')
                WHEN(singular.tracker_name ilike '%motive%')
                THEN('MOTIVEINTERACTIVE')
                WHEN(singular.tracker_name ilike '%aarki%')
                THEN('AARKI')
                WHEN(singular.tracker_name ilike '%unity%')
                THEN('UNITYADS')
                WHEN((singular.tracker_name ilike '%chartboost%')
                    AND (singular.adn_campaign_name ilike '%_dd_%'))
                THEN('CHARTBOOSTDIRECTDEAL')
                WHEN((singular.tracker_name ilike '%instagram%')
                    AND (singular.adn_campaign_name ilike '%sprinstagram%'))
                THEN('SPRINKLRINSTAGRAM')
                WHEN((singular.source ilike '%consumer%acquisition%instagram%')
                    AND (singular.adn_campaign_name ilike '%[conacq]%'))
                THEN('CONSUMERACQUISITIONINSTAGRAM')
                WHEN((singular.source ilike '%consumer%acquisition%')
                    AND (singular.adn_campaign_name ilike '%[conacq]%'))
                THEN('CONSUMERACQUISITIONFACEBOOK')
                WHEN(singular.tracker_name ilike '%sprinklr%instagram%')
                THEN('SPRINKLRINSTAGRAM')
                WHEN(singular.tracker_name ilike '%instagram%sprinklr%')
                THEN('SPRINKLRINSTAGRAM')
                WHEN(singular.tracker_name ilike '%sprinklr%facebook%')
                THEN('SPRINKLRFACEBOOK')
                WHEN(singular.tracker_name ilike '%facebook%sprinklr%')
                THEN('SPRINKLRFACEBOOK')
                WHEN(singular.tracker_name ilike '%sprinklr%yahoo%')
                THEN('SPRINKLRYAHOO')
                WHEN(singular.tracker_name ilike '%yahoo%sprinklr%')
                THEN('SPRINKLRYAHOO')
                WHEN((singular.tracker_name ilike '%sprinklr%')
                    AND (singular.adn_campaign_name ilike '%sprinstagram%'))
                THEN('SPRINKLRINSTAGRAM')
                WHEN((singular.tracker_name ilike '%sprinklr%')
                    AND (singular.adn_campaign_name NOT ilike '%sprinstagram%'))
                THEN('SPRINKLRFACEBOOK')
                WHEN(((singular.tracker_name IS NULL)
                        OR  (singular.tracker_name=''))
                    AND (singular.source ilike '%sprinklr%')
                    AND (singular.adn_campaign_name ilike '%instagram%'))
                THEN('SPRINKLRINSTAGRAM')
                WHEN(((singular.tracker_name IS NULL)
                        OR  (singular.tracker_name=''))
                    AND (singular.source ilike '%sprinklr%')
                    AND (singular.adn_campaign_name NOT ilike '%instagram%'))
                THEN('SPRINKLRFACEBOOK')
                ELSE TRIM(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(
                        CASE
                            WHEN((singular.tracker_name IS NULL)
                                OR  (singular.tracker_name = ''))
                            THEN(singular.source)
                            ELSE(singular.tracker_name)
                        END),
                    '- ANDROID|- IOS|- FACEBOOK|- AMAZON|- ANROID|– ANDROID|– IOS|– FACEBOOK|– AMAZON|– ANROID|-ANDROID|-IOS|-FACEBOOK|-AMAZON|-ANROID|–ANDROID|–IOS|–FACEBOOK|–AMAZON|–ANROID'
                    ,''),'[^A-Z0-9]',''))
            END ||'||'||
            CASE
                WHEN(((singular.source ilike '%google%') OR (singular.source ilike '%adwords%') OR (singular.source ilike '%admob%')) 
                     AND ((singular.platform ilike '%IPAD%') OR (singular.platform ilike '%IOS%'))) 
                THEN 'IPHONE'
                WHEN(singular.platform ilike '%ANDROID%')
                THEN 'ANDROID'
                WHEN(singular.platform ilike '%IPHONE%')
                THEN 'IPHONE'
                WHEN(singular.platform ilike '%IPAD%')
                THEN 'IPAD'
                WHEN((singular.platform ilike '%AMAZON%')
                    OR  (singular.platform ilike'%KINDLE%'))
                THEN 'AMAZON'
                WHEN(singular.platform ilike '%IPOD%')
                THEN 'IPHONE'
                WHEN(singular.tracker_name ilike 'kobingobashandroid%')
                THEN('ANDROID')
                WHEN(singular.tracker_name ilike 'kobingobashhdios%')
                THEN('IPAD')
                WHEN(singular.tracker_name ilike 'kobingobashios%')
                THEN('IPHONE')
                WHEN(singular.tracker_name ilike'kofreshdeckpokerandroid%')
                THEN('ANDROID')
                WHEN(singular.tracker_name ilike 'kofreshdeckpokerios%')
                THEN('IPHONE')
                WHEN(singular.tracker_name ilike'kofreshdeckpokerkindle%')
                THEN('AMAZON')
                WHEN(singular.tracker_name ilike 'kogo-poker-android%')
                THEN('ANDROID')
                WHEN(singular.tracker_name ilike 'kogo-poker-kindle%')
                THEN('AMAZON')
                WHEN(singular.tracker_name ilike'kogsn-grand-casino-ios%')
                THEN('IPHONE')
                WHEN(singular.tracker_name ilike 'kogsn-grand-casino%')
                THEN('ANDROID')
                WHEN(singular.tracker_name ilike 'kogsncasinoamazon%')
                THEN('AMAZON')
                WHEN(singular.tracker_name ilike 'kogsncasinoandroid%')
                THEN('ANDROID')
                WHEN(singular.tracker_name ilike 'kogsncasinoios%')
                THEN('IPHONE')
                WHEN(singular.tracker_name ilike 'komybingobash%')
                THEN('AMAZON')
                WHEN(singular.tracker_name ilike 'koslotsbash%')
                THEN('IPAD')
                WHEN(singular.tracker_name ilike 'koslotsbashandroid%')
                THEN('ANDROID')
                WHEN(singular.tracker_name ilike 'koslotsoffunandroid%')
                THEN('ANDROID')
                WHEN(singular.tracker_name ilike 'koslotsoffunios%')
                THEN('IPHONE')
                WHEN(singular.tracker_name ilike 'koslotsoffunkindle%')
                THEN('AMAZON')
                WHEN(singular.tracker_name ilike'kosolitaire-tripeaks-amazon%')
                THEN('AMAZON')
                WHEN(singular.tracker_name ilike'kosolitaire-tripeaks-ios%')
                THEN('IPHONE')
                WHEN(singular.tracker_name ilike 'kosolitairetripeaks%')
                THEN('ANDROID')
                WHEN(singular.platform ilike '%IOS%')
                THEN ('IPHONE')
                WHEN(LOWER(singular.platform) IN ('pc',
                                                  'desktop',
                                                  'web',
                                                  'canvas'))
                THEN ('FBCANVAS')
                WHEN(LOWER(singular.os) IN ('pc',
                                            'desktop',
                                            'web',
                                            'canvas'))
                THEN ('FBCANVAS')
                ELSE('UNKNOWN')
            END ||'||'|| CAST(CAST(EXTRACT(EPOCH FROM DATE_TRUNC('day',singular.start_date)) AS
            INTEGER) AS VARCHAR) AS join_field_singular_to_kochava,
            REGEXP_REPLACE(UPPER(
                CASE
                    WHEN(singular.app ilike '%wheel%of%fortune%slots%')
                    THEN('WOF SLOTS')
                    WHEN(singular.app ilike '%wof%')
                    THEN('WOF SLOTS')
                    WHEN(singular.app ilike '%world%winner%')
                    THEN('WORLDWINNER')
                    WHEN(singular.app ilike '%phoenix%')
                    THEN('WORLDWINNER')
                    WHEN(singular.app ilike '%animal%heroes%blast%')
                    THEN('SMASH')
                    ELSE(singular.app)
                END),'[^A-Z0-9]','') AS singular_app_clean,
            CASE
                WHEN((singular.source ilike '%amazon%media%')
                    OR  (singular.source ilike '%amazon%wake%screen%'))
                THEN('AMAZONMEDIAGROUP')
                WHEN((singular.adn_account_id = '1922818028024038') 
                     AND (singular.adn_campaign_name ilike '%fbcoupon%')
                     AND (DATE(singular.start_date) <= '2018-12-30'))         
                THEN('FACEBOOKCOUPON')
                WHEN((lower(singular.source) in ('facebook','smartly.io'))
                     AND (singular.adn_campaign_name ilike '%creativetest%')
                     )
                THEN('CREATIVETESTFB')
                WHEN(singular.adn_account_id = '275053936659451')
                THEN('STEALTHVENTURELABSFACEBOOK')
                WHEN((singular.adn_account_id = '1922818028024038')
                    AND (DATE(singular.start_date) <= '2018-12-30'))
                THEN('STEALTHVENTURELABSFACEBOOK')
                WHEN((singular.adn_account_id = '1922818028024038')
                    AND (DATE(singular.start_date) > '2018-12-30'))
                THEN('SMARTLY')
                WHEN((singular.adn_account_id = '452423075594349')
                    OR (singular.adn_account_id = '325870488289336'))
                THEN('BIDALGO')
                WHEN(singular.adn_account_id = '1374598489457433')
                THEN('SPRINKLRFACEBOOK')
                WHEN(singular.adn_account_id = '1218987018194054')
                THEN('FACEBOOKRETARGETING')
                WHEN(singular.adn_account_id = '1330741603685261')
                THEN('NANIGANS')
                WHEN((singular.adn_account_id = '1337828676309887')
                    AND (DATE(singular.start_date)<='2017-12-12'))
                THEN('NANIGANS')
                WHEN((singular.adn_account_id = '1337828676309887')
                    AND (DATE(singular.start_date)>'2017-12-12'))
                THEN('SMARTLY')
                WHEN(singular.adn_account_id = '1547198355372917')
                THEN('SMARTLY')
                WHEN(singular.adn_account_id = '1582304278528991')
                THEN('SMARTLY')
                WHEN(singular.source ilike '%mobilda%')
                THEN('SNAPCHAT')
                WHEN(singular.source ilike '%snap%')
                THEN('SNAPCHAT')
                WHEN(singular.source ilike '%smartly%')
                THEN('SMARTLY')
                WHEN(singular.source ilike '%manage.com%')
                THEN('MANAGE')
                WHEN(singular.source ilike '%applovin%')
                THEN('APPLOVIN')
                WHEN(singular.source ilike '%bing%')
                THEN('BINGSEARCH')
                WHEN(singular.source ilike '%aura%ironsource%')
                THEN('IRONSOURCEPRELOAD')
                WHEN(singular.source ilike '%super%sonic%')
                THEN('IRONSOURCE')
                WHEN(singular.source ilike '%iron%source%')
                THEN('IRONSOURCE')
                WHEN(singular.source ilike '%liquid%')
                THEN('LIQUIDWIRELESS')
                WHEN(singular.source ilike '%tresensa%')
                THEN('TRESENSA')
                WHEN(singular.source ilike '%digital%turbine%')
                THEN('DIGITALTURBINE')
                WHEN(singular.source ilike '%moloco%')
                THEN('MOLOCO')
                WHEN(singular.source ilike '%adaction%')
                THEN('ADACTION')
                WHEN(singular.tracker_name ilike '%google%')
                THEN('GOOGLE')
                WHEN(singular.tracker_name ilike '%admob%')
                THEN('GOOGLE')
                WHEN(singular.tracker_name ilike '%adwords%')
                THEN('GOOGLE')
                WHEN(singular.source ilike '%google%')
                THEN('GOOGLE')
                WHEN(singular.source ilike '%admob%')
                THEN('GOOGLE')
                WHEN(singular.source ilike '%adwords%')
                THEN('GOOGLE')
                WHEN(singular.tracker_name ilike '%dauup%facebook%')
                THEN('DAUUPFACEBOOK')
                WHEN(singular.tracker_name ilike '%dauup%android%')
                THEN('DAUUPNETWORK')
                WHEN(singular.tracker_name ilike '%motive%')
                THEN('MOTIVEINTERACTIVE')
                WHEN(singular.tracker_name ilike '%aarki%')
                THEN('AARKI')
                WHEN(singular.tracker_name ilike '%unity%')
                THEN('UNITYADS')
                WHEN((singular.tracker_name ilike '%chartboost%')
                    AND (singular.adn_campaign_name ilike '%_dd_%'))
                THEN('CHARTBOOSTDIRECTDEAL')
                WHEN((singular.tracker_name ilike '%instagram%')
                    AND (singular.adn_campaign_name ilike '%sprinstagram%'))
                THEN('SPRINKLRINSTAGRAM')
                WHEN((singular.source ilike '%consumer%acquisition%instagram%')
                    AND (singular.adn_campaign_name ilike '%[conacq]%'))
                THEN('CONSUMERACQUISITIONINSTAGRAM')
                WHEN((singular.source ilike '%consumer%acquisition%')
                    AND (singular.adn_campaign_name ilike '%[conacq]%'))
                THEN('CONSUMERACQUISITIONFACEBOOK')
                WHEN(singular.tracker_name ilike '%sprinklr%instagram%')
                THEN('SPRINKLRINSTAGRAM')
                WHEN(singular.tracker_name ilike '%instagram%sprinklr%')
                THEN('SPRINKLRINSTAGRAM')
                WHEN(singular.tracker_name ilike '%sprinklr%facebook%')
                THEN('SPRINKLRFACEBOOK')
                WHEN(singular.tracker_name ilike '%facebook%sprinklr%')
                THEN('SPRINKLRFACEBOOK')
                WHEN(singular.tracker_name ilike '%sprinklr%yahoo%')
                THEN('SPRINKLRYAHOO')
                WHEN(singular.tracker_name ilike '%yahoo%sprinklr%')
                THEN('SPRINKLRYAHOO')
                WHEN((singular.tracker_name ilike '%sprinklr%')
                    AND (singular.adn_campaign_name ilike '%sprinstagram%'))
                THEN('SPRINKLRINSTAGRAM')
                WHEN((singular.tracker_name ilike '%sprinklr%')
                    AND (singular.adn_campaign_name NOT ilike '%sprinstagram%'))
                THEN('SPRINKLRFACEBOOK')
                WHEN(((singular.tracker_name IS NULL)
                        OR  (singular.tracker_name=''))
                    AND (singular.source ilike '%sprinklr%')
                    AND (singular.adn_campaign_name ilike '%instagram%'))
                THEN('SPRINKLRINSTAGRAM')
                WHEN(((singular.tracker_name IS NULL)
                        OR  (singular.tracker_name=''))
                    AND (singular.source ilike '%sprinklr%')
                    AND (singular.adn_campaign_name NOT ilike '%instagram%'))
                THEN('SPRINKLRFACEBOOK')
                ELSE TRIM(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(
                        CASE
                            WHEN((singular.tracker_name IS NULL)
                                OR  (singular.tracker_name = ''))
                            THEN(singular.source)
                            ELSE(singular.tracker_name)
                        END),
                    '- ANDROID|- IOS|- FACEBOOK|- AMAZON|- ANROID|– ANDROID|– IOS|– FACEBOOK|– AMAZON|– ANROID|-ANDROID|-IOS|-FACEBOOK|-AMAZON|-ANROID|–ANDROID|–IOS|–FACEBOOK|–AMAZON|–ANROID'
                    ,''),'[^A-Z0-9]',''))
            END AS singular_tracker_name_clean,
            CASE
                WHEN(((singular.source ilike '%google%') OR (singular.source ilike '%adwords%') OR (singular.source ilike '%admob%')) 
                     AND ((singular.platform ilike '%IPAD%') OR (singular.platform ilike '%IOS%'))) 
                THEN 'IPHONE'
                WHEN(singular.platform ilike '%ANDROID%')
                THEN 'ANDROID'
                WHEN(singular.platform ilike '%IPHONE%')
                THEN 'IPHONE'
                WHEN(singular.platform ilike '%IPAD%')
                THEN 'IPAD'
                WHEN((singular.platform ilike '%AMAZON%')
                    OR  (singular.platform ilike'%KINDLE%'))
                THEN 'AMAZON'
                WHEN(singular.platform ilike '%IPOD%')
                THEN 'IPHONE'
                WHEN(singular.tracker_name ilike 'kobingobashandroid%')
                THEN('ANDROID')
                WHEN(singular.tracker_name ilike 'kobingobashhdios%')
                THEN('IPAD')
                WHEN(singular.tracker_name ilike 'kobingobashios%')
                THEN('IPHONE')
                WHEN(singular.tracker_name ilike'kofreshdeckpokerandroid%')
                THEN('ANDROID')
                WHEN(singular.tracker_name ilike 'kofreshdeckpokerios%')
                THEN('IPHONE')
                WHEN(singular.tracker_name ilike'kofreshdeckpokerkindle%')
                THEN('AMAZON')
                WHEN(singular.tracker_name ilike 'kogo-poker-android%')
                THEN('ANDROID')
                WHEN(singular.tracker_name ilike 'kogo-poker-kindle%')
                THEN('AMAZON')
                WHEN(singular.tracker_name ilike'kogsn-grand-casino-ios%')
                THEN('IPHONE')
                WHEN(singular.tracker_name ilike 'kogsn-grand-casino%')
                THEN('ANDROID')
                WHEN(singular.tracker_name ilike 'kogsncasinoamazon%')
                THEN('AMAZON')
                WHEN(singular.tracker_name ilike 'kogsncasinoandroid%')
                THEN('ANDROID')
                WHEN(singular.tracker_name ilike 'kogsncasinoios%')
                THEN('IPHONE')
                WHEN(singular.tracker_name ilike 'komybingobash%')
                THEN('AMAZON')
                WHEN(singular.tracker_name ilike 'koslotsbash%')
                THEN('IPAD')
                WHEN(singular.tracker_name ilike 'koslotsbashandroid%')
                THEN('ANDROID')
                WHEN(singular.tracker_name ilike 'koslotsoffunandroid%')
                THEN('ANDROID')
                WHEN(singular.tracker_name ilike 'koslotsoffunios%')
                THEN('IPHONE')
                WHEN(singular.tracker_name ilike 'koslotsoffunkindle%')
                THEN('AMAZON')
                WHEN(singular.tracker_name ilike'kosolitaire-tripeaks-amazon%')
                THEN('AMAZON')
                WHEN(singular.tracker_name ilike'kosolitaire-tripeaks-ios%')
                THEN('IPHONE')
                WHEN(singular.tracker_name ilike 'kosolitairetripeaks%')
                THEN('ANDROID')
                WHEN(singular.platform ilike '%IOS%')
                THEN ('IPHONE')
                WHEN(LOWER(singular.platform) IN ('pc',
                                                  'desktop',
                                                  'web',
                                                  'canvas'))
                THEN ('FBCANVAS')
                WHEN(LOWER(singular.os) IN ('pc',
                                            'desktop',
                                            'web',
                                            'canvas'))
                THEN ('FBCANVAS')
                ELSE('UNKNOWN')
            END AS singular_platform_clean,
            CAST(CAST(EXTRACT(EPOCH FROM DATE_TRUNC('day',singular.start_date)) AS INTEGER) AS
            VARCHAR)                                    AS singular_start_date_clean,
            DATE(DATE_TRUNC('day',singular.start_date))   AS singular_start_date,
            SUM(singular.adn_impressions)                 AS singular_adn_impressions,
            SUM(singular.custom_clicks)                   AS singular_custom_clicks,
            SUM(singular.custom_installs)                 AS singular_custom_installs,
            SUM(singular.adn_cost)                        AS singular_adn_cost,
            SUM(singular.adn_estimated_total_conversions) AS
                                               singular_adn_estimated_total_conversions,
            SUM(singular.adn_original_cost) AS singular_adn_original_cost,
            SUM(singular.adn_clicks)        AS singular_adn_clicks,
            SUM(singular.tracker_clicks)    AS singular_tracker_clicks,
            SUM(singular.adn_installs)      AS singular_adn_installs,
            SUM(singular.tracker_installs)  AS singular_tracker_installs
        FROM
            apis.singular_campaign_stats singular
        WHERE ( platform , app , source , start_date ) IN (  SELECT DISTINCT  platform , app , source , start_date
             FROM  apis.singular_campaign_stats
             WHERE app IN ('GSN Casino',
                             'Solitaire TriPeaks',
                             'Animal Heroes Blast',
                             'GSN Grand Casino',
                             'Bingo Bash',
                             'Fresh Deck Poker',
                             'Wheel of Fortune Slots',
                             'Sparcade',
                             'WorldWinner', 
                             'WW Sol Rush')
        AND updated_at >=  date('{{ ds }}') -1 --  last two day's data
        )
        GROUP BY
            1,2,3,4,5,6 ) temp
ON
    (
        main.join_field_singular_to_kochava = temp.join_field_singular_to_kochava)
WHEN MATCHED
    THEN
UPDATE
SET
    singular_app_clean = temp.singular_app_clean,
    singular_tracker_name_clean = temp.singular_tracker_name_clean,
    singular_platform_clean = temp.singular_platform_clean,
    singular_start_date_clean = temp.singular_start_date_clean,
    singular_start_date = temp.singular_start_date,
    singular_adn_impressions = temp.singular_adn_impressions,
    singular_custom_clicks = temp.singular_custom_clicks,
    singular_custom_installs = temp.singular_custom_installs,
    singular_adn_cost = temp.singular_adn_cost,
    singular_adn_estimated_total_conversions = temp.singular_adn_estimated_total_conversions,
    singular_adn_original_cost = temp.singular_adn_original_cost,
    singular_adn_clicks = temp.singular_adn_clicks,
    singular_tracker_clicks = temp.singular_tracker_clicks,
    singular_adn_installs = temp.singular_adn_installs,
    singular_tracker_installs = temp.singular_tracker_installs
WHEN NOT MATCHED
    THEN
INSERT
    (
        join_field_singular_to_kochava,
        singular_app_clean ,
        singular_tracker_name_clean ,
        singular_platform_clean ,
        singular_start_date_clean ,
        singular_start_date ,
        singular_adn_impressions ,
        singular_custom_clicks ,
        singular_custom_installs ,
        singular_adn_cost ,
        singular_adn_estimated_total_conversions ,
        singular_adn_original_cost ,
        singular_adn_clicks ,
        singular_tracker_clicks ,
        singular_adn_installs ,
        singular_tracker_installs
    )
    VALUES
    (
        temp.join_field_singular_to_kochava,
        temp.singular_app_clean,
        temp.singular_tracker_name_clean,
        temp.singular_platform_clean,
        temp.singular_start_date_clean,
        temp.singular_start_date,
        temp.singular_adn_impressions,
        temp.singular_custom_clicks,
        temp.singular_custom_installs,
        temp.singular_adn_cost,
        temp.singular_adn_estimated_total_conversions,
        temp.singular_adn_original_cost,
        temp.singular_adn_clicks,
        temp.singular_tracker_clicks,
        temp.singular_adn_installs,
        temp.singular_tracker_installs
    );
COMMIT;

-- SELECT PURGE_TABLE('gsnmobile.tableau_ua_singular_data');
