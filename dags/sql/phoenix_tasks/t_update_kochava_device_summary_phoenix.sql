TRUNCATE TABLE gsnmobile.kochava_device_summary_phoenix_ods;

CREATE local TEMPORARY TABLE ad_partner_installs_kochava_deduped ON COMMIT preserve rows direct AS
SELECT
    i.synthetic_id,
    CASE
        WHEN i.app_name ilike '%TRIPEAKS%'
        THEN 'TRIPEAKS SOLITAIRE'
        WHEN i.app_name ilike '%WOF SLOTS%'
        THEN 'WOF APP'
        ELSE UPPER(i.app_name)
    END AS app_name,
    i.inserted_on,
    i.network_name,
    i.install_date,
    i.campaign_name,
    i.campaign_id,
    i.site_id,
    i.creative,
    i.country_code,
    i.request_campaign_id,
    i.request_adgroup,
    i.request_keyword,
    i.request_matchtype,
    i.platform,
    i.kochava_os_version,
    i.request_bundle_id,
    i.idfv,
    i.android_id,
    i.ad_partner,
    i.row_number
FROM
    (
        SELECT
            i.synthetic_id,
            i.app_name,
            i.inserted_on,
            i.network_name,
            i.install_date,
            i.campaign_name,
            i.campaign_id,
            i.site_id,
            i.creative,
            i.country_code,
            i.request_campaign_id,
            i.request_adgroup,
            i.request_keyword,
            i.request_matchtype,
            i.platform,
            regexp_replace(i.device_version,'.*-','') AS kochava_os_version,
            i.request_bundle_id,
            i.idfv,
            i.android_id,
            i.ad_partner,
            row_number() OVER (PARTITION BY i.app_name,
            CASE
                WHEN (i.platform = 'ios'::VARCHAR(3))
                THEN COALESCE(i.synthetic_id, i.idfa, i.idfv)
                WHEN (i.platform = ANY (ARRAY['android'::VARCHAR(7), 'amazon'::VARCHAR(6)]))
                THEN COALESCE(i.synthetic_id, i.android_id, i.adid)
                ELSE NULL
            END ORDER BY i.click_date DESC, i.network_name) AS row_number
        FROM
            gsnmobile.ad_partner_installs i
        WHERE
        ((
                    i.ad_partner = 'kochava'::VARCHAR(7))
            AND (
                    i.platform = ANY (ARRAY['ios'::VARCHAR(3), 'android'::VARCHAR(7), 'amazon'::
                    VARCHAR(6)]))
            AND (
                    COALESCE(i.synthetic_id, i.idfa, i.idfv, i.android_id, i.adid, ''::VARCHAR) <>
                    ''::VARCHAR))) i
WHERE
    (i.row_number = 1)
    ORDER BY app_name, synthetic_id;

CREATE local TEMPORARY TABLE kds_phoenix_base ON COMMIT preserve rows direct AS
SELECT x.platform,
       NVL(i.install_timestamp, x.install_date) AS install_date,
       nvl(i.install_day, DATE (x.install_date)) AS install_day,
       i.install_timestamp as gsn_install_timestamp,
       i.install_day as gsn_install_day,
       x.install_date as kochava_install_timestamp,
       DATE (x.install_date) AS kochava_install_day,
       campaign_name,
       campaign_id,
       site_id,
       network_name,
       CASE
        WHEN((x.network_name ilike '%smartlink%')
            AND (x.campaign_id IN ('kosolitairetripeaks179552cdd213d1b1ec8dadb11e078f',
                                 'kosolitairetripeaks179552cdd213d1b1eff27d8be17f25',
                                 'kosolitaire-tripeaks-ios53a8762dc3a45f7f36992c7065',
                                 'kosolitaire-tripeaks-amazon542c346dd4e8d62409345f40e6',
                                 'kosolitairetripeaks179552cdd213d1b1e2ed740d2e8bfa',
                                 'kosolitairetripeaks179552cdd213d1b1e8c45df91957da',
                                 'kosolitaire-tripeaks-ios53a8762dc3a4559d6e0f025582',
                                 'kosolitaire-tripeaks-amazon542c346dd4e8d2134a9d909d97')))
        THEN('PLAYANEXTYAHOO')
        WHEN(x.network_name ilike '%amazon%fire%')
        THEN('AMAZONMEDIAGROUP')
        WHEN(x.network_name ilike '%wake%screen%')
        THEN('AMAZONMEDIAGROUP')
        WHEN((x.app_name ilike '%bingo%bash%')
            AND (x.network_name ilike '%amazon%')
            AND (x.campaign_name ilike '%wakescreen%'))
        THEN('AMAZONMEDIAGROUP')
        WHEN(x.network_name ilike '%mobilda%')
        THEN('SNAPCHAT')
        WHEN(x.network_name ilike '%snap%')
        THEN('SNAPCHAT')
        WHEN(x.network_name ilike '%applovin%')
        THEN('APPLOVIN')
        WHEN(x.network_name ilike '%bing%')
        THEN('BINGSEARCH')
        WHEN(x.network_name ilike '%supersonic%')
        THEN('IRONSOURCE')
        WHEN(x.network_name ilike '%iron%source%')
        THEN('IRONSOURCE')
        WHEN((x.network_name ilike '%pch%')
            OR  (x.network_name ilike '%liquid%'))
        THEN('LIQUIDWIRELESS')
        WHEN(x.network_name ilike '%tresensa%')
        THEN('TRESENSA')
        WHEN(x.network_name ilike '%digital%turbine%')
        THEN('DIGITALTURBINE')
        WHEN(x.network_name ilike '%moloco%')
        THEN('MOLOCO')
        WHEN(x.network_name ilike '%adaction%')
        THEN('ADACTION')
        WHEN(x.network_name ilike '%google%')
        THEN('GOOGLE')
        WHEN(x.network_name ilike '%admob%')
        THEN('GOOGLE')
        WHEN(x.network_name ilike '%adwords%')
        THEN('GOOGLE')
        WHEN(x.network_name ilike '%dauup%android%')
        THEN('DAUUPNETWORK')
        WHEN(x.network_name ilike '%motive%')
        THEN('MOTIVEINTERACTIVE')
        WHEN(x.network_name ilike '%aarki%')
        THEN('AARKI')
        WHEN(x.network_name ilike '%unity%')
        THEN('UNITYADS')
        WHEN ((x.network_name ilike '%chartboost%')
            AND (x.campaign_name ilike '%_dd_%'))
        THEN('CHARTBOOSTDIRECTDEAL')
        WHEN(((x.network_name ilike '%facebook%')
                OR  (x.network_name ilike '%instagram%'))
            AND (x.site_id ilike '%fbcoupon%')
            AND (nvl(i.install_day, DATE (x.install_date)) <= '2018-12-30'))
        THEN('FACEBOOKCOUPON')
        WHEN ((x.network_name ilike '%smartly%')
            OR  (x.network_name ilike 'Restricted'))
        THEN('SMARTLY')
        WHEN (x.network_name ilike '%sprinklr%instagram%')
        THEN('SPRINKLRINSTAGRAM')
        WHEN (x.network_name ilike '%instagram%sprinklr%')
        THEN('SPRINKLRINSTAGRAM')
        WHEN (x.network_name ilike '%sprinklr%facebook%')
        THEN('SPRINKLRFACEBOOK')
        WHEN (x.network_name ilike '%facebook%sprinklr%')
        THEN('SPRINKLRFACEBOOK')
        WHEN (x.network_name ilike '%sprinklr%yahoo%')
        THEN('SPRINKLRYAHOO')
        WHEN (x.network_name ilike '%yahoo%sprinklr%')
        THEN('SPRINKLRYAHOO')
        WHEN((x.network_name ilike '%facebook%')
            AND (x.site_id ilike 'svl%'))
        THEN('STEALTHVENTURELABSFACEBOOK')
        WHEN ((x.network_name ilike '%facebook%')
            AND (x.site_id ilike '[conacq]%'))
        THEN('CONSUMERACQUISITIONFACEBOOK')
        WHEN (x.network_name ilike '%sprinklr%')
        THEN('SPRINKLRFACEBOOK')
        WHEN ((x.network_name ilike '%instagram%')
            AND (x.app_name ilike '%wof%'))
        THEN('FACEBOOK')
        WHEN ((x.network_name ilike '%instagram%')
            AND (nvl(i.install_day, DATE (x.install_date))>='2015-12-10'))
        THEN('CONSUMERACQUISITIONINSTAGRAM')
        WHEN ((x.network_name ilike '%instagram%')
            AND (nvl(i.install_day, DATE (x.install_date))<'2015-12-10'))
        THEN('INSTAGRAM')
        WHEN(x.network_name ilike '%dauup%facebook%')
        THEN('DAUUPFACEBOOK')
        WHEN(x.network_name ilike '%glispaandroid%')
        THEN('GLISPA')
        WHEN(x.network_name ilike '%apple%search%')
        THEN('APPLESEARCHADS')
        ELSE TRIM(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(x.network_name),
            '- ANDROID|- IOS|- FACEBOOK|- AMAZON|- ANROID|– ANDROID|– IOS|– FACEBOOK|– AMAZON|– ANROID|-ANDROID|-IOS|-FACEBOOK|-AMAZON|-ANROID|–ANDROID|–IOS|–FACEBOOK|–AMAZON|–ANROID'
            ,''),'[^A-Z0-9]',''))
        END AS network_name_defined,
        creative,
        country_code,
        request_campaign_id,
        request_adgroup,
        request_keyword,
        request_matchtype,
        case when lower(x.platform) = 'ios' then x.idfv else md5(x.android_id) end as idfv
        FROM ad_partner_installs_kochava_deduped x
        left outer join phoenix.devices i
        ON i.idfv = (case when lower(x.platform) = 'ios' then x.idfv else md5(x.android_id) end)
        WHERE COALESCE(x.idfv,x.android_id,'') <> ''
        AND   network_name NOT ilike 'unattributed'
        AND   app_name = 'PHOENIX'
        UNION ALL
        --organics: all installs from our data that don't have a record in kochava data
        SELECT NULL AS platform,
               install_timestamp AS install_date,
               install_day AS install_day,
               install_timestamp AS gsn_install_timestamp,
               install_day AS gsn_install_day,
               null as kochava_install_timestamp,
               null as kochava_install_day,
               NULL AS campaign_name,
               NULL AS campaign_id,
               NULL AS site_id,
               'unattributed' AS network_name,
               'UNATTRIBUTED' AS network_name_defined,
               NULL AS creative,
               NULL AS country_code,
               NULL AS request_campaign_id,
               NULL AS request_adgroup,
               NULL AS request_keyword,
               NULL AS request_matchtype,
               i.idfv
        FROM phoenix.devices i
              LEFT OUTER JOIN gsnmobile.ad_partner_installs k
                           ON i.idfv = (case when lower(k.platform) = 'ios' then k.idfv else md5(k.android_id) end)
                          AND k.ad_partner = 'kochava'
                          AND k.platform IN ('ios', 'android', 'amazon')
                          AND k.app_name = 'PHOENIX'
                          AND COALESCE (k.idfv,k.android_id,'') <> ''
                          AND k.network_name NOT ilike 'unattributed'
            WHERE k.app_name IS NULL;

CREATE local TEMPORARY TABLE kds_phoenix_base_level_1 ON COMMIT preserve rows direct AS
              SELECT
               x.idfv,
               x.platform,
               x.install_date,
               x.install_day,
               x.gsn_install_timestamp,
               x.gsn_install_day,
               x.kochava_install_timestamp,
               x.kochava_install_day,
               x.campaign_name,
               x.campaign_id,
               x.site_id,
               x.network_name,
               x.network_name_defined,
               x.creative,
               x.country_code,
               x.request_campaign_id,
               x.request_adgroup,
               x.request_keyword,
               x.request_matchtype,
               MIN(b.event_time) AS first_payment_timestamp,
               MAX(b.event_time) AS last_payment_timestamp,
               SUM(nvl (amount_paid_usd,0)) AS todate_bookings,
               MAX(CASE WHEN b.event_time IS NOT NULL THEN 1 ELSE 0 END) AS payer,
               SUM(CASE WHEN datediff ('day',install_day,b.event_day) <= 1 THEN amount_paid_usd ELSE 0 END) AS day1_bookings,
               SUM(CASE WHEN datediff ('day',install_day,b.event_day) <= 2 THEN amount_paid_usd ELSE 0 END) AS day2_bookings,
               SUM(CASE WHEN datediff ('day',install_day,b.event_day) <= 3 THEN amount_paid_usd ELSE 0 END) AS day3_bookings,
               SUM(CASE WHEN datediff ('day',install_day,b.event_day) <= 7 THEN amount_paid_usd ELSE 0 END) AS day7_bookings,
               SUM(CASE WHEN datediff ('day',install_day,b.event_day) <= 14 THEN amount_paid_usd ELSE 0 END) AS day14_bookings,
               SUM(CASE WHEN datediff ('day',install_day,b.event_day) <= 30 THEN amount_paid_usd ELSE 0 END) AS day30_bookings,
               SUM(CASE WHEN datediff ('day',install_day,b.event_day) <= 60 THEN amount_paid_usd ELSE 0 END) AS day60_bookings,
               SUM(CASE WHEN datediff ('day',install_day,b.event_day) <= 90 THEN amount_paid_usd ELSE 0 END) AS day90_bookings,
               SUM(CASE WHEN datediff ('day',install_day,b.event_day) <= 120 THEN amount_paid_usd ELSE 0 END) AS day120_bookings,
               SUM(CASE WHEN datediff ('day',install_day,b.event_day) <= 150 THEN amount_paid_usd ELSE 0 END) AS day150_bookings,
               SUM(CASE WHEN datediff ('day',install_day,b.event_day) <= 180 THEN amount_paid_usd ELSE 0 END) AS day180_bookings,
               SUM(CASE WHEN datediff ('day',install_day,b.event_day) <= 210 THEN amount_paid_usd ELSE 0 END) AS day210_bookings,
               SUM(CASE WHEN datediff ('day',install_day,b.event_day) <= 240 THEN amount_paid_usd ELSE 0 END) AS day240_bookings,
               SUM(CASE WHEN datediff ('day',install_day,b.event_day) <= 270 THEN amount_paid_usd ELSE 0 END) AS day270_bookings,
               SUM(CASE WHEN datediff ('day',install_day,b.event_day) <= 300 THEN amount_paid_usd ELSE 0 END) AS day300_bookings,
               SUM(CASE WHEN datediff ('day',install_day,b.event_day) <= 330 THEN amount_paid_usd ELSE 0 END) AS day330_bookings,
               SUM(CASE WHEN datediff ('day',install_day,b.event_day) <= 360 THEN amount_paid_usd ELSE 0 END) AS day360_bookings,
               SUM(CASE WHEN datediff ('day',install_day,b.event_day) <= 540 THEN amount_paid_usd ELSE 0 END) AS day540_bookings,
               SUM(CASE WHEN datediff ('day',install_day,b.event_day) <= 735 THEN amount_paid_usd ELSE 0 END) AS day735_bookings,
               MAX(CASE WHEN datediff ('day',install_day,b.event_day) <= 1 THEN 1 ELSE 0 END) AS day1_payer,
               MAX(CASE WHEN datediff ('day',install_day,b.event_day) <= 2 THEN 1 ELSE 0 END) AS day2_payer,
               MAX(CASE WHEN datediff ('day',install_day,b.event_day) <= 3 THEN 1 ELSE 0 END) AS day3_payer,
               MAX(CASE WHEN datediff ('day',install_day,b.event_day) <= 7 THEN 1 ELSE 0 END) AS day7_payer,
               MAX(CASE WHEN datediff ('day',install_day,b.event_day) <= 14 THEN 1 ELSE 0 END) AS day14_payer,
               MAX(CASE WHEN datediff ('day',install_day,b.event_day) <= 30 THEN 1 ELSE 0 END) AS day30_payer,
               MAX(CASE WHEN datediff ('day',install_day,b.event_day) <= 60 THEN 1 ELSE 0 END) AS day60_payer,
               MAX(CASE WHEN datediff ('day',install_day,b.event_day) <= 90 THEN 1 ELSE 0 END) AS day90_payer,
               MAX(CASE WHEN datediff ('day',install_day,b.event_day) <= 180 THEN 1 ELSE 0 END) AS day180_payer,
               MAX(CASE WHEN datediff ('day',install_day,b.event_day) <= 360 THEN 1 ELSE 0 END) AS day360_payer,
               MAX(CASE WHEN datediff ('day',install_day,b.event_day) <= 540 THEN 1 ELSE 0 END) AS day540_payer,
               MAX(CASE WHEN datediff ('day',install_day,b.event_day) <= 540 THEN 1 ELSE 0 END) AS day735_payer,
               MIN(b.tournament_entry_time) AS first_cef_timestamp,
               MAX(b.tournament_entry_time) AS last_cef_timestamp,
               SUM(nvl (cef,0)) AS todate_cef,
               MAX(CASE WHEN b.tournament_entry_time IS NOT NULL THEN 1 ELSE 0 END) AS cash_entrant,
               SUM(CASE WHEN datediff ('day',install_day,b.tournament_entry_day) <= 1 THEN cef ELSE 0 END) AS day1_cef,
               SUM(CASE WHEN datediff ('day',install_day,b.tournament_entry_day) <= 2 THEN cef ELSE 0 END) AS day2_cef,
               SUM(CASE WHEN datediff ('day',install_day,b.tournament_entry_day) <= 3 THEN cef ELSE 0 END) AS day3_cef,
               SUM(CASE WHEN datediff ('day',install_day,b.tournament_entry_day) <= 7 THEN cef ELSE 0 END) AS day7_cef,
               SUM(CASE WHEN datediff ('day',install_day,b.tournament_entry_day) <= 14 THEN cef ELSE 0 END) AS day14_cef,
               SUM(CASE WHEN datediff ('day',install_day,b.tournament_entry_day) <= 30 THEN cef ELSE 0 END) AS day30_cef,
               SUM(CASE WHEN datediff ('day',install_day,b.tournament_entry_day) <= 60 THEN cef ELSE 0 END) AS day60_cef,
               SUM(CASE WHEN datediff ('day',install_day,b.tournament_entry_day) <= 90 THEN cef ELSE 0 END) AS day90_cef,
               SUM(CASE WHEN datediff ('day',install_day,b.tournament_entry_day) <= 120 THEN cef ELSE 0 END) AS day120_cef,
               SUM(CASE WHEN datediff ('day',install_day,b.tournament_entry_day) <= 150 THEN cef ELSE 0 END) AS day150_cef,
               SUM(CASE WHEN datediff ('day',install_day,b.tournament_entry_day) <= 180 THEN cef ELSE 0 END) AS day180_cef,
               SUM(CASE WHEN datediff ('day',install_day,b.tournament_entry_day) <= 210 THEN cef ELSE 0 END) AS day210_cef,
               SUM(CASE WHEN datediff ('day',install_day,b.tournament_entry_day) <= 240 THEN cef ELSE 0 END) AS day240_cef,
               SUM(CASE WHEN datediff ('day',install_day,b.tournament_entry_day) <= 270 THEN cef ELSE 0 END) AS day270_cef,
               SUM(CASE WHEN datediff ('day',install_day,b.tournament_entry_day) <= 300 THEN cef ELSE 0 END) AS day300_cef,
               SUM(CASE WHEN datediff ('day',install_day,b.tournament_entry_day) <= 330 THEN cef ELSE 0 END) AS day330_cef,
               SUM(CASE WHEN datediff ('day',install_day,b.tournament_entry_day) <= 360 THEN cef ELSE 0 END) AS day360_cef,
               SUM(CASE WHEN datediff ('day',install_day,b.tournament_entry_day) <= 540 THEN cef ELSE 0 END) AS day540_cef,
               SUM(CASE WHEN datediff ('day',install_day,b.tournament_entry_day) <= 735 THEN cef ELSE 0 END) AS day735_cef,
               MAX(CASE WHEN datediff ('day',install_day,b.tournament_entry_day) <= 1 THEN 1 ELSE 0 END) AS day1_cash_entrant,
               MAX(CASE WHEN datediff ('day',install_day,b.tournament_entry_day) <= 2 THEN 1 ELSE 0 END) AS day2_cash_entrant,
               MAX(CASE WHEN datediff ('day',install_day,b.tournament_entry_day) <= 3 THEN 1 ELSE 0 END) AS day3_cash_entrant,
               MAX(CASE WHEN datediff ('day',install_day,b.tournament_entry_day) <= 7 THEN 1 ELSE 0 END) AS day7_cash_entrant,
               MAX(CASE WHEN datediff ('day',install_day,b.tournament_entry_day) <= 14 THEN 1 ELSE 0 END) AS day14_cash_entrant,
               MAX(CASE WHEN datediff ('day',install_day,b.tournament_entry_day) <= 30 THEN 1 ELSE 0 END) AS day30_cash_entrant,
               MAX(CASE WHEN datediff ('day',install_day,b.tournament_entry_day) <= 60 THEN 1 ELSE 0 END) AS day60_cash_entrant,
               MAX(CASE WHEN datediff ('day',install_day,b.tournament_entry_day) <= 90 THEN 1 ELSE 0 END) AS day90_cash_entrant,
               MAX(CASE WHEN datediff ('day',install_day,b.tournament_entry_day) <= 180 THEN 1 ELSE 0 END) AS day180_cash_entrant,
               MAX(CASE WHEN datediff ('day',install_day,b.tournament_entry_day) <= 360 THEN 1 ELSE 0 END) AS day360_cash_entrant,
               MAX(CASE WHEN datediff ('day',install_day,b.tournament_entry_day) <= 540 THEN 1 ELSE 0 END) AS day540_cash_entrant,
               MAX(CASE WHEN datediff ('day',install_day,b.tournament_entry_day) <= 540 THEN 1 ELSE 0 END) AS day735_cash_entrant,
               -- by platform
               SUM(CASE WHEN b.ww_platform = 'app' AND datediff ('day',install_day,b.event_day) <= 1 THEN amount_paid_usd ELSE 0 END) AS app_day1_bookings,
               SUM(CASE WHEN b.ww_platform = 'app' AND datediff ('day',install_day,b.event_day) <= 2 THEN amount_paid_usd ELSE 0 END) AS app_day2_bookings,
               SUM(CASE WHEN b.ww_platform = 'app' AND datediff ('day',install_day,b.event_day) <= 3 THEN amount_paid_usd ELSE 0 END) AS app_day3_bookings,
               SUM(CASE WHEN b.ww_platform = 'app' AND datediff ('day',install_day,b.event_day) <= 7 THEN amount_paid_usd ELSE 0 END) AS app_day7_bookings,
               SUM(CASE WHEN b.ww_platform = 'app' AND datediff ('day',install_day,b.event_day) <= 14 THEN amount_paid_usd ELSE 0 END) AS app_day14_bookings,
               SUM(CASE WHEN b.ww_platform = 'app' AND datediff ('day',install_day,b.event_day) <= 30 THEN amount_paid_usd ELSE 0 END) AS app_day30_bookings,
               SUM(CASE WHEN b.ww_platform = 'app' AND datediff ('day',install_day,b.event_day) <= 60 THEN amount_paid_usd ELSE 0 END) AS app_day60_bookings,
               SUM(CASE WHEN b.ww_platform = 'app' AND datediff ('day',install_day,b.event_day) <= 90 THEN amount_paid_usd ELSE 0 END) AS app_day90_bookings,
               SUM(CASE WHEN b.ww_platform = 'app' AND datediff ('day',install_day,b.event_day) <= 180 THEN amount_paid_usd ELSE 0 END) AS app_day180_bookings,
               SUM(CASE WHEN b.ww_platform = 'app' AND datediff ('day',install_day,b.event_day) <= 360 THEN amount_paid_usd ELSE 0 END) AS app_day360_bookings,
               SUM(CASE WHEN b.ww_platform = 'app' AND datediff ('day',install_day,b.event_day) <= 540 THEN amount_paid_usd ELSE 0 END) AS app_day540_bookings,
               SUM(CASE WHEN b.ww_platform = 'app' AND datediff ('day',install_day,b.event_day) <= 735 THEN amount_paid_usd ELSE 0 END) AS app_day735_bookings,
               SUM(CASE WHEN b.ww_platform = 'moweb' AND datediff ('day',install_day,b.event_day) <= 1 THEN amount_paid_usd ELSE 0 END) AS moweb_day1_bookings,
               SUM(CASE WHEN b.ww_platform = 'moweb' AND datediff ('day',install_day,b.event_day) <= 2 THEN amount_paid_usd ELSE 0 END) AS moweb_day2_bookings,
               SUM(CASE WHEN b.ww_platform = 'moweb' AND datediff ('day',install_day,b.event_day) <= 3 THEN amount_paid_usd ELSE 0 END) AS moweb_day3_bookings,
               SUM(CASE WHEN b.ww_platform = 'moweb' AND datediff ('day',install_day,b.event_day) <= 7 THEN amount_paid_usd ELSE 0 END) AS moweb_day7_bookings,
               SUM(CASE WHEN b.ww_platform = 'moweb' AND datediff ('day',install_day,b.event_day) <= 14 THEN amount_paid_usd ELSE 0 END) AS moweb_day14_bookings,
               SUM(CASE WHEN b.ww_platform = 'moweb' AND datediff ('day',install_day,b.event_day) <= 30 THEN amount_paid_usd ELSE 0 END) AS moweb_day30_bookings,
               SUM(CASE WHEN b.ww_platform = 'moweb' AND datediff ('day',install_day,b.event_day) <= 60 THEN amount_paid_usd ELSE 0 END) AS moweb_day60_bookings,
               SUM(CASE WHEN b.ww_platform = 'moweb' AND datediff ('day',install_day,b.event_day) <= 90 THEN amount_paid_usd ELSE 0 END) AS moweb_day90_bookings,
               SUM(CASE WHEN b.ww_platform = 'moweb' AND datediff ('day',install_day,b.event_day) <= 180 THEN amount_paid_usd ELSE 0 END) AS moweb_day180_bookings,
               SUM(CASE WHEN b.ww_platform = 'moweb' AND datediff ('day',install_day,b.event_day) <= 360 THEN amount_paid_usd ELSE 0 END) AS moweb_day360_bookings,
               SUM(CASE WHEN b.ww_platform = 'moweb' AND datediff ('day',install_day,b.event_day) <= 540 THEN amount_paid_usd ELSE 0 END) AS moweb_day540_bookings,
               SUM(CASE WHEN b.ww_platform = 'moweb' AND datediff ('day',install_day,b.event_day) <= 735 THEN amount_paid_usd ELSE 0 END) AS moweb_day735_bookings,
               SUM(CASE WHEN b.ww_platform = 'desktop' AND datediff ('day',install_day,b.event_day) <= 1 THEN amount_paid_usd ELSE 0 END) AS desktop_day1_bookings,
               SUM(CASE WHEN b.ww_platform = 'desktop' AND datediff ('day',install_day,b.event_day) <= 2 THEN amount_paid_usd ELSE 0 END) AS desktop_day2_bookings,
               SUM(CASE WHEN b.ww_platform = 'desktop' AND datediff ('day',install_day,b.event_day) <= 3 THEN amount_paid_usd ELSE 0 END) AS desktop_day3_bookings,
               SUM(CASE WHEN b.ww_platform = 'desktop' AND datediff ('day',install_day,b.event_day) <= 7 THEN amount_paid_usd ELSE 0 END) AS desktop_day7_bookings,
               SUM(CASE WHEN b.ww_platform = 'desktop' AND datediff ('day',install_day,b.event_day) <= 14 THEN amount_paid_usd ELSE 0 END) AS desktop_day14_bookings,
               SUM(CASE WHEN b.ww_platform = 'desktop' AND datediff ('day',install_day,b.event_day) <= 30 THEN amount_paid_usd ELSE 0 END) AS desktop_day30_bookings,
               SUM(CASE WHEN b.ww_platform = 'desktop' AND datediff ('day',install_day,b.event_day) <= 60 THEN amount_paid_usd ELSE 0 END) AS desktop_day60_bookings,
               SUM(CASE WHEN b.ww_platform = 'desktop' AND datediff ('day',install_day,b.event_day) <= 90 THEN amount_paid_usd ELSE 0 END) AS desktop_day90_bookings,
               SUM(CASE WHEN b.ww_platform = 'desktop' AND datediff ('day',install_day,b.event_day) <= 180 THEN amount_paid_usd ELSE 0 END) AS desktop_day180_bookings,
               SUM(CASE WHEN b.ww_platform = 'desktop' AND datediff ('day',install_day,b.event_day) <= 360 THEN amount_paid_usd ELSE 0 END) AS desktop_day360_bookings,
               SUM(CASE WHEN b.ww_platform = 'desktop' AND datediff ('day',install_day,b.event_day) <= 540 THEN amount_paid_usd ELSE 0 END) AS desktop_day540_bookings,
               SUM(CASE WHEN b.ww_platform = 'desktop' AND datediff ('day',install_day,b.event_day) <= 735 THEN amount_paid_usd ELSE 0 END) AS desktop_day735_bookings,
               MAX(CASE WHEN b.ww_platform = 'app' AND datediff ('day',install_day,b.event_day) <= 1 THEN 1 ELSE 0 END) AS app_day1_payer,
               MAX(CASE WHEN  b.ww_platform = 'app' AND datediff ('day',install_day,b.event_day) <= 2 THEN 1 ELSE 0 END) AS app_day2_payer,
               MAX(CASE WHEN  b.ww_platform = 'app' AND datediff ('day',install_day,b.event_day) <= 3 THEN 1 ELSE 0 END) AS app_day3_payer,
               MAX(CASE WHEN  b.ww_platform = 'app' AND datediff ('day',install_day,b.event_day) <= 7 THEN 1 ELSE 0 END) AS app_day7_payer,
               MAX(CASE WHEN  b.ww_platform = 'app' AND datediff ('day',install_day,b.event_day) <= 14 THEN 1 ELSE 0 END) AS app_day14_payer,
               MAX(CASE WHEN  b.ww_platform = 'app' AND datediff ('day',install_day,b.event_day) <= 30 THEN 1 ELSE 0 END) AS app_day30_payer,
               MAX(CASE WHEN  b.ww_platform = 'app' AND datediff ('day',install_day,b.event_day) <= 60 THEN 1 ELSE 0 END) AS app_day60_payer,
               MAX(CASE WHEN  b.ww_platform = 'app' AND datediff ('day',install_day,b.event_day) <= 90 THEN 1 ELSE 0 END) AS app_day90_payer,
               MAX(CASE WHEN  b.ww_platform = 'app' AND datediff ('day',install_day,b.event_day) <= 180 THEN 1 ELSE 0 END) AS app_day180_payer,
               MAX(CASE WHEN  b.ww_platform = 'app' AND datediff ('day',install_day,b.event_day) <= 360 THEN 1 ELSE 0 END) AS app_day360_payer,
               MAX(CASE WHEN  b.ww_platform = 'app' AND datediff ('day',install_day,b.event_day) <= 540 THEN 1 ELSE 0 END) AS app_day540_payer,
               MAX(CASE WHEN  b.ww_platform = 'app' AND datediff ('day',install_day,b.event_day) <= 540 THEN 1 ELSE 0 END) AS app_day735_payer,
               MAX(CASE WHEN b.ww_platform = 'moweb' AND datediff ('day',install_day,b.event_day) <= 1 THEN 1 ELSE 0 END) AS moweb_day1_payer,
               MAX(CASE WHEN  b.ww_platform = 'moweb' AND datediff ('day',install_day,b.event_day) <= 2 THEN 1 ELSE 0 END) AS moweb_day2_payer,
               MAX(CASE WHEN  b.ww_platform = 'moweb' AND datediff ('day',install_day,b.event_day) <= 3 THEN 1 ELSE 0 END) AS moweb_day3_payer,
               MAX(CASE WHEN  b.ww_platform = 'moweb' AND datediff ('day',install_day,b.event_day) <= 7 THEN 1 ELSE 0 END) AS moweb_day7_payer,
               MAX(CASE WHEN  b.ww_platform = 'moweb' AND datediff ('day',install_day,b.event_day) <= 14 THEN 1 ELSE 0 END) AS moweb_day14_payer,
               MAX(CASE WHEN  b.ww_platform = 'moweb' AND datediff ('day',install_day,b.event_day) <= 30 THEN 1 ELSE 0 END) AS moweb_day30_payer,
               MAX(CASE WHEN  b.ww_platform = 'moweb' AND datediff ('day',install_day,b.event_day) <= 60 THEN 1 ELSE 0 END) AS moweb_day60_payer,
               MAX(CASE WHEN  b.ww_platform = 'moweb' AND datediff ('day',install_day,b.event_day) <= 90 THEN 1 ELSE 0 END) AS moweb_day90_payer,
               MAX(CASE WHEN  b.ww_platform = 'moweb' AND datediff ('day',install_day,b.event_day) <= 180 THEN 1 ELSE 0 END) AS moweb_day180_payer,
               MAX(CASE WHEN  b.ww_platform = 'moweb' AND datediff ('day',install_day,b.event_day) <= 360 THEN 1 ELSE 0 END) AS moweb_day360_payer,
               MAX(CASE WHEN  b.ww_platform = 'moweb' AND datediff ('day',install_day,b.event_day) <= 540 THEN 1 ELSE 0 END) AS moweb_day540_payer,
               MAX(CASE WHEN  b.ww_platform = 'moweb' AND datediff ('day',install_day,b.event_day) <= 540 THEN 1 ELSE 0 END) AS moweb_day735_payer,
               MAX(CASE WHEN b.ww_platform = 'desktop' AND datediff ('day',install_day,b.event_day) <= 1 THEN 1 ELSE 0 END) AS desktop_day1_payer,
               MAX(CASE WHEN  b.ww_platform = 'desktop' AND datediff ('day',install_day,b.event_day) <= 2 THEN 1 ELSE 0 END) AS desktop_day2_payer,
               MAX(CASE WHEN  b.ww_platform = 'desktop' AND datediff ('day',install_day,b.event_day) <= 3 THEN 1 ELSE 0 END) AS desktop_day3_payer,
               MAX(CASE WHEN  b.ww_platform = 'desktop' AND datediff ('day',install_day,b.event_day) <= 7 THEN 1 ELSE 0 END) AS desktop_day7_payer,
               MAX(CASE WHEN  b.ww_platform = 'desktop' AND datediff ('day',install_day,b.event_day) <= 14 THEN 1 ELSE 0 END) AS desktop_day14_payer,
               MAX(CASE WHEN  b.ww_platform = 'desktop' AND datediff ('day',install_day,b.event_day) <= 30 THEN 1 ELSE 0 END) AS desktop_day30_payer,
               MAX(CASE WHEN  b.ww_platform = 'desktop' AND datediff ('day',install_day,b.event_day) <= 60 THEN 1 ELSE 0 END) AS desktop_day60_payer,
               MAX(CASE WHEN  b.ww_platform = 'desktop' AND datediff ('day',install_day,b.event_day) <= 90 THEN 1 ELSE 0 END) AS desktop_day90_payer,
               MAX(CASE WHEN  b.ww_platform = 'desktop' AND datediff ('day',install_day,b.event_day) <= 180 THEN 1 ELSE 0 END) AS desktop_day180_payer,
               MAX(CASE WHEN  b.ww_platform = 'desktop' AND datediff ('day',install_day,b.event_day) <= 360 THEN 1 ELSE 0 END) AS desktop_day360_payer,
               MAX(CASE WHEN  b.ww_platform = 'desktop' AND datediff ('day',install_day,b.event_day) <= 540 THEN 1 ELSE 0 END) AS desktop_day540_payer,
               MAX(CASE WHEN  b.ww_platform = 'desktop' AND datediff ('day',install_day,b.event_day) <= 540 THEN 1 ELSE 0 END) AS desktop_day735_payer,
               SUM(CASE WHEN  b.ww_platform = 'app' AND datediff ('day',install_day,b.tournament_entry_day) <= 1 THEN cef ELSE 0 END) AS app_day1_cef,
               SUM(CASE WHEN b.ww_platform = 'app' AND datediff ('day',install_day,b.tournament_entry_day) <= 2 THEN cef ELSE 0 END) AS app_day2_cef,
               SUM(CASE WHEN b.ww_platform = 'app' AND datediff ('day',install_day,b.tournament_entry_day) <= 3 THEN cef ELSE 0 END) AS app_day3_cef,
               SUM(CASE WHEN b.ww_platform = 'app' AND datediff ('day',install_day,b.tournament_entry_day) <= 7 THEN cef ELSE 0 END) AS app_day7_cef,
               SUM(CASE WHEN b.ww_platform = 'app' AND datediff ('day',install_day,b.tournament_entry_day) <= 14 THEN cef ELSE 0 END) AS app_day14_cef,
               SUM(CASE WHEN b.ww_platform = 'app' AND datediff ('day',install_day,b.tournament_entry_day) <= 30 THEN cef ELSE 0 END) AS app_day30_cef,
               SUM(CASE WHEN b.ww_platform = 'app' AND datediff ('day',install_day,b.tournament_entry_day) <= 60 THEN cef ELSE 0 END) AS app_day60_cef,
               SUM(CASE WHEN b.ww_platform = 'app' AND datediff ('day',install_day,b.tournament_entry_day) <= 90 THEN cef ELSE 0 END) AS app_day90_cef,
               SUM(CASE WHEN b.ww_platform = 'app' AND datediff ('day',install_day,b.tournament_entry_day) <= 180 THEN cef ELSE 0 END) AS app_day180_cef,
               SUM(CASE WHEN b.ww_platform = 'app' AND datediff ('day',install_day,b.tournament_entry_day) <= 360 THEN cef ELSE 0 END) AS app_day360_cef,
               SUM(CASE WHEN b.ww_platform = 'app' AND datediff ('day',install_day,b.tournament_entry_day) <= 540 THEN cef ELSE 0 END) AS app_day540_cef,
               SUM(CASE WHEN b.ww_platform = 'app' AND datediff ('day',install_day,b.tournament_entry_day) <= 735 THEN cef ELSE 0 END) AS app_day735_cef,
               SUM(CASE WHEN  b.ww_platform = 'moweb' AND datediff ('day',install_day,b.tournament_entry_day) <= 1 THEN cef ELSE 0 END) AS moweb_day1_cef,
               SUM(CASE WHEN b.ww_platform = 'moweb' AND datediff ('day',install_day,b.tournament_entry_day) <= 2 THEN cef ELSE 0 END) AS moweb_day2_cef,
               SUM(CASE WHEN b.ww_platform = 'moweb' AND datediff ('day',install_day,b.tournament_entry_day) <= 3 THEN cef ELSE 0 END) AS moweb_day3_cef,
               SUM(CASE WHEN b.ww_platform = 'moweb' AND datediff ('day',install_day,b.tournament_entry_day) <= 7 THEN cef ELSE 0 END) AS moweb_day7_cef,
               SUM(CASE WHEN b.ww_platform = 'moweb' AND datediff ('day',install_day,b.tournament_entry_day) <= 14 THEN cef ELSE 0 END) AS moweb_day14_cef,
               SUM(CASE WHEN b.ww_platform = 'moweb' AND datediff ('day',install_day,b.tournament_entry_day) <= 30 THEN cef ELSE 0 END) AS moweb_day30_cef,
               SUM(CASE WHEN b.ww_platform = 'moweb' AND datediff ('day',install_day,b.tournament_entry_day) <= 60 THEN cef ELSE 0 END) AS moweb_day60_cef,
               SUM(CASE WHEN b.ww_platform = 'moweb' AND datediff ('day',install_day,b.tournament_entry_day) <= 90 THEN cef ELSE 0 END) AS moweb_day90_cef,
               SUM(CASE WHEN b.ww_platform = 'moweb' AND datediff ('day',install_day,b.tournament_entry_day) <= 180 THEN cef ELSE 0 END) AS moweb_day180_cef,
               SUM(CASE WHEN b.ww_platform = 'moweb' AND datediff ('day',install_day,b.tournament_entry_day) <= 360 THEN cef ELSE 0 END) AS moweb_day360_cef,
               SUM(CASE WHEN b.ww_platform = 'moweb' AND datediff ('day',install_day,b.tournament_entry_day) <= 540 THEN cef ELSE 0 END) AS moweb_day540_cef,
               SUM(CASE WHEN b.ww_platform = 'moweb' AND datediff ('day',install_day,b.tournament_entry_day) <= 735 THEN cef ELSE 0 END) AS moweb_day735_cef,
               SUM(CASE WHEN  b.ww_platform = 'desktop' AND datediff ('day',install_day,b.tournament_entry_day) <= 1 THEN cef ELSE 0 END) AS desktop_day1_cef,
               SUM(CASE WHEN b.ww_platform = 'desktop' AND datediff ('day',install_day,b.tournament_entry_day) <= 2 THEN cef ELSE 0 END) AS desktop_day2_cef,
               SUM(CASE WHEN b.ww_platform = 'desktop' AND datediff ('day',install_day,b.tournament_entry_day) <= 3 THEN cef ELSE 0 END) AS desktop_day3_cef,
               SUM(CASE WHEN b.ww_platform = 'desktop' AND datediff ('day',install_day,b.tournament_entry_day) <= 7 THEN cef ELSE 0 END) AS desktop_day7_cef,
               SUM(CASE WHEN b.ww_platform = 'desktop' AND datediff ('day',install_day,b.tournament_entry_day) <= 14 THEN cef ELSE 0 END) AS desktop_day14_cef,
               SUM(CASE WHEN b.ww_platform = 'desktop' AND datediff ('day',install_day,b.tournament_entry_day) <= 30 THEN cef ELSE 0 END) AS desktop_day30_cef,
               SUM(CASE WHEN b.ww_platform = 'desktop' AND datediff ('day',install_day,b.tournament_entry_day) <= 60 THEN cef ELSE 0 END) AS desktop_day60_cef,
               SUM(CASE WHEN b.ww_platform = 'desktop' AND datediff ('day',install_day,b.tournament_entry_day) <= 90 THEN cef ELSE 0 END) AS desktop_day90_cef,
               SUM(CASE WHEN b.ww_platform = 'desktop' AND datediff ('day',install_day,b.tournament_entry_day) <= 180 THEN cef ELSE 0 END) AS desktop_day180_cef,
               SUM(CASE WHEN b.ww_platform = 'desktop' AND datediff ('day',install_day,b.tournament_entry_day) <= 360 THEN cef ELSE 0 END) AS desktop_day360_cef,
               SUM(CASE WHEN b.ww_platform = 'desktop' AND datediff ('day',install_day,b.tournament_entry_day) <= 540 THEN cef ELSE 0 END) AS desktop_day540_cef,
               SUM(CASE WHEN b.ww_platform = 'desktop' AND datediff ('day',install_day,b.tournament_entry_day) <= 735 THEN cef ELSE 0 END) AS desktop_day735_cef,
               MAX(CASE WHEN b.ww_platform = 'app' AND  datediff ('day',install_day,b.tournament_entry_day) <= 1 THEN 1 ELSE 0 END) AS app_day1_cash_entrant,
               MAX(CASE WHEN b.ww_platform = 'app' AND  datediff ('day',install_day,b.tournament_entry_day) <= 2 THEN 1 ELSE 0 END) AS app_day2_cash_entrant,
               MAX(CASE WHEN b.ww_platform = 'app' AND  datediff ('day',install_day,b.tournament_entry_day) <= 3 THEN 1 ELSE 0 END) AS app_day3_cash_entrant,
               MAX(CASE WHEN b.ww_platform = 'app' AND  datediff ('day',install_day,b.tournament_entry_day) <= 7 THEN 1 ELSE 0 END) AS app_day7_cash_entrant,
               MAX(CASE WHEN b.ww_platform = 'app' AND  datediff ('day',install_day,b.tournament_entry_day) <= 14 THEN 1 ELSE 0 END) AS app_day14_cash_entrant,
               MAX(CASE WHEN b.ww_platform = 'app' AND  datediff ('day',install_day,b.tournament_entry_day) <= 30 THEN 1 ELSE 0 END) AS app_day30_cash_entrant,
               MAX(CASE WHEN b.ww_platform = 'app' AND  datediff ('day',install_day,b.tournament_entry_day) <= 60 THEN 1 ELSE 0 END) AS app_day60_cash_entrant,
               MAX(CASE WHEN b.ww_platform = 'app' AND  datediff ('day',install_day,b.tournament_entry_day) <= 90 THEN 1 ELSE 0 END) AS app_day90_cash_entrant,
               MAX(CASE WHEN b.ww_platform = 'app' AND  datediff ('day',install_day,b.tournament_entry_day) <= 180 THEN 1 ELSE 0 END) AS app_day180_cash_entrant,
               MAX(CASE WHEN b.ww_platform = 'app' AND  datediff ('day',install_day,b.tournament_entry_day) <= 360 THEN 1 ELSE 0 END) AS app_day360_cash_entrant,
               MAX(CASE WHEN b.ww_platform = 'app' AND  datediff ('day',install_day,b.tournament_entry_day) <= 540 THEN 1 ELSE 0 END) AS app_day540_cash_entrant,
               MAX(CASE WHEN b.ww_platform = 'app' AND  datediff ('day',install_day,b.tournament_entry_day) <= 540 THEN 1 ELSE 0 END) AS app_day735_cash_entrant,
               MAX(CASE WHEN b.ww_platform = 'moweb' AND  datediff ('day',install_day,b.tournament_entry_day) <= 1 THEN 1 ELSE 0 END) AS moweb_day1_cash_entrant,
               MAX(CASE WHEN b.ww_platform = 'moweb' AND  datediff ('day',install_day,b.tournament_entry_day) <= 2 THEN 1 ELSE 0 END) AS moweb_day2_cash_entrant,
               MAX(CASE WHEN b.ww_platform = 'moweb' AND  datediff ('day',install_day,b.tournament_entry_day) <= 3 THEN 1 ELSE 0 END) AS moweb_day3_cash_entrant,
               MAX(CASE WHEN b.ww_platform = 'moweb' AND  datediff ('day',install_day,b.tournament_entry_day) <= 7 THEN 1 ELSE 0 END) AS moweb_day7_cash_entrant,
               MAX(CASE WHEN b.ww_platform = 'moweb' AND  datediff ('day',install_day,b.tournament_entry_day) <= 14 THEN 1 ELSE 0 END) AS moweb_day14_cash_entrant,
               MAX(CASE WHEN b.ww_platform = 'moweb' AND  datediff ('day',install_day,b.tournament_entry_day) <= 30 THEN 1 ELSE 0 END) AS moweb_day30_cash_entrant,
               MAX(CASE WHEN b.ww_platform = 'moweb' AND  datediff ('day',install_day,b.tournament_entry_day) <= 60 THEN 1 ELSE 0 END) AS moweb_day60_cash_entrant,
               MAX(CASE WHEN b.ww_platform = 'moweb' AND  datediff ('day',install_day,b.tournament_entry_day) <= 90 THEN 1 ELSE 0 END) AS moweb_day90_cash_entrant,
               MAX(CASE WHEN b.ww_platform = 'moweb' AND  datediff ('day',install_day,b.tournament_entry_day) <= 180 THEN 1 ELSE 0 END) AS moweb_day180_cash_entrant,
               MAX(CASE WHEN b.ww_platform = 'moweb' AND  datediff ('day',install_day,b.tournament_entry_day) <= 360 THEN 1 ELSE 0 END) AS moweb_day360_cash_entrant,
               MAX(CASE WHEN b.ww_platform = 'moweb' AND  datediff ('day',install_day,b.tournament_entry_day) <= 540 THEN 1 ELSE 0 END) AS moweb_day540_cash_entrant,
               MAX(CASE WHEN b.ww_platform = 'moweb' AND  datediff ('day',install_day,b.tournament_entry_day) <= 540 THEN 1 ELSE 0 END) AS moweb_day735_cash_entrant,
               MAX(CASE WHEN b.ww_platform = 'desktop' AND  datediff ('day',install_day,b.tournament_entry_day) <= 1 THEN 1 ELSE 0 END) AS desktop_day1_cash_entrant,
               MAX(CASE WHEN b.ww_platform = 'desktop' AND  datediff ('day',install_day,b.tournament_entry_day) <= 2 THEN 1 ELSE 0 END) AS desktop_day2_cash_entrant,
               MAX(CASE WHEN b.ww_platform = 'desktop' AND  datediff ('day',install_day,b.tournament_entry_day) <= 3 THEN 1 ELSE 0 END) AS desktop_day3_cash_entrant,
               MAX(CASE WHEN b.ww_platform = 'desktop' AND  datediff ('day',install_day,b.tournament_entry_day) <= 7 THEN 1 ELSE 0 END) AS desktop_day7_cash_entrant,
               MAX(CASE WHEN b.ww_platform = 'desktop' AND  datediff ('day',install_day,b.tournament_entry_day) <= 14 THEN 1 ELSE 0 END) AS desktop_day14_cash_entrant,
               MAX(CASE WHEN b.ww_platform = 'desktop' AND  datediff ('day',install_day,b.tournament_entry_day) <= 30 THEN 1 ELSE 0 END) AS desktop_day30_cash_entrant,
               MAX(CASE WHEN b.ww_platform = 'desktop' AND  datediff ('day',install_day,b.tournament_entry_day) <= 60 THEN 1 ELSE 0 END) AS desktop_day60_cash_entrant,
               MAX(CASE WHEN b.ww_platform = 'desktop' AND  datediff ('day',install_day,b.tournament_entry_day) <= 90 THEN 1 ELSE 0 END) AS desktop_day90_cash_entrant,
               MAX(CASE WHEN b.ww_platform = 'desktop' AND  datediff ('day',install_day,b.tournament_entry_day) <= 180 THEN 1 ELSE 0 END) AS desktop_day180_cash_entrant,
               MAX(CASE WHEN b.ww_platform = 'desktop' AND  datediff ('day',install_day,b.tournament_entry_day) <= 360 THEN 1 ELSE 0 END) AS desktop_day360_cash_entrant,
               MAX(CASE WHEN b.ww_platform = 'desktop' AND  datediff ('day',install_day,b.tournament_entry_day) <= 540 THEN 1 ELSE 0 END) AS desktop_day540_cash_entrant,
               MAX(CASE WHEN b.ww_platform = 'desktop' AND  datediff ('day',install_day,b.tournament_entry_day) <= 540 THEN 1 ELSE 0 END) AS desktop_day735_cash_entrant
      FROM kds_phoenix_base x
      --for bookings info
        LEFT OUTER JOIN (SELECT
                                tournament_close_time AS event_time,
                                tournament_close_day AS event_day,
                                tournament_entry_day,
                                tournament_entry_time,
                                CAST(entry_nar AS FLOAT) AS amount_paid_usd,
                                CAST(cef AS FLOAT) AS cef,
                                idfv,
                                ww_platform
                                FROM phoenix.tournament_all_platform_cef_nar_v) b
                     ON x.idfv = b.idfv
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19;

CREATE local TEMPORARY TABLE kds_phoenix_base_level_2 ON COMMIT preserve rows direct AS
      SELECT
        x.*,
        MIN(CASE WHEN dw.event_name = 'cashDeposit' THEN dw.event_time ELSE NULL END) AS first_deposit_timestamp,
        MAX(CASE WHEN dw.event_name = 'cashDeposit' THEN dw.event_time ELSE NULL END) AS last_deposit_timestamp,
        SUM(CASE WHEN dw.event_name = 'cashDeposit' THEN amount ELSE 0 END) AS todate_deposits,
        COUNT(CASE WHEN event_name = 'cashDeposit' THEN 1 ELSE NULL END) AS todate_deposit_count,
        MAX(CASE WHEN dw.event_name = 'cashDeposit' THEN 1 ELSE 0 END) AS depositor,
        SUM(CASE WHEN dw.event_name = 'cashDeposit' AND datediff ('day',install_day,dw.event_day) <= 1 THEN amount ELSE 0 END) AS day1_deposits,
        SUM(CASE WHEN dw.event_name = 'cashDeposit' AND datediff ('day',install_day,dw.event_day) <= 2 THEN amount ELSE 0 END) AS day2_deposits,
        SUM(CASE WHEN dw.event_name = 'cashDeposit' AND datediff ('day',install_day,dw.event_day) <= 3 THEN amount ELSE 0 END) AS day3_deposits,
        SUM(CASE WHEN dw.event_name = 'cashDeposit' AND datediff ('day',install_day,dw.event_day) <= 7 THEN amount ELSE 0 END) AS day7_deposits,
        SUM(CASE WHEN dw.event_name = 'cashDeposit' AND datediff ('day',install_day,dw.event_day) <= 14 THEN amount ELSE 0 END) AS day14_deposits,
        SUM(CASE WHEN dw.event_name = 'cashDeposit' AND datediff ('day',install_day,dw.event_day) <= 30 THEN amount ELSE 0 END) AS day30_deposits,
        SUM(CASE WHEN dw.event_name = 'cashDeposit' AND datediff ('day',install_day,dw.event_day) <= 60 THEN amount ELSE 0 END) AS day60_deposits,
        SUM(CASE WHEN dw.event_name = 'cashDeposit' AND datediff ('day',install_day,dw.event_day) <= 90 THEN amount ELSE 0 END) AS day90_deposits,
        SUM(CASE WHEN dw.event_name = 'cashDeposit' AND datediff ('day',install_day,dw.event_day) <= 120 THEN amount ELSE 0 END) AS day120_deposits,
        SUM(CASE WHEN dw.event_name = 'cashDeposit' AND datediff ('day',install_day,dw.event_day) <= 150 THEN amount ELSE 0 END) AS day150_deposits,
        SUM(CASE WHEN dw.event_name = 'cashDeposit' AND datediff ('day',install_day,dw.event_day) <= 180 THEN amount ELSE 0 END) AS day180_deposits,
        SUM(CASE WHEN dw.event_name = 'cashDeposit' AND datediff ('day',install_day,dw.event_day) <= 210 THEN amount ELSE 0 END) AS day210_deposits,
        SUM(CASE WHEN dw.event_name = 'cashDeposit' AND datediff ('day',install_day,dw.event_day) <= 240 THEN amount ELSE 0 END) AS day240_deposits,
        SUM(CASE WHEN dw.event_name = 'cashDeposit' AND datediff ('day',install_day,dw.event_day) <= 270 THEN amount ELSE 0 END) AS day270_deposits,
        SUM(CASE WHEN dw.event_name = 'cashDeposit' AND datediff ('day',install_day,dw.event_day) <= 300 THEN amount ELSE 0 END) AS day300_deposits,
        SUM(CASE WHEN dw.event_name = 'cashDeposit' AND datediff ('day',install_day,dw.event_day) <= 330 THEN amount ELSE 0 END) AS day330_deposits,
        SUM(CASE WHEN dw.event_name = 'cashDeposit' AND datediff ('day',install_day,dw.event_day) <= 360 THEN amount ELSE 0 END) AS day360_deposits,
        SUM(CASE WHEN dw.event_name = 'cashDeposit' AND datediff ('day',install_day,dw.event_day) <= 540 THEN amount ELSE 0 END) AS day540_deposits,
        SUM(CASE WHEN dw.event_name = 'cashDeposit' AND datediff ('day',install_day,dw.event_day) <= 735 THEN amount ELSE 0 END) AS day735_deposits,
        MIN(CASE WHEN dw.event_name = 'cashWithdrawal' THEN dw.event_time ELSE NULL END) AS first_withdrawal_timestamp,
        MAX(CASE WHEN dw.event_name = 'cashWithdrawal' THEN dw.event_time ELSE NULL END) AS last_withdrawal_timestamp,
        SUM(CASE WHEN dw.event_name = 'cashWithdrawal' THEN amount ELSE 0 END) AS todate_withdrawals,
        COUNT(CASE WHEN event_name = 'cashWithdrawal' THEN 1 ELSE NULL END) AS todate_withdrawal_count,
        MAX(CASE WHEN dw.event_name = 'cashWithdrawal' THEN 1 ELSE 0 END) AS withdrawer,
        SUM(CASE WHEN dw.event_name = 'cashWithdrawal' AND datediff ('day',install_day,dw.event_day) <= 1 THEN amount ELSE 0 END) AS day1_withdrawals,
        SUM(CASE WHEN dw.event_name = 'cashWithdrawal' AND datediff ('day',install_day,dw.event_day) <= 2 THEN amount ELSE 0 END) AS day2_withdrawals,
        SUM(CASE WHEN dw.event_name = 'cashWithdrawal' AND datediff ('day',install_day,dw.event_day) <= 3 THEN amount ELSE 0 END) AS day3_withdrawals,
        SUM(CASE WHEN dw.event_name = 'cashWithdrawal' AND datediff ('day',install_day,dw.event_day) <= 7 THEN amount ELSE 0 END) AS day7_withdrawals,
        SUM(CASE WHEN dw.event_name = 'cashWithdrawal' AND datediff ('day',install_day,dw.event_day) <= 14 THEN amount ELSE 0 END) AS day14_withdrawals,
        SUM(CASE WHEN dw.event_name = 'cashWithdrawal' AND datediff ('day',install_day,dw.event_day) <= 30 THEN amount ELSE 0 END) AS day30_withdrawals,
        SUM(CASE WHEN dw.event_name = 'cashWithdrawal' AND datediff ('day',install_day,dw.event_day) <= 60 THEN amount ELSE 0 END) AS day60_withdrawals,
        SUM(CASE WHEN dw.event_name = 'cashWithdrawal' AND datediff ('day',install_day,dw.event_day) <= 90 THEN amount ELSE 0 END) AS day90_withdrawals,
        SUM(CASE WHEN dw.event_name = 'cashWithdrawal' AND datediff ('day',install_day,dw.event_day) <= 120 THEN amount ELSE 0 END) AS day120_withdrawals,
        SUM(CASE WHEN dw.event_name = 'cashWithdrawal' AND datediff ('day',install_day,dw.event_day) <= 150 THEN amount ELSE 0 END) AS day150_withdrawals,
        SUM(CASE WHEN dw.event_name = 'cashWithdrawal' AND datediff ('day',install_day,dw.event_day) <= 180 THEN amount ELSE 0 END) AS day180_withdrawals,
        SUM(CASE WHEN dw.event_name = 'cashWithdrawal' AND datediff ('day',install_day,dw.event_day) <= 210 THEN amount ELSE 0 END) AS day210_withdrawals,
        SUM(CASE WHEN dw.event_name = 'cashWithdrawal' AND datediff ('day',install_day,dw.event_day) <= 240 THEN amount ELSE 0 END) AS day240_withdrawals,
        SUM(CASE WHEN dw.event_name = 'cashWithdrawal' AND datediff ('day',install_day,dw.event_day) <= 270 THEN amount ELSE 0 END) AS day270_withdrawals,
        SUM(CASE WHEN dw.event_name = 'cashWithdrawal' AND datediff ('day',install_day,dw.event_day) <= 300 THEN amount ELSE 0 END) AS day300_withdrawals,
        SUM(CASE WHEN dw.event_name = 'cashWithdrawal' AND datediff ('day',install_day,dw.event_day) <= 330 THEN amount ELSE 0 END) AS day330_withdrawals,
        SUM(CASE WHEN dw.event_name = 'cashWithdrawal' AND datediff ('day',install_day,dw.event_day) <= 360 THEN amount ELSE 0 END) AS day360_withdrawals,
        SUM(CASE WHEN dw.event_name = 'cashWithdrawal' AND datediff ('day',install_day,dw.event_day) <= 540 THEN amount ELSE 0 END) AS day540_withdrawals,
        SUM(CASE WHEN dw.event_name = 'cashWithdrawal' AND datediff ('day',install_day,dw.event_day) <= 735 THEN amount ELSE 0 END) AS day735_withdrawals
      FROM kds_phoenix_base_level_1 x
LEFT OUTER JOIN phoenix.deposits_withdrawals dw ON x.idfv = dw.idfv
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
      25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48,
      49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72,
      73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88,89,90 ,91 ,92 ,93 ,94 ,95 ,96 ,97 ,
      98 ,99 ,100 ,101 ,102 ,103 ,104 ,105 ,106 ,107 ,108 ,109 ,110 ,111 ,112 ,113 ,114 ,115 ,116 ,117 ,
      118 ,119 ,120 ,121 ,122 ,123 ,124 ,125 ,126 ,127 ,128 ,129 ,130 ,131 ,132 ,133 ,134 ,135 ,136 ,137 ,
      138 ,139 ,140 ,141 ,142 ,143 ,144 ,145 ,146 ,147 ,148 ,149 ,150 ,151 ,152 ,153 ,154 ,155 ,156 ,157 ,
      158 ,159 ,160 ,161 ,162 ,163 ,164 ,165 ,166 ,167 ,168 ,169 ,170 ,171 ,172 ,173 ,174 ,175 ,176 ,177 ,
      178 ,179 ,180 ,181 ,182 ,183 ,184 ,185 ,186 ,187 ,188 ,189 ,190 ,191 ,192 ,193 ,194 ,195 ,196 ,197 ,
      198 ,199 ,200 ,201 ,202 ,203 ,204 ,205 ,206 ,207 ,208 ,209 ,210 ,211 ,212 ,213 ,214 ,215 ,216 ,217 ,218 ,
       219 ,220 ,221 ,222 ,223 ,224 ,225 ,226 ,227 ,228 ,229 ,230 ,231 ,232, 233;

CREATE local TEMPORARY TABLE phoenix_events_dim_user ON COMMIT preserve rows direct AS
    select
        idfv,
        a.user_id,
        event_time as first_ww_app_login,
        b.advertiser_id,
        timestampadd('hour', -3, createdate::TIMESTAMP) as first_ww_reg
    from (
        select
            idfv,
            user_id,
            event_time,
            row_number() over (partition by idfv order by event_time asc) as rownum
        from (
            select
                idfv,
                user_id,
                min(event_time) as event_time
            from phoenix.events_client
            where user_id is not null
            group by 1,2
            order by 1,3
        ) a
        order by 1,3 asc
    )a
    left join ww.dim_users b
    on a.user_id=b.user_id
    where rownum=1;

Insert /*+ direct,LABEL ('airflow-ua-ua_master_main_dag.phoenix_subdag-t_update_kochava_device_summary_phoenix')*/
into gsnmobile.kochava_device_summary_phoenix_ods
SELECT x.idfv,
       nvl(nvl(x.platform,e.platform), 'ios') AS platform,
       x.install_date AS install_timestamp,
       x.install_day,
       'PHOENIX' AS app_name,
       x.campaign_name,
       x.campaign_id,
       x.site_id,
       x.network_name,
       x.creative,
       MAX(x.country_code) AS country_code,
       x.request_campaign_id,
       x.request_adgroup,
       x.request_keyword,
       x.request_matchtype,
       --need to update once we get this
       MAX(e.device_type) as model,
       MAX(e.device_model) AS device,
       NULL AS most_recent_os_version,
       x.first_payment_timestamp,
       x.last_payment_timestamp,
       MIN(e.event_day) AS first_active_day,
       MAX(e.event_day) AS last_active_day,
       MIN(first_ww_app_login) AS first_ww_app_login,
       MIN(first_ww_reg) AS first_ww_reg,
       x.todate_bookings,
       x.payer,
       x.day1_bookings,
       x.day2_bookings,
       x.day3_bookings,
       x.day7_bookings,
       x.day14_bookings,
       x.day30_bookings,
       x.day60_bookings,
       x.day90_bookings,
       x.day180_bookings,
       x.day360_bookings,
       x.day540_bookings,
       x.day735_bookings,
       x.day1_payer,
       x.day2_payer,
       x.day3_payer,
       x.day7_payer,
       x.day14_payer,
       x.day30_payer,
       x.day60_payer,
       x.day90_payer,
       x.day180_payer,
       x.day360_payer,
       x.day540_payer,
       x.day735_payer,
       x.first_cef_timestamp,
       x.last_cef_timestamp,
       x.todate_cef,
       x.cash_entrant,
       x.day1_cef,
       x.day2_cef,
       x.day3_cef,
       x.day7_cef,
       x.day14_cef,
       x.day30_cef,
       x.day60_cef,
       x.day90_cef,
       x.day180_cef,
       x.day360_cef,
       x.day540_cef,
       x.day735_cef,
       x.day1_cash_entrant,
       x.day2_cash_entrant,
       x.day3_cash_entrant,
       x.day7_cash_entrant,
       x.day14_cash_entrant,
       x.day30_cash_entrant,
       x.day60_cash_entrant,
       x.day90_cash_entrant,
       x.day180_cash_entrant,
       x.day360_cash_entrant,
       x.day540_cash_entrant,
       x.day735_cash_entrant,
       x.first_deposit_timestamp,
       x.last_deposit_timestamp,
       x.todate_deposits,
       x.todate_deposit_count,
       x.depositor,
       x.day1_deposits,
       x.day2_deposits,
       x.day3_deposits,
       x.day7_deposits,
       x.day14_deposits,
       x.day30_deposits,
       x.day60_deposits,
       x.day90_deposits,
       x.day120_deposits,
       x.day150_deposits,
       x.day180_deposits,
       x.day210_deposits,
       x.day240_deposits,
       x.day270_deposits,
       x.day300_deposits,
       x.day330_deposits,
       x.day360_deposits,
       x.day540_deposits,
       x.day735_deposits,
       x.first_withdrawal_timestamp,
       x.last_withdrawal_timestamp,
       x.todate_withdrawals,
       x.todate_withdrawal_count,
       x.withdrawer,
       x.day1_withdrawals,
       x.day2_withdrawals,
       x.day3_withdrawals,
       x.day7_withdrawals,
       x.day14_withdrawals,
       x.day30_withdrawals,
       x.day60_withdrawals,
       x.day90_withdrawals,
       x.day120_withdrawals,
       x.day150_withdrawals,
       x.day180_withdrawals,
       x.day210_withdrawals,
       x.day240_withdrawals,
       x.day270_withdrawals,
       x.day300_withdrawals,
       x.day330_withdrawals,
       x.day360_withdrawals,
       x.day540_withdrawals,
       x.day735_withdrawals,
       MAX(CASE WHEN datediff ('day',install_day,e.event_day) = 1 THEN 1 ELSE 0 END) AS day1_retained,
       MAX(CASE WHEN datediff ('day',install_day,e.event_day) = 2 THEN 1 ELSE 0 END) AS day2_retained,
       MAX(CASE WHEN datediff ('day',install_day,e.event_day) = 3 THEN 1 ELSE 0 END) AS day3_retained,
       MAX(CASE WHEN datediff ('day',install_day,e.event_day) = 7 THEN 1 ELSE 0 END) AS day7_retained,
       MAX(CASE WHEN datediff ('day',install_day,e.event_day) = 14 THEN 1 ELSE 0 END) AS day14_retained,
       MAX(CASE WHEN datediff ('day',install_day,e.event_day) = 30 THEN 1 ELSE 0 END) AS day30_retained,
       MAX(CASE WHEN datediff ('day',install_day,e.event_day) = 60 THEN 1 ELSE 0 END) AS day60_retained,
       MAX(CASE WHEN datediff ('day',install_day,e.event_day) = 90 THEN 1 ELSE 0 END) AS day90_retained,
       MAX(CASE WHEN datediff ('day',install_day,e.event_day) = 180 THEN 1 ELSE 0 END) AS day180_retained,
       MAX(CASE WHEN datediff ('day',install_day,e.event_day) = 360 THEN 1 ELSE 0 END) AS day360_retained,
       MAX(CASE WHEN datediff ('day',install_day,e.event_day) = 540 THEN 1 ELSE 0 END) AS day540_retained,
       MAX(CASE WHEN datediff ('day',install_day,e.event_day) = 735 THEN 1 ELSE 0 END) AS day735_retained,
       x.kochava_install_timestamp,
       x.kochava_install_day,
       x.gsn_install_timestamp,
       x.gsn_install_day,
       --- By platform
       x.app_day1_bookings,
        x.app_day2_bookings,
        x.app_day3_bookings,
        x.app_day7_bookings,
        x.app_day14_bookings,
        x.app_day30_bookings,
        x.app_day60_bookings,
        x.app_day90_bookings,
        x.app_day180_bookings,
        x.app_day360_bookings,
        x.app_day540_bookings,
        x.app_day735_bookings,
        x.moweb_day1_bookings,
        x.moweb_day2_bookings,
        x.moweb_day3_bookings,
        x.moweb_day7_bookings,
        x.moweb_day14_bookings,
        x.moweb_day30_bookings,
        x.moweb_day60_bookings,
        x.moweb_day90_bookings,
        x.moweb_day180_bookings,
        x.moweb_day360_bookings,
        x.moweb_day540_bookings,
        x.moweb_day735_bookings,
        x.desktop_day1_bookings,
        x.desktop_day2_bookings,
        x.desktop_day3_bookings,
        x.desktop_day7_bookings,
        x.desktop_day14_bookings,
        x.desktop_day30_bookings,
        x.desktop_day60_bookings,
        x.desktop_day90_bookings,
        x.desktop_day180_bookings,
        x.desktop_day360_bookings,
        x.desktop_day540_bookings,
        x.desktop_day735_bookings,
        x.app_day1_payer,
        x.app_day2_payer,
        x.app_day3_payer,
        x.app_day7_payer,
        x.app_day14_payer,
        x.app_day30_payer,
        x.app_day60_payer,
        x.app_day90_payer,
        x.app_day180_payer,
        x.app_day360_payer,
        x.app_day540_payer,
        x.app_day735_payer,
        x.moweb_day1_payer,
        x.moweb_day2_payer,
        x.moweb_day3_payer,
        x.moweb_day7_payer,
        x.moweb_day14_payer,
        x.moweb_day30_payer,
        x.moweb_day60_payer,
        x.moweb_day90_payer,
        x.moweb_day180_payer,
        x.moweb_day360_payer,
        x.moweb_day540_payer,
        x.moweb_day735_payer,
        x.desktop_day1_payer,
        x.desktop_day2_payer,
        x.desktop_day3_payer,
        x.desktop_day7_payer,
        x.desktop_day14_payer,
        x.desktop_day30_payer,
        x.desktop_day60_payer,
        x.desktop_day90_payer,
        x.desktop_day180_payer,
        x.desktop_day360_payer,
        x.desktop_day540_payer,
        x.desktop_day735_payer,
        x.app_day1_cef,
        x.app_day2_cef,
        x.app_day3_cef,
        x.app_day7_cef,
        x.app_day14_cef,
        x.app_day30_cef,
        x.app_day60_cef,
        x.app_day90_cef,
        x.app_day180_cef,
        x.app_day360_cef,
        x.app_day540_cef,
        x.app_day735_cef,
        x.moweb_day1_cef,
        x.moweb_day2_cef,
        x.moweb_day3_cef,
        x.moweb_day7_cef,
        x.moweb_day14_cef,
        x.moweb_day30_cef,
        x.moweb_day60_cef,
        x.moweb_day90_cef,
        x.moweb_day180_cef,
        x.moweb_day360_cef,
        x.moweb_day540_cef,
        x.moweb_day735_cef,
        x.desktop_day1_cef,
        x.desktop_day2_cef,
        x.desktop_day3_cef,
        x.desktop_day7_cef,
        x.desktop_day14_cef,
        x.desktop_day30_cef,
        x.desktop_day60_cef,
        x.desktop_day90_cef,
        x.desktop_day180_cef,
        x.desktop_day360_cef,
        x.desktop_day540_cef,
        x.desktop_day735_cef,
        x.app_day1_cash_entrant,
        x.app_day2_cash_entrant,
        x.app_day3_cash_entrant,
        x.app_day7_cash_entrant,
        x.app_day14_cash_entrant,
        x.app_day30_cash_entrant,
        x.app_day60_cash_entrant,
        x.app_day90_cash_entrant,
        x.app_day180_cash_entrant,
        x.app_day360_cash_entrant,
        x.app_day540_cash_entrant,
        x.app_day735_cash_entrant,
        x.moweb_day1_cash_entrant,
        x.moweb_day2_cash_entrant,
        x.moweb_day3_cash_entrant,
        x.moweb_day7_cash_entrant,
        x.moweb_day14_cash_entrant,
        x.moweb_day30_cash_entrant,
        x.moweb_day60_cash_entrant,
        x.moweb_day90_cash_entrant,
        x.moweb_day180_cash_entrant,
        x.moweb_day360_cash_entrant,
        x.moweb_day540_cash_entrant,
        x.moweb_day735_cash_entrant,
        x.desktop_day1_cash_entrant,
        x.desktop_day2_cash_entrant,
        x.desktop_day3_cash_entrant,
        x.desktop_day7_cash_entrant,
        x.desktop_day14_cash_entrant,
        x.desktop_day30_cash_entrant,
        x.desktop_day60_cash_entrant,
        x.desktop_day90_cash_entrant,
        x.desktop_day180_cash_entrant,
        x.desktop_day360_cash_entrant,
        x.desktop_day540_cash_entrant,
        x.desktop_day735_cash_entrant,
        x.request_campaign_id           AS adn_campaign_id,
        x.request_adgroup               AS adn_sub_campaign_id,
        x.network_name_defined,
        case when x.network_name_defined = 'UNATTRIBUTED' then r.advertiser_id else NULL end as advertiser_id_defined
FROM kds_phoenix_base_level_2 x
--for retention info
 LEFT OUTER JOIN phoenix.device_day e ON x.idfv = e.idfv
 LEFT OUTER JOIN phoenix_events_dim_user r ON x.idfv = r.idfv
GROUP BY
  1,   2,   3,   4,   5,   6,   7,   8,   9,  10, 12,  13,  14,  15, 18,  19,  20,  25,
 26,  27,  28,  29,  30,  31,  32,  33,  34,  35,  36,  37,  38,  39,  40,  41,  42,  43,  44,  45,  46,  47,  48,  49, 50,
 51,  52,  53,  54,  55,  56,  57,  58,  59,  60,  61,  62,  63,  64,  65,  66,  67,  68,  69,  70,  71,  72,  73,  74, 75,
 76,  77,  78,  79,  80,  81,  82,  83,  84,  85,  86,  87,  88,  89,  90,  91,  92,  93,  94,  95,  96,  97,  98,  99, 100,
101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125,
126, 139, 140, 141, 142,143 , 144 , 145 , 146 , 147 , 148 , 149 , 150 , 151 , 152 , 153 , 154 , 155 , 156 , 157 , 158 , 159 ,
160 , 161 , 162 , 163 , 164 , 165 , 166 , 167 , 168 , 169 , 170 , 171 , 172 , 173 , 174 , 175 , 176 , 177 , 178 , 179 , 180 ,
181 , 182 , 183 , 184 , 185 , 186 , 187 , 188 , 189 , 190 , 191 , 192 , 193 , 194 , 195 , 196 , 197 , 198 , 199 , 200 , 201 ,
202 , 203 , 204 , 205 , 206 , 207 , 208 , 209 , 210 , 211 , 212 , 213 , 214 , 215 , 216 , 217 , 218 , 219 , 220 , 221 , 222 ,
223 , 224 , 225 , 226 , 227 , 228 , 229 , 230 , 231 , 232 , 233 , 234 , 235 , 236 , 237 , 238 , 239 , 240 , 241 , 242 , 243 ,
244 , 245 , 246 , 247 , 248 , 249 , 250 , 251 , 252 , 253 , 254 , 255 , 256 , 257 , 258 , 259 , 260 , 261 , 262 , 263 , 264 ,
265 , 266 , 267 , 268 , 269 , 270 , 271 , 272 , 273 , 274 , 275 , 276 , 277 , 278 , 279 , 280 , 281 , 282 , 283 , 284 , 285 ,
286 , 287 , 288, 289, 290;

commit;

alter table gsnmobile.kochava_device_summary_phoenix,gsnmobile.kochava_device_summary_phoenix_ods rename to kochava_device_summary_phoenix_old,kochava_device_summary_phoenix;
alter table gsnmobile.kochava_device_summary_phoenix_old rename to kochava_device_summary_phoenix_ods;

SELECT analyze_statistics('gsnmobile.kochava_device_summary_phoenix');