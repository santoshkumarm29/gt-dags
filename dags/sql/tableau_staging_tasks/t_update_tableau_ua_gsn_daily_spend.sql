TRUNCATE TABLE
    gsnmobile.tableau_ua_gsn_daily_spend;
INSERT
    /*+ direct,LABEL ('airflow-ua-ua_master_main_dag.tableau_subdag-t_update_tableau_ua_gsn_daily_spend')*/
INTO gsnmobile.tableau_ua_gsn_daily_spend

SELECT
    REGEXP_REPLACE(UPPER(
        CASE
            WHEN(spend.app ilike '%phoenix%')
            THEN('WORLDWINNER')
            WHEN(spend.app ilike '%world%winner%')
            THEN('WORLDWINNER')
            ELSE(spend.app)
        END),'[^A-Z0-9]','') ||'||'||
    CASE
        WHEN((spend.ad_partner ilike '%amazon%media%')
            OR  (spend.ad_partner ilike '%amazon%wake%screen%'))
        THEN('AMAZONMEDIAGROUP')
        WHEN(spend.ad_partner ilike '%applovin%')
        THEN('APPLOVIN')
        WHEN(spend.ad_partner ilike '%bing%')
        THEN('BINGSEARCH')
        WHEN(spend.ad_partner ilike '%aura%ironsource%')
        THEN('IRONSOURCEPRELOAD')
        WHEN(spend.ad_partner ilike '%super%sonic%')
        THEN('IRONSOURCE')
        WHEN(spend.ad_partner ilike '%iron%source%')
        THEN('IRONSOURCE')
        WHEN((spend.ad_partner ilike '%pch%')
            OR  (spend.ad_partner ilike '%liquid%'))
        THEN('LIQUIDWIRELESS')
        WHEN(spend.ad_partner ilike '%get%it%google%adwords%')
        THEN('GETIT')
        WHEN(spend.ad_partner ilike '%tresensa%')
        THEN('TRESENSA')
        WHEN(spend.ad_partner ilike '%digital%turbine%')
        THEN('DIGITALTURBINE')
        WHEN(spend.ad_partner ilike '%smartly%')
        THEN('SMARTLY')
        WHEN(spend.ad_partner ilike '%Bidalgo%')
        THEN('BIDALGO')
        WHEN(spend.ad_partner ilike '%moloco%')
        THEN('MOLOCO')

        WHEN(spend.ad_partner ilike '%google%')
        THEN('GOOGLE')
        WHEN(spend.ad_partner ilike '%admob%')
        THEN('GOOGLE')
        WHEN(spend.ad_partner ilike '%adwords%')
        THEN('GOOGLE')
        WHEN(spend.ad_partner ilike '%dauup%facebook%')
        THEN('DAUUPFACEBOOK')
        WHEN(spend.ad_partner ilike '%dauup%android%')
        THEN('DAUUPNETWORK')
        WHEN(spend.ad_partner ilike '%motive%')
        THEN('MOTIVEINTERACTIVE')
        WHEN(spend.ad_partner ilike '%aarki%')
        THEN('AARKI')
        WHEN(spend.ad_partner ilike '%unity%')
        THEN('UNITYADS')
        WHEN((spend.ad_partner ilike '%chartboost%')
            AND (spend.type ilike '%direct%deal%'))
        THEN('CHARTBOOSTDIRECTDEAL') 
        WHEN((spend.ad_partner ilike '%facebook%fbcoupon%')
            AND (DATE(spend.thedate) <= '2018-12-30'))
        THEN('FACEBOOKCOUPON') 
        WHEN(spend.ad_partner ilike '%creativetest%')
        THEN('CREATIVETESTFB')
        WHEN(spend.ad_partner ilike '%facebook%svl%')
        THEN('STEALTHVENTURELABSFACEBOOK')
        WHEN((spend.ad_partner ilike '%consumer%acquisition%instagram%')
            AND (DATE(spend.thedate)>='2015-12-10'))
        THEN('CONSUMERACQUISITIONINSTAGRAM')
        WHEN((spend.ad_partner ilike '%consumer%acquisition%instagram%')
            AND (DATE(spend.thedate)<'2015-12-10'))
        THEN('INSTAGRAM')
        WHEN(spend.ad_partner ilike '%sprinklr%instagram%')
        THEN('SPRINKLRINSTAGRAM')
        WHEN(spend.ad_partner ilike '%instagram%sprinklr%')
        THEN('SPRINKLRINSTAGRAM')
        WHEN(spend.ad_partner ilike '%sprinklr%facebook%')
        THEN('SPRINKLRFACEBOOK')
        WHEN(spend.ad_partner ilike '%facebook%sprinklr%')
        THEN('SPRINKLRFACEBOOK')
        WHEN(spend.ad_partner ilike '%sprinklr%yahoo%')
        THEN('SPRINKLRYAHOO')
        WHEN(spend.ad_partner ilike '%yahoo%sprinklr%')
        THEN('SPRINKLRYAHOO')
        WHEN(spend.ad_partner ilike '%consumer%acquisition%facebook%')
        THEN('CONSUMERACQUISITIONFACEBOOK')
        WHEN(spend.ad_partner ilike '%sprinklr%')
        THEN('SPRINKLRFACEBOOK')
        ELSE UPPER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(spend.ad_partner),
            '- ANDROID|- IOS|- FACEBOOK|- AMAZON|- ANROID|– ANDROID|– IOS|– FACEBOOK|– AMAZON|– ANROID|-ANDROID|-IOS|-FACEBOOK|-AMAZON|-ANROID|–ANDROID|–IOS|–FACEBOOK|–AMAZON|–ANROID'
            ,''),'[^A-Z0-9]','')))
    END ||'||'||
    CASE
        WHEN (((spend.ad_partner ilike '%google%') OR (spend.ad_partner ilike '%adwords%') OR (spend.ad_partner ilike '%admob%')) 
              AND ((spend.device ilike '%IPAD%') OR (spend.device ilike '%IOS%')))
        THEN 'IPHONE'
        WHEN (spend.device ilike '%ANDROID%')
        THEN 'ANDROID'
        WHEN (spend.device ilike '%IPHONE%')
        THEN 'IPHONE'
        WHEN (spend.device ilike '%IPAD%')
        THEN 'IPAD'
        WHEN ((spend.device ilike '%AMAZON%')
            OR  (spend.device ilike '%KINDLE%'))
        THEN 'AMAZON'
        WHEN (spend.device ilike '%IPOD%')
        THEN 'IPHONE'
        WHEN (spend.device ilike '%IOS%')
        THEN 'IPHONE'
        ELSE ('UNKNOWN')
    END ||'||'|| CAST(CAST(EXTRACT(EPOCH FROM DATE_TRUNC('day',spend.thedate)) AS INTEGER) AS
    VARCHAR) AS join_field_spend_to_kochava,
    REGEXP_REPLACE(UPPER(
        CASE
            WHEN(spend.app ilike '%phoenix%')
            THEN('WORLDWINNER')
            WHEN(spend.app ilike '%world%winner%')
            THEN('WORLDWINNER')
            ELSE(spend.app)
        END),'[^A-Z0-9]','') AS spend_app_clean,
    CASE
        WHEN((spend.ad_partner ilike '%amazon%media%')
            OR  (spend.ad_partner ilike '%amazon%wake%screen%'))
        THEN('AMAZONMEDIAGROUP')
        WHEN(spend.ad_partner ilike '%applovin%')
        THEN('APPLOVIN')
        WHEN(spend.ad_partner ilike '%bing%')
        THEN('BINGSEARCH')
        WHEN(spend.ad_partner ilike '%aura%ironsource%')
        THEN('IRONSOURCEPRELOAD')
        WHEN(spend.ad_partner ilike '%super%sonic%')
        THEN('IRONSOURCE')
        WHEN(spend.ad_partner ilike '%iron%source%')
        THEN('IRONSOURCE')
        WHEN((spend.ad_partner ilike '%pch%')
            OR  (spend.ad_partner ilike '%liquid%'))
        THEN('LIQUIDWIRELESS')
        WHEN(spend.ad_partner ilike '%get%it%google%adwords%')
        THEN('GETIT')
        WHEN(spend.ad_partner ilike '%tresensa%')
        THEN('TRESENSA')
        WHEN(spend.ad_partner ilike '%digital%turbine%')
        THEN('DIGITALTURBINE')
        WHEN(spend.ad_partner ilike '%smartly%')
        THEN('SMARTLY')
        WHEN(spend.ad_partner ilike '%Bidalgo%')
        THEN('BIDALGO')
        WHEN(spend.ad_partner ilike '%moloco%')
        THEN('MOLOCO')
        WHEN(spend.ad_partner ilike '%google%')
        THEN('GOOGLE')
        WHEN(spend.ad_partner ilike '%admob%')
        THEN('GOOGLE')
        WHEN(spend.ad_partner ilike '%adwords%')
        THEN('GOOGLE')
        WHEN(spend.ad_partner ilike '%dauup%facebook%')
        THEN('DAUUPFACEBOOK')
        WHEN(spend.ad_partner ilike '%dauup%android%')
        THEN('DAUUPNETWORK')
        WHEN(spend.ad_partner ilike '%motive%')
        THEN('MOTIVEINTERACTIVE')
        WHEN(spend.ad_partner ilike '%aarki%')
        THEN('AARKI')
        WHEN(spend.ad_partner ilike '%unity%')
        THEN('UNITYADS')
        WHEN((spend.ad_partner ilike '%chartboost%')
            AND (spend.type ilike '%direct%deal%'))
        THEN('CHARTBOOSTDIRECTDEAL')
        WHEN((spend.ad_partner ilike '%facebook%fbcoupon%')
                 AND (DATE(spend.thedate) <= '2018-12-30'))
        THEN('FACEBOOKCOUPON') 
        WHEN(spend.ad_partner ilike '%creativetest%')
        THEN('CREATIVETESTFB')
        WHEN(spend.ad_partner ilike '%facebook%svl%')
        THEN('STEALTHVENTURELABSFACEBOOK')
        WHEN((spend.ad_partner ilike '%consumer%acquisition%instagram%')
            AND (DATE(spend.thedate)>='2015-12-10'))
        THEN('CONSUMERACQUISITIONINSTAGRAM')
        WHEN((spend.ad_partner ilike '%consumer%acquisition%instagram%')
            AND (DATE(spend.thedate)<'2015-12-10'))
        THEN('INSTAGRAM')
        WHEN(spend.ad_partner ilike '%sprinklr%instagram%')
        THEN('SPRINKLRINSTAGRAM')
        WHEN(spend.ad_partner ilike '%instagram%sprinklr%')
        THEN('SPRINKLRINSTAGRAM')
        WHEN(spend.ad_partner ilike '%sprinklr%facebook%')
        THEN('SPRINKLRFACEBOOK')
        WHEN(spend.ad_partner ilike '%facebook%sprinklr%')
        THEN('SPRINKLRFACEBOOK')
        WHEN(spend.ad_partner ilike '%sprinklr%yahoo%')
        THEN('SPRINKLRYAHOO')
        WHEN(spend.ad_partner ilike '%yahoo%sprinklr%')
        THEN('SPRINKLRYAHOO')
        WHEN(spend.ad_partner ilike '%consumer%acquisition%facebook%')
        THEN('CONSUMERACQUISITIONFACEBOOK')
        ELSE UPPER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(spend.ad_partner),
            '- ANDROID|- IOS|- FACEBOOK|- AMAZON|- ANROID|– ANDROID|– IOS|– FACEBOOK|– AMAZON|– ANROID|-ANDROID|-IOS|-FACEBOOK|-AMAZON|-ANROID|–ANDROID|–IOS|–FACEBOOK|–AMAZON|–ANROID'
            ,''),'[^A-Z0-9]','')))
    END AS spend_ad_partner_clean,
    CASE
        WHEN (((spend.ad_partner ilike '%google%') OR (spend.ad_partner ilike '%adwords%') OR (spend.ad_partner ilike '%admob%')) 
              AND ((spend.device ilike '%IPAD%') OR (spend.device ilike '%IOS%')))
        THEN 'IPHONE'
        WHEN (spend.device ilike '%ANDROID%')
        THEN 'ANDROID'
        WHEN (spend.device ilike '%IPHONE%')
        THEN 'IPHONE'
        WHEN (spend.device ilike '%IPAD%')
        THEN 'IPAD'
        WHEN ((spend.device ilike '%AMAZON%')
            OR  (spend.device ilike '%KINDLE%'))
        THEN 'AMAZON'
        WHEN (spend.device ilike '%IPOD%')
        THEN 'IPHONE'
        WHEN (spend.device ilike '%IOS%')
        THEN 'IPHONE'
        ELSE ('UNKNOWN')
    END                                                                       AS spend_device_clean,
    CAST(CAST(EXTRACT(EPOCH FROM DATE_TRUNC('day',spend.thedate)) AS INTEGER) AS VARCHAR) AS
                                             spend_date_epoch_clean,
    DATE(DATE_TRUNC('day',spend.thedate)) AS spend_date,
    SUM(spend.cost)                       AS spend_cost,
    SUM(spend.impressions)                AS spend_impressions,
    SUM(spend.clicks)                     AS spend_clicks,
    SUM(spend.conversions)                AS spend_conversions
FROM
    gsnmobile.dim_daily_spend_v spend
WHERE
    spend.thedate >= '2014-01-01'
GROUP BY
    1,2,3,4,5,6;
COMMIT;
