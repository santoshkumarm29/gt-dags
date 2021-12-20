TRUNCATE TABLE
    gsnmobile.kochava_device_summary_ods;
TRUNCATE TABLE
    gsnmobile.kochava_device_summary_loading ;
--- load loading table with data that have changed over x number of days
INSERT
    /*+ direct , LABEL ('airflow-all-ua_master_main_dag.ua_attribution_subdag-t_update_kochava_device_summary')*/
INTO
    gsnmobile.kochava_device_summary_loading
-- 1 , get deduped ad_partner_install records from last run , inserted_on will have data that
-- actually changed
SELECT
    x.synthetic_id,
    CASE
        WHEN x.app_name ilike '%TRIPEAKS%'
        THEN 'TRIPEAKS SOLITAIRE'
        WHEN x.app_name ilike '%WOF SLOTS%'
        THEN 'WOF APP'
        ELSE UPPER(x.app_name)
    END AS app_name
FROM
    gsnmobile.ad_partner_installs x
WHERE
    COALESCE(x.synthetic_id,'') <> ''
AND TRUNC(greatest(x.inserted_on, x.updated_at)) >= date('{{ ds }}')-1 ------ INCREMENTAL INSERT
AND network_name NOT ilike 'unattributed'
AND x.app_sid_claim_winner = 1
UNION
---- 2 dim_app_installs get all records changed first_seen is the column that will hold data for
-- new installs
SELECT
    i.synthetic_id,
    UPPER(case when app = 'GSN Casino 2' then 'GSN Casino' else app end) AS app_name
FROM
    gsnmobile.dim_app_installs i
WHERE
    TRUNC(i.first_seen)> date('{{ ds }}')-1
UNION
--- 3  gsnmobile.events_dau event_day is the column that will hold data for any events that occur
-- we go back 3 days because the vaggs has condition to look back 3 days and we can assume 3 days
-- of data could change
SELECT
    synthetic_id ,
    UPPER(case when app = 'GSN Casino 2' then 'GSN Casino' else app end) AS app_name
FROM
    gsnmobile.events_dau
WHERE
    TRUNC(event_day) >= date('{{ ds }}')-3 --- WHY 3 days because event_day  can go back upto three days for
-- its insert
UNION
-- 4  gsnmobile.events_payments event_day again vaggs goes back upto 7 days
SELECT
    synthetic_id ,
    UPPER(case when app = 'GSN Casino 2' then 'GSN Casino' else app end) AS app_name
FROM
    gsnmobile.events_payments
WHERE
    TRUNC(event_day) >= date('{{ ds }}')-7 --- WHY 7 days because event_day  can go back upto seven days for
    -- its insert
    ;

SELECT analyze_statistics('gsnmobile.kochava_device_summary_loading');

--- use only the data from loading table to join existing gsnmobile.kochava_device_summary_ods
-- insert


INSERT  /*+direct,LABEL ('airflow-all-ua_master_main_dag.ua_attribution_subdag-t_update_kochava_device_summary-3')*/
INTO
gsnmobile.kochava_device_summary_ods
(       synthetic_id,
        platform,
        install_timestamp,
        install_day,
        app_name,
        campaign_name,
        campaign_id,
        site_id,
        network_name,
        creative,
        adset_name,
        country_code,
        request_campaign_id,
        request_adgroup,
        request_keyword,
        request_matchtype,
        model,
        device,
        most_recent_os_version,
        first_payment_timestamp,
        last_payment_timestamp,
        first_active_day,
        last_active_day,
        first_ww_app_login,
        first_ww_reg,
        todate_bookings,
        payer,
        day1_bookings,
        day2_bookings,
        day3_bookings,
        day7_bookings,
        day14_bookings,
        day30_bookings,
        day60_bookings,
        day90_bookings,
        day180_bookings,
        day360_bookings,
        day540_bookings,
        day1_payer,
        day2_payer,
        day3_payer,
        day7_payer,
        day14_payer,
        day30_payer,
        day60_payer,
        day90_payer,
        day180_payer,
        day360_payer,
        day540_payer,
        day1_retained,
        day2_retained,
        day3_retained,
        day7_retained,
        day14_retained,
        day30_retained,
        day60_retained,
        day90_retained,
        day180_retained,
        day360_retained,
        day540_retained,
        app,
        last_open,
        day1_open,
        day2_open,
        day3_open,
        day7_open,
        day14_open,
        day30_open,
        day60_open,
        day90_open,
        day1_cumu_bookings,
        day2_cumu_bookings,
        day3_cumu_bookings,
        day7_cumu_bookings,
        day14_cumu_bookings,
        day30_cumu_bookings,
        day60_cumu_bookings,
        day90_cumu_bookings,
        total_bookings,
        day120_cumu_bookings,
        day150_cumu_bookings,
        day180_cumu_bookings,
        day210_cumu_bookings,
        day240_cumu_bookings,
        day270_cumu_bookings,
        day300_cumu_bookings,
        day330_cumu_bookings,
        day360_cumu_bookings,
        day540_cumu_bookings,
        day735_cumu_bookings,
        os_version,
        day180_open,
        day360_open,
        day540_open,
        install_date,
        kochava_install_timestamp,
        kochava_install_day,
        gsn_install_timestamp,
        gsn_install_day,
        request_bundle_id
    )
SELECT
    x.synthetic_id,
    NVL(NVL(h.platform, x.platform),
    CASE
        WHEN e.os = 'NULL'
        THEN NULL
        ELSE e.os
    END )          AS platform,
    x.install_date AS install_timestamp,
    x.install_day  AS install_day,
    CASE
        WHEN x.app_name = 'WOF APP'
        THEN 'WOF SLOTS'
        WHEN x.app_name = 'TRIPEAKS SOLITAIRE'
        THEN 'SOLITAIRE TRIPEAKS'
        ELSE x.app_name
    END AS app_name,
    x.campaign_name,
    x.campaign_id,
    x.site_id,
    x.network_name,
    x.creative,
    x.adset_name,
    --prefers Kochava country source when available, if not then client event data
    MAX(NVL (x.country_code,CAST(
        CASE
            WHEN e.country = 'NULL'
            THEN NULL
            ELSE e.country
        END AS VARCHAR(5)))) AS country_code,
    x.request_campaign_id,
    x.request_adgroup,
    x.request_keyword,
    x.request_matchtype,
    MAX(h.model)  AS model,
    MAX(h.device) AS device,
    MAX(
        CASE
            WHEN NVL(e.most_recent_os_version,'NULL')= 'NULL'
            THEN x.kochava_os_version
            ELSE e.most_recent_os_version
        END )most_recent_os_version ,
    x.first_payment_timestamp,
    x.last_payment_timestamp,
    MIN(e.event_day) AS first_active_day,
    MAX(e.event_day) AS last_active_day,
    NULL             AS first_ww_app_login,
    NULL             AS first_ww_reg,
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
    MAX(
        CASE
            WHEN DATEDIFF ('day',install_day,e.event_day) = 1
            THEN 1
            ELSE 0
        END) AS day1_retained,
    MAX(
        CASE
            WHEN DATEDIFF ('day',install_day,e.event_day) = 2
            THEN 1
            ELSE 0
        END) AS day2_retained,
    MAX(
        CASE
            WHEN DATEDIFF ('day',install_day,e.event_day) = 3
            THEN 1
            ELSE 0
        END) AS day3_retained,
    MAX(
        CASE
            WHEN DATEDIFF ('day',install_day,e.event_day) = 7
            THEN 1
            ELSE 0
        END) AS day7_retained,
    MAX(
        CASE
            WHEN DATEDIFF ('day',install_day,e.event_day) = 14
            THEN 1
            ELSE 0
        END) AS day14_retained,
    MAX(
        CASE
            WHEN DATEDIFF ('day',install_day,e.event_day) = 30
            THEN 1
            ELSE 0
        END) AS day30_retained,
    MAX(
        CASE
            WHEN DATEDIFF ('day',install_day,e.event_day) = 60
            THEN 1
            ELSE 0
        END) AS day60_retained,
    MAX(
        CASE
            WHEN DATEDIFF ('day',install_day,e.event_day) = 90
            THEN 1
            ELSE 0
        END) AS day90_retained,
    MAX(
        CASE
            WHEN DATEDIFF ('day',install_day,e.event_day) = 180
            THEN 1
            ELSE 0
        END) AS day180_retained,
    MAX(
        CASE
            WHEN DATEDIFF ('day',install_day,e.event_day) = 360
            THEN 1
            ELSE 0
        END) AS day360_retained,
    MAX(
        CASE
            WHEN DATEDIFF ('day',install_day,e.event_day) = 540
            THEN 1
            ELSE 0
        END) AS day540_retained,
    ---drop these fields after switch
    x.app_name       AS app,
    MAX(e.event_day) AS last_open,
    MAX(
        CASE
            WHEN DATEDIFF ('day',install_day,e.event_day) = 1
            THEN 1
            ELSE 0
        END) AS day1_open,
    MAX(
        CASE
            WHEN DATEDIFF ('day',install_day,e.event_day) = 2
            THEN 1
            ELSE 0
        END) AS day2_open,
    MAX(
        CASE
            WHEN DATEDIFF ('day',install_day,e.event_day) = 3
            THEN 1
            ELSE 0
        END) AS day3_open,
    MAX(
        CASE
            WHEN DATEDIFF ('day',install_day,e.event_day) = 7
            THEN 1
            ELSE 0
        END) AS day7_open,
    MAX(
        CASE
            WHEN DATEDIFF ('day',install_day,e.event_day) = 14
            THEN 1
            ELSE 0
        END) AS day14_open,
    MAX(
        CASE
            WHEN DATEDIFF ('day',install_day,e.event_day) = 30
            THEN 1
            ELSE 0
        END) AS day30_open,
    MAX(
        CASE
            WHEN DATEDIFF ('day',install_day,e.event_day) = 60
            THEN 1
            ELSE 0
        END) AS day60_open,
    MAX(
        CASE
            WHEN DATEDIFF ('day',install_day,e.event_day) = 90
            THEN 1
            ELSE 0
        END)                      AS day90_open,
    x.day1_bookings               AS day1_cumu_bookings,
    x.day2_bookings               AS day2_cumu_bookings,
    x.day3_bookings               AS day3_cumu_bookings,
    x.day7_bookings               AS day7_cumu_bookings,
    x.day14_bookings              AS day14_cumu_bookings,
    x.day30_bookings              AS day30_cumu_bookings,
    x.day60_bookings              AS day60_cumu_bookings,
    x.day90_bookings              AS day90_cumu_bookings,
    x.todate_bookings             AS total_bookings,
    x.day120_bookings             AS day120_cumu_bookings,
    x.day150_bookings             AS day150_cumu_bookings,
    x.day180_bookings             AS day180_cumu_bookings,
    x.day210_bookings             AS day210_cumu_bookings,
    x.day240_bookings             AS day240_cumu_bookings,
    x.day270_bookings             AS day270_cumu_bookings,
    x.day300_bookings             AS day300_cumu_bookings,
    x.day330_bookings             AS day330_cumu_bookings,
    x.day360_bookings             AS day360_cumu_bookings,
    x.day540_bookings             AS day540_cumu_bookings,
    x.day735_bookings             AS day735_cumu_bookings,
    MAX(
        CASE
            WHEN NVL(e.most_recent_os_version,'NULL')= 'NULL'
            THEN x.kochava_os_version
            ELSE e.most_recent_os_version
        END )  AS os_version,
    MAX(
        CASE
            WHEN DATEDIFF ('day',install_day,e.event_day) = 180
            THEN 1
            ELSE 0
        END) AS day180_open,
    MAX(
        CASE
            WHEN DATEDIFF ('day',install_day,e.event_day) = 360
            THEN 1
            ELSE 0
        END) AS day360_open,
    MAX(
        CASE
            WHEN DATEDIFF ('day',install_day,e.event_day) = 540
            THEN 1
            ELSE 0
        END)      AS day540_open,
    x.install_day AS install_date,
    x.kochava_install_timestamp,
    x.kochava_install_day,
    x.gsn_install_timestamp,
    x.gsn_install_day,
    x.request_bundle_id
FROM
    (
        SELECT
            x.synthetic_id,
            x.platform,
            x.install_date,
            x.install_day,
            x.gsn_install_timestamp,
            x.gsn_install_day,
            x.kochava_install_timestamp,
            x.kochava_install_day,
            x.app_name,
            x.campaign_name,
            x.campaign_id,
            x.site_id,
            x.network_name,
            x.creative,
            x.adset_name,
            x.country_code,
            x.request_campaign_id,
            x.request_adgroup,
            x.request_keyword,
            x.request_matchtype,
            x.kochava_os_version,
            x.request_bundle_id,
            MIN(b.event_time)            AS first_payment_timestamp,
            MAX(b.event_time)            AS last_payment_timestamp,
            SUM(NVL (amount_paid_usd,0)) AS todate_bookings,
            MAX(
                CASE
                    WHEN b.event_time IS NOT NULL
                    THEN 1
                    ELSE 0
                END) AS payer,
            SUM(
                CASE
                    WHEN DATEDIFF ('day',install_day,b.event_day) <= 1
                    THEN amount_paid_usd
                    ELSE 0
                END) AS day1_bookings,
            SUM(
                CASE
                    WHEN DATEDIFF ('day',install_day,b.event_day) <= 2
                    THEN amount_paid_usd
                    ELSE 0
                END) AS day2_bookings,
            SUM(
                CASE
                    WHEN DATEDIFF ('day',install_day,b.event_day) <= 3
                    THEN amount_paid_usd
                    ELSE 0
                END) AS day3_bookings,
            SUM(
                CASE
                    WHEN DATEDIFF ('day',install_day,b.event_day) <= 7
                    THEN amount_paid_usd
                    ELSE 0
                END) AS day7_bookings,
            SUM(
                CASE
                    WHEN DATEDIFF ('day',install_day,b.event_day) <= 14
                    THEN amount_paid_usd
                    ELSE 0
                END) AS day14_bookings,
            SUM(
                CASE
                    WHEN DATEDIFF ('day',install_day,b.event_day) <= 30
                    THEN amount_paid_usd
                    ELSE 0
                END) AS day30_bookings,
            SUM(
                CASE
                    WHEN DATEDIFF ('day',install_day,b.event_day) <= 60
                    THEN amount_paid_usd
                    ELSE 0
                END) AS day60_bookings,
            SUM(
                CASE
                    WHEN DATEDIFF ('day',install_day,b.event_day) <= 90
                    THEN amount_paid_usd
                    ELSE 0
                END) AS day90_bookings,
            SUM(
                CASE
                    WHEN DATEDIFF ('day',install_day,b.event_day) <= 120
                    THEN amount_paid_usd
                    ELSE 0
                END) AS day120_bookings,
            SUM(
                CASE
                    WHEN DATEDIFF ('day',install_day,b.event_day) <= 150
                    THEN amount_paid_usd
                    ELSE 0
                END) AS day150_bookings,
            SUM(
                CASE
                    WHEN DATEDIFF ('day',install_day,b.event_day) <= 180
                    THEN amount_paid_usd
                    ELSE 0
                END) AS day180_bookings,
            SUM(
                CASE
                    WHEN DATEDIFF ('day',install_day,b.event_day) <= 210
                    THEN amount_paid_usd
                    ELSE 0
                END) AS day210_bookings,
            SUM(
                CASE
                    WHEN DATEDIFF ('day',install_day,b.event_day) <= 240
                    THEN amount_paid_usd
                    ELSE 0
                END) AS day240_bookings,
            SUM(
                CASE
                    WHEN DATEDIFF ('day',install_day,b.event_day) <= 270
                    THEN amount_paid_usd
                    ELSE 0
                END) AS day270_bookings,
            SUM(
                CASE
                    WHEN DATEDIFF ('day',install_day,b.event_day) <= 300
                    THEN amount_paid_usd
                    ELSE 0
                END) AS day300_bookings,
            SUM(
                CASE
                    WHEN DATEDIFF ('day',install_day,b.event_day) <= 330
                    THEN amount_paid_usd
                    ELSE 0
                END) AS day330_bookings,
            SUM(
                CASE
                    WHEN DATEDIFF ('day',install_day,b.event_day) <= 360
                    THEN amount_paid_usd
                    ELSE 0
                END) AS day360_bookings,
            SUM(
                CASE
                    WHEN DATEDIFF ('day',install_day,b.event_day) <= 540
                    THEN amount_paid_usd
                    ELSE 0
                END) AS day540_bookings,
            SUM(
                CASE
                    WHEN DATEDIFF ('day',install_day,b.event_day) <= 735
                    THEN amount_paid_usd
                    ELSE 0
                END) AS day735_bookings,
            MAX(
                CASE
                    WHEN DATEDIFF ('day',install_day,b.event_day) <= 1
                    THEN 1
                    ELSE 0
                END) AS day1_payer,
            MAX(
                CASE
                    WHEN DATEDIFF ('day',install_day,b.event_day) <= 2
                    THEN 1
                    ELSE 0
                END) AS day2_payer,
            MAX(
                CASE
                    WHEN DATEDIFF ('day',install_day,b.event_day) <= 3
                    THEN 1
                    ELSE 0
                END) AS day3_payer,
            MAX(
                CASE
                    WHEN DATEDIFF ('day',install_day,b.event_day) <= 7
                    THEN 1
                    ELSE 0
                END) AS day7_payer,
            MAX(
                CASE
                    WHEN DATEDIFF ('day',install_day,b.event_day) <= 14
                    THEN 1
                    ELSE 0
                END) AS day14_payer,
            MAX(
                CASE
                    WHEN DATEDIFF ('day',install_day,b.event_day) <= 30
                    THEN 1
                    ELSE 0
                END) AS day30_payer,
            MAX(
                CASE
                    WHEN DATEDIFF ('day',install_day,b.event_day) <= 60
                    THEN 1
                    ELSE 0
                END) AS day60_payer,
            MAX(
                CASE
                    WHEN DATEDIFF ('day',install_day,b.event_day) <= 90
                    THEN 1
                    ELSE 0
                END) AS day90_payer,
            MAX(
                CASE
                    WHEN DATEDIFF ('day',install_day,b.event_day) <= 180
                    THEN 1
                    ELSE 0
                END) AS day180_payer,
            MAX(
                CASE
                    WHEN DATEDIFF ('day',install_day,b.event_day) <= 360
                    THEN 1
                    ELSE 0
                END) AS day360_payer,
            MAX(
                CASE
                    WHEN DATEDIFF ('day',install_day,b.event_day) <= 540
                    THEN 1
                    ELSE 0
                END) AS day540_payer
        FROM
            (
                SELECT
                    platform,
                    NVL(i.first_seen_ts, x.install_date)     AS install_date,
                    NVL(i.first_seen, DATE (x.install_date)) AS install_day,
                    i.first_seen_ts                          AS gsn_install_timestamp,
                    i.first_seen                             AS gsn_install_day,
                    x.install_date                           AS kochava_install_timestamp,
                    DATE (x.install_date)                    AS kochava_install_day,
                    l.app_name,
                    campaign_name,
                    campaign_id,
                    site_id,
                    network_name,
                    creative,
                    ad_squad_name as adset_name,
                    country_code,
                    request_campaign_id,
                    request_adgroup,
                    request_keyword,
                    request_matchtype,
                    x.synthetic_id,
                    regexp_replace(x.device_version,'.*-','') kochava_os_version,
                    x.request_bundle_id
                FROM
                    gsnmobile.ad_partner_installs x
                INNER JOIN
                    gsnmobile.kochava_device_summary_loading l
                ON
                    x.synthetic_id=l.synthetic_id
                AND CASE
                         WHEN x.app_name ilike '%TRIPEAKS%'
                               THEN 'TRIPEAKS SOLITAIRE'
                         WHEN x.app_name ilike '%WOF SLOTS%'
                               THEN 'WOF APP'
                         ELSE x.app_name
                         END = l.app_name
                LEFT OUTER JOIN
                    gsnmobile.dim_app_installs i
                ON l.app_name = UPPER(case when i.app = 'GSN Casino 2' then 'GSN Casino' else i.app end)
                AND l.synthetic_id = i.synthetic_id
                WHERE
                    network_name NOT ilike 'unattributed'
                    and app_sid_claim_winner = 1
                UNION ALL
                SELECT
                    NULL              AS platform,
                    first_seen_ts     AS install_date,
                    first_seen        AS install_day,
                    first_seen_ts     AS gsn_install_timestamp,
                    first_seen        AS gsn_install_day,
                    NULL              AS kochava_install_timestamp,
                    NULL              AS kochava_install_day,
                    l.app_name AS app_name,
                    NULL              AS campaign_name,
                    NULL              AS campaign_id,
                    NULL              AS site_id,
                    'unattributed'    AS network_name,
                    NULL              AS creative,
                    NULL              AS adset_name,
                    NULL              AS country_code,
                    NULL              AS request_campaign_id,
                    NULL              AS request_adgroup,
                    NULL              AS request_keyword,
                    NULL              AS request_matchtype,
                    i.synthetic_id,
                    NULL kochava_os_version,
                    NULL              AS request_bundle_id
                FROM
                    gsnmobile.dim_app_installs i
                INNER JOIN
                    gsnmobile.kochava_device_summary_loading l
                ON
                    i.synthetic_id=l.synthetic_id
                AND UPPER(case when i.app = 'GSN Casino 2' then 'GSN Casino' else i.app end) = l.app_name
                LEFT OUTER JOIN
                    gsnmobile.ad_partner_installs k
                ON CASE
                        WHEN k.app_name ilike '%TRIPEAKS%'
                        THEN 'TRIPEAKS SOLITAIRE'
                        WHEN k.app_name ilike '%WOF SLOTS%'
                        THEN 'WOF APP'
                        ELSE k.app_name
                    END = l.app_name
                AND i.synthetic_id = k.synthetic_id
                AND k.ad_partner = 'kochava'
                AND k.platform IN ('ios',
                                   'android',
                                   'amazon')
                AND COALESCE (k.synthetic_id,'') <> ''
                AND k.network_name NOT ilike 'unattributed'
                and k.app_sid_claim_winner = 1
                WHERE
                    k.synthetic_id IS NULL ) x
            --for bookings info
        LEFT OUTER JOIN
            (
                SELECT
                    os AS platform,
                    event_time,
                    event_day,
                    CAST(amount_paid_usd AS FLOAT) AS amount_paid_usd,
                    synthetic_id,
                    UPPER(case when app = 'GSN Casino 2' then 'GSN Casino' else app end) AS app_name
                FROM
                    gsnmobile.events_payments) b
        ON x.app_name = b.app_name

        AND x.synthetic_id = b.synthetic_id
        -- 2019-05-06 adding in because for Casino we have different sources of data for purchase vs install timestamp
        -- and purchases can occur before installs when there's a great delay in client events
        AND (case when x.app_name = 'GSN CASINO' then x.install_date - 28 else x.install_date end) <= b.event_time
        WHERE
            ((
                    x.app_name = 'WOF APP'
                AND x.synthetic_id NOT IN
                    (
                        SELECT
                            hold_back_id
                        FROM
                            app_wofs.blacklist))
            OR  x.app_name IN ('GSN CASINO',
                               'GSN GRAND CASINO',
                               'TRIPEAKS SOLITAIRE'))
        GROUP BY
            1, -- platform
            2, -- install_date
            3, -- install_day
            4, -- gsn_install_timestamp
            5, -- gsn_install_day
            6, -- kochava_install_timestamp
            7, -- kochava_install_day
            8, -- app_name
            9, -- campaign_name
            10, -- campaign_id
            11, -- site_id
            12, -- network_name
            13, -- creative
            14, -- adset_name
            15, -- country_code
            16, -- request_campaign_id
            17, -- request_adgroup
            18, -- request_keyword
            19, -- request_matchtype
            20, -- synthetic_id
            21, -- kochava_os_version
            22 -- request_bundle_id
    ) x
    --for retention info
LEFT OUTER JOIN
    (
        SELECT
            UPPER(case when e.app = 'GSN Casino 2' then 'GSN Casino' else e.app end) app_name,
            e.synthetic_id,
            event_day,
            --keeping this as opposed to first_value from last iteration of code to keep query
            -- running quickly
            MAX(case when e.country = 'NULL' then null
            else e.country end) AS country,
            MAX(CASE WHEN os_version='NULL' THEN NULL ELSE os_version END)    most_recent_os_version,
            os
        FROM
            gsnmobile.events_dau e ,
            gsnmobile.kochava_device_summary_loading l
        WHERE
            e.synthetic_id=l.synthetic_id
        AND UPPER(case when e.app = 'GSN Casino 2' then 'GSN Casino' else e.app end) = l.app_name
        GROUP BY
            1, -- app_name
            2, -- synthetic_id
            3, -- country
            6  -- os
    ) e
ON
    e.synthetic_id = x.synthetic_id
AND e.app_name = x.app_name

LEFT OUTER JOIN
    (
        SELECT
            synthetic_id,
            model,
            platform,
            device
        FROM
            gsnmobile.dim_device_hardware) h
ON
    x.synthetic_id = h.synthetic_id
GROUP BY
    1, --synthetic_id
    2, --platform
    3, --install_timestamp
    4, --install_day
    5, --app_name
    6, --campaign_name
    7, --campaign_id
    8, --site_id
    9, --network_name
    10, --creative
    11, --adset_name
    13, --request_campaign_id
    14, --request_adgroup
    15, --request_keyword
    16, --request_matchtype
    20, --first_payment_timestamp
    21, --last_payment_timestamp
    26, --todate_bookings
    27, --payer
    28, --day1_bookings
    29, --day2_bookings
    30, --day3_bookings
    31, --day7_bookings
    32, --day14_bookings
    33, --day30_bookings
    34, --day60_bookings
    35, --day90_bookings
    36, --day180_bookings
    37, --day360_bookings
    38, --day540_bookings
    39, --day1_payer
    40, --day2_payer
    41, --day3_payer
    42, --day7_payer
    43, --day14_payer
    44, --day30_payer
    45, --day60_payer
    46, --day90_payer
    47, --day180_payer
    48, --day360_payer
    49, --day540_payer
    61, --app
    71, --day1_cumu_bookings
    72, --day2_cumu_bookings
    73, --day3_cumu_bookings
    74, --day7_cumu_bookings
    75, --day14_cumu_bookings
    76, --day30_cumu_bookings
    77, --day60_cumu_bookings
    78, --day90_cumu_bookings
    79, --total_bookings
    80, --day120_cumu_bookings
    81, --day150_cumu_bookings
    82, --day180_cumu_bookings
    83, --day210_cumu_bookings
    84, --day240_cumu_bookings
    85, --day270_cumu_bookings
    86, --day300_cumu_bookings
    87, --day330_cumu_bookings
    88, --day360_cumu_bookings
    89, --day540_cumu_bookings
    90, --day735_cumu_bookings
    95, --install_date
    96, --kochava_install_timestamp
    97, --kochava_install_day
    98, --gsn_install_timestamp
    99, --gsn_install_day
    100 --request_bundle_id
ORDER BY 1, 5, 2; --synthetic_id, app_name, platform



/* after joining on gsnmobile.ad_partner_Clicks at the level of app, synthetic_id and platform
   there are some dupes that need to be addressed that appear because of multiple platforms
   for the same app and synthetic id

   Action : only pick the dups , find the oldest install record,
            delete the dups and insert with the deduplicated set
 */
create local temporary table kds on commit preserve rows as
/*+ direct , LABEL ('airflow-all-ua_master_main_dag.ua_attribution_subdag-t_update_kochava_device_summary-5') */
select
synthetic_id,
 platform,
 install_timestamp,
 install_day,
 app_name,
 campaign_name,
 campaign_id,
 site_id,
 network_name,
 creative,
 adset_name,
 country_code,
 request_campaign_id,
 request_adgroup,
 request_keyword,
 request_matchtype,
 model,
 device,
 most_recent_os_version,
 first_payment_timestamp,
 last_payment_timestamp,
 first_active_day,
 last_active_day,
 first_ww_app_login,
 first_ww_reg,
 todate_bookings,
 payer,
 day1_bookings,
 day2_bookings,
 day3_bookings,
 day7_bookings,
 day14_bookings,
 day30_bookings,
 day60_bookings,
 day90_bookings,
 day180_bookings,
 day360_bookings,
 day540_bookings,
 day1_payer,
 day2_payer,
 day3_payer,
 day7_payer,
 day14_payer,
 day30_payer,
 day60_payer,
 day90_payer,
 day180_payer,
 day360_payer,
 day540_payer,
 day1_retained,
 day2_retained,
 day3_retained,
 day7_retained,
 day14_retained,
 day30_retained,
 day60_retained,
 day90_retained,
 day180_retained,
 day360_retained,
 day540_retained,
 app,
 last_open,
 day1_open,
 day2_open,
 day3_open,
 day7_open,
 day14_open,
 day30_open,
 day60_open,
 day90_open,
 day1_cumu_bookings,
 day2_cumu_bookings,
 day3_cumu_bookings,
 day7_cumu_bookings,
 day14_cumu_bookings,
 day30_cumu_bookings,
 day60_cumu_bookings,
 day90_cumu_bookings,
 total_bookings,
 day120_cumu_bookings,
 day150_cumu_bookings,
 day180_cumu_bookings,
 day210_cumu_bookings,
 day240_cumu_bookings,
 day270_cumu_bookings,
 day300_cumu_bookings,
 day330_cumu_bookings,
 day360_cumu_bookings,
 day540_cumu_bookings,
 day735_cumu_bookings,
 os_version,
 day180_open,
 day360_open,
 day540_open,
 install_date,
 kochava_install_timestamp,
 kochava_install_day,
 gsn_install_timestamp,
 gsn_install_day,
 request_cp_value1,
 request_cp_value2,
 request_cp_value3,
 request_cp_name1,
 request_cp_name2,
 request_cp_name3,
 last_click_timestamp ,
 last_click_network_name ,
 last_click_tracker,
 request_bundle_id
from (
select x.* , row_number() over ( partition by app_name, synthetic_id, install_day order by install_day ) row_number
 from (
select a.* from gsnmobile.kochava_device_summary_ods a
join
(   /* ensures that we only deal with existing dups to reduce the writes, since dups are very few */
select
   app_name,
   synthetic_id,
    min(install_day) min_install_day,
   count(1) as dupe_count
 from gsnmobile.kochava_device_summary_ods kds
 group by 1, 2
 having count(1) > 1
 order by 1, 2
) x
on ( a.app_name = x.app_name and a.synthetic_id = x.synthetic_id and a.install_day = x.min_install_day )
)x )x where row_number = 1 ;


/* delete all the dups */
delete /*+ direct, LABEL ('airflow-all-ua_master_main_dag.ua_attribution_subdag-t_update_kochava_device_summary-7') */
from gsnmobile.kochava_device_summary_ods
where
( app_name , synthetic_id) in (
select
   app_name,
   synthetic_id
 from gsnmobile.kochava_device_summary_ods kds
 group by 1, 2
 having count(1) > 1
);

/* insert with deduplicated set */
insert /*+ direct ,LABEL ('airflow-all-ua_master_main_dag.ua_attribution_subdag-t_update_kochava_device_summary-8') */
into gsnmobile.kochava_device_summary_ods
select * from kds;

commit;

SELECT analyze_statistics('gsnmobile.kochava_device_summary_ods');




MERGE /*+direct, LABEL ('airflow-all-ua_master_main_dag.ua_attribution_subdag-t_update_kochava_device_summary-9')*/
INTO

    gsnmobile.kochava_device_summary S -- change temp here
USING
    gsnmobile.kochava_device_summary_ods O
ON
    (
        s.SYNTHETIC_ID = O.SYNTHETIC_ID
    AND S.app_name = O.app_name
     )
WHEN MATCHED
    THEN
UPDATE
SET
    platform = O.platform,
    install_timestamp = O.install_timestamp,
    install_day = O.install_day,
    campaign_name = O.campaign_name,
    campaign_id = O.campaign_id,
    site_id = O.site_id,
    network_name = O.network_name,
    creative = O.creative,
    adset_name = O.adset_name,
    country_code = O.country_code,
    request_campaign_id = O.request_campaign_id,
    request_adgroup = O.request_adgroup,
    request_keyword = O.request_keyword,
    request_matchtype = O.request_matchtype,
    model = O.model,
    device = O.device,
    most_recent_os_version = O.most_recent_os_version,
    first_payment_timestamp = O.first_payment_timestamp,
    last_payment_timestamp = O.last_payment_timestamp,
    first_active_day = O.first_active_day,
    last_active_day = O.last_active_day,
    first_ww_app_login = O.first_ww_app_login,
    first_ww_reg = O.first_ww_reg,
    todate_bookings = O.todate_bookings,
    payer = O.payer,
    day1_bookings = O.day1_bookings,
    day2_bookings = O.day2_bookings,
    day3_bookings = O.day3_bookings,
    day7_bookings = O.day7_bookings,
    day14_bookings = O.day14_bookings,
    day30_bookings = O.day30_bookings,
    day60_bookings = O.day60_bookings,
    day90_bookings = O.day90_bookings,
    day180_bookings = O.day180_bookings,
    day360_bookings = O.day360_bookings,
    day540_bookings = O.day540_bookings,
    day1_payer = O.day1_payer,
    day2_payer = O.day2_payer,
    day3_payer = O.day3_payer,
    day7_payer = O.day7_payer,
    day14_payer = O.day14_payer,
    day30_payer = O.day30_payer,
    day60_payer = O.day60_payer,
    day90_payer = O.day90_payer,
    day180_payer = O.day180_payer,
    day360_payer = O.day360_payer,
    day540_payer = O.day540_payer,
    day1_retained = O.day1_retained,
    day2_retained = O.day2_retained,
    day3_retained = O.day3_retained,
    day7_retained = O.day7_retained,
    day14_retained = O.day14_retained,
    day30_retained = O.day30_retained,
    day60_retained = O.day60_retained,
    day90_retained = O.day90_retained,
    day180_retained = O.day180_retained,
    day360_retained = O.day360_retained,
    day540_retained = O.day540_retained,
    app = O.app,
    last_open = O.last_open,
    day1_open = O.day1_open,
    day2_open = O.day2_open,
    day3_open = O.day3_open,
    day7_open = O.day7_open,
    day14_open = O.day14_open,
    day30_open = O.day30_open,
    day60_open = O.day60_open,
    day90_open = O.day90_open,
    day1_cumu_bookings = O.day1_cumu_bookings,
    day2_cumu_bookings = O.day2_cumu_bookings,
    day3_cumu_bookings = O.day3_cumu_bookings,
    day7_cumu_bookings = O.day7_cumu_bookings,
    day14_cumu_bookings = O.day14_cumu_bookings,
    day30_cumu_bookings = O.day30_cumu_bookings,
    day60_cumu_bookings = O.day60_cumu_bookings,
    day90_cumu_bookings = O.day90_cumu_bookings,
    total_bookings = O.total_bookings,
    day120_cumu_bookings = O.day120_cumu_bookings,
    day150_cumu_bookings = O.day150_cumu_bookings,
    day180_cumu_bookings = O.day180_cumu_bookings,
    day210_cumu_bookings = O.day210_cumu_bookings,
    day240_cumu_bookings = O.day240_cumu_bookings,
    day270_cumu_bookings = O.day270_cumu_bookings,
    day300_cumu_bookings = O.day300_cumu_bookings,
    day330_cumu_bookings = O.day330_cumu_bookings,
    day360_cumu_bookings = O.day360_cumu_bookings,
    day540_cumu_bookings = O.day540_cumu_bookings,
    day735_cumu_bookings = O.day735_cumu_bookings,
    os_version = O.os_version,
    day180_open = O.day180_open,
    day360_open = O.day360_open,
    day540_open = O.day540_open,
    install_date = O.install_date,
    kochava_install_timestamp = O.kochava_install_timestamp,
    kochava_install_day = O.kochava_install_day,
    gsn_install_timestamp = O.gsn_install_timestamp,
    gsn_install_day = O.gsn_install_day,
    last_updated_at = SYSDATE,
    request_bundle_id = O.request_bundle_id
WHEN NOT MATCHED
    THEN
INSERT
    (
        synthetic_id ,
        platform ,
        install_timestamp ,
        install_day ,
        app_name ,
        campaign_name ,
        campaign_id ,
        site_id ,
        network_name ,
        creative ,
        adset_name ,
        country_code ,
        request_campaign_id ,
        request_adgroup ,
        request_keyword ,
        request_matchtype ,
        model ,
        device ,
        most_recent_os_version ,
        first_payment_timestamp ,
        last_payment_timestamp ,
        first_active_day ,
        last_active_day ,
        first_ww_app_login ,
        first_ww_reg ,
        todate_bookings ,
        payer ,
        day1_bookings ,
        day2_bookings ,
        day3_bookings ,
        day7_bookings ,
        day14_bookings ,
        day30_bookings ,
        day60_bookings ,
        day90_bookings ,
        day180_bookings ,
        day360_bookings ,
        day540_bookings ,
        day1_payer ,
        day2_payer ,
        day3_payer ,
        day7_payer ,
        day14_payer ,
        day30_payer ,
        day60_payer ,
        day90_payer ,
        day180_payer ,
        day360_payer ,
        day540_payer ,
        day1_retained ,
        day2_retained ,
        day3_retained ,
        day7_retained ,
        day14_retained ,
        day30_retained ,
        day60_retained ,
        day90_retained ,
        day180_retained ,
        day360_retained ,
        day540_retained ,
        app ,
        last_open ,
        day1_open ,
        day2_open ,
        day3_open ,
        day7_open ,
        day14_open ,
        day30_open ,
        day60_open ,
        day90_open ,
        day1_cumu_bookings ,
        day2_cumu_bookings ,
        day3_cumu_bookings ,
        day7_cumu_bookings ,
        day14_cumu_bookings ,
        day30_cumu_bookings ,
        day60_cumu_bookings ,
        day90_cumu_bookings ,
        total_bookings ,
        day120_cumu_bookings ,
        day150_cumu_bookings ,
        day180_cumu_bookings ,
        day210_cumu_bookings ,
        day240_cumu_bookings ,
        day270_cumu_bookings ,
        day300_cumu_bookings ,
        day330_cumu_bookings ,
        day360_cumu_bookings ,
        day540_cumu_bookings ,
        day735_cumu_bookings ,
        os_version ,
        day180_open ,
        day360_open ,
        day540_open ,
        install_date ,
        kochava_install_timestamp ,
        kochava_install_day ,
        gsn_install_timestamp ,
        gsn_install_day,
        last_updated_at,
        request_bundle_id
    )
    VALUES
    (
        O.synthetic_id,
        O.platform,
        O.install_timestamp,
        O.install_day,
        O.app_name,
        O.campaign_name,
        O.campaign_id,
        O.site_id,
        O.network_name,
        O.creative,
        O.adset_name,
        O.country_code,
        O.request_campaign_id,
        O.request_adgroup,
        O.request_keyword,
        O.request_matchtype,
        O.model,
        O.device,
        O.most_recent_os_version,
        O.first_payment_timestamp,
        O.last_payment_timestamp,
        O.first_active_day,
        O.last_active_day,
        O.first_ww_app_login,
        O.first_ww_reg,
        O.todate_bookings,
        O.payer,
        O.day1_bookings,
        O.day2_bookings,
        O.day3_bookings,
        O.day7_bookings,
        O.day14_bookings,
        O.day30_bookings,
        O.day60_bookings,
        O.day90_bookings,
        O.day180_bookings,
        O.day360_bookings,
        O.day540_bookings,
        O.day1_payer,
        O.day2_payer,
        O.day3_payer,
        O.day7_payer,
        O.day14_payer,
        O.day30_payer,
        O.day60_payer,
        O.day90_payer,
        O.day180_payer,
        O.day360_payer,
        O.day540_payer,
        O.day1_retained,
        O.day2_retained,
        O.day3_retained,
        O.day7_retained,
        O.day14_retained,
        O.day30_retained,
        O.day60_retained,
        O.day90_retained,
        O.day180_retained,
        O.day360_retained,
        O.day540_retained,
        O.app,
        O.last_open,
        O.day1_open,
        O.day2_open,
        O.day3_open,
        O.day7_open,
        O.day14_open,
        O.day30_open,
        O.day60_open,
        O.day90_open,
        O.day1_cumu_bookings,
        O.day2_cumu_bookings,
        O.day3_cumu_bookings,
        O.day7_cumu_bookings,
        O.day14_cumu_bookings,
        O.day30_cumu_bookings,
        O.day60_cumu_bookings,
        O.day90_cumu_bookings,
        O.total_bookings,
        O.day120_cumu_bookings,
        O.day150_cumu_bookings,
        O.day180_cumu_bookings,
        O.day210_cumu_bookings,
        O.day240_cumu_bookings,
        O.day270_cumu_bookings,
        O.day300_cumu_bookings,
        O.day330_cumu_bookings,
        O.day360_cumu_bookings,
        O.day540_cumu_bookings,
        O.day735_cumu_bookings,
        O.os_version,
        O.day180_open,
        O.day360_open,
        O.day540_open,
        O.install_date,
        O.kochava_install_timestamp,
        O.kochava_install_day,
        O.gsn_install_timestamp,
        O.gsn_install_day,
        SYSDATE,
        O.request_bundle_id
    ) ;

MERGE
    /*+ DIRECT, LABEL ('airflow-all-ua_master_main_dag.ua_attribution_subdag-t_update_kochava_device_summary-10')*/
INTO
    gsnmobile.kochava_device_summary AS summary
USING
    (
        SELECT
            *
        FROM
            (
                SELECT
                    S.synthetic_id,
                    S.app_name,
                    ROW_NUMBER () OVER (PARTITION BY S.synthetic_id,S.app_name ORDER BY
                    c.click_Date DESC ) rownum ,
                    c.click_Date        last_click_timestamp_new,
                    c.network_name      last_click_network_name_new,
                    c.tracker           last_click_tracker_new
                FROM
                    gsnmobile.kochava_device_summary S
                LEFT JOIN
                    gsnmobile.dim_device_mapping M
                ON
                    S.synthetic_id = M.synthetic_id
                INNER JOIN
                    gsnmobile.ad_partner_Clicks C
                ON
                    (
                        C.id = M.id
                    AND S.app_name = C.app_name
                    AND C.click_date < S.install_timestamp)
                WHERE
                    M.id_type IN ('idfa',
                                  'android_id',
                                  'googleAdId',
                                  'idfv')
                AND S.last_click_timestamp IS NULL
                AND S.install_Day > date('{{ ds }}')-3  -- REFRESH ONLY LAST THREE DAYS DATA
                ) A
        WHERE
            rownum=1 ) AS source
ON
    (
        summary.synthetic_id =source.synthetic_id
    AND summary.app_name = source.app_name)
WHEN MATCHED
    THEN
UPDATE
SET
    last_click_timestamp=source.last_click_timestamp_new,
    last_click_network_name=source.last_click_network_name_new,
    last_click_tracker = source.last_click_tracker_new ,
    last_updated_at = SYSDATE;


--/*
--create table gsnmobile.kochava_device_summary_history like gsnmobile.kochava_device_summary including projections;
--alter table gsnmobile.kochava_device_summary_history add column inserted_at timestamptz default (now() at time zone 'utc');
--*/
--INSERT
--/*+ DIRECT, LABEL ('airflow-all-ua_master_main_dag.ua_attribution_subdag-t_update_kochava_device_summary-10')*/
--INTO gsnmobile.kochava_device_summary_history
--SELECT *, (now() at time zone 'utc')
--FROM gsnmobile.kochava_device_summary
--WHERE last_updated_at = SYSDATE;


COMMIT;

-- SELECT PURGE_TABLE('gsnmobile.kochava_device_summary')    ;
-- SELECT
--     analyze_statistics('gsnmobile.kochava_device_summary'); -- change temp here
