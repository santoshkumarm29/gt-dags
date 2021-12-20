TRUNCATE TABLE ww.app_acquisition_ods;

CREATE LOCAL TEMP TABLE first_idfv
ON COMMIT PRESERVE ROWS DIRECT AS
        select /*+ DIRECT, LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}') */
        idfv, user_id, event_time from
        (select
              idfv
            , user_id
            , event_time
            , row_number() over (partition by idfv order by event_time asc) as rownum
        from
            (
            select
                  idfv
                , id as user_id
                , event_time
            from phoenix.dim_device_mapping
            where id_type = 'user_id'
            order by 1,3
            ) a
            ) x where rownum = 1;

CREATE LOCAL TEMP TABLE idfv_user_lookup
ON COMMIT PRESERVE ROWS DIRECT AS
        select /*+ DIRECT, LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}') */
            distinct
              user_id
            , idfv
        from
            (
            select distinct
                  e.id as user_id
                , e.idfv
                , e.event_time as first_seen
                , row_number() over (partition by e.id order by e.event_time) as device_rank
            from ww.dim_users u
            join phoenix.dim_device_mapping e
                on u.user_id = e.id
            where id_type = 'user_id'
            ) x
        where device_rank = 1
        UNSEGMENTED ALL NODES;


CREATE LOCAL TEMP TABLE app_registrations
ON COMMIT PRESERVE ROWS DIRECT AS
select /*+ DIRECT, LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}') */
      idfv,
      user_id,
      case when guest_play_account_convert_date is not null then guest_play_account_convert_date
           when createdate is not null and guest_play_account is null then createdate end as createdate, -- added 11/13; createdate field in there for clarity only; this field still needs to be labeled "createdate" for other parts of the report;
      real_money_date
      from
      (select
            distinct
              k.idfv
            , c.user_id
            , timestampadd(hour,-3,c.createdate) as createdate --- PST
            , timestampadd(hour,-3,c.real_money_date) as real_money_date ---PST
            , timestampadd(hour,-3,c.guest_play_account_convert_date) as guest_play_account_convert_date -- added 11/13 --- PST
            , c.guest_play_account -- added 11/13
        from gsnmobile.kochava_device_summary_phoenix k
        join
            (
            select
                  b.idfv
                , u.user_id
                , u.real_money_date
                , timestampadd(hour,3,b.event_time) as first_ww_app_login --- PST
                , u.createdate
                , u.guest_play_account_convert_date
                , u.guest_play_account
            from first_idfv b
            left join ww.dim_users u
                on b.user_id = u.user_id
            ) c
            on k.idfv = c.idfv
        where date(k.first_ww_app_login) <= date(k.first_ww_reg)
        and date(timestampadd(hour,3,k.first_ww_reg)) <= '2018-10-08'
        union
        select distinct
             '0' as idfv
            , a.user_id
            , timestampadd(hour,-3,u.createdate) as createdate --- PST
            , timestampadd(hour,-3,u.real_money_date) as real_money_date --- PST
            , timestampadd(hour,-3,u.guest_play_account_convert_date) as guest_play_account_convert_date -- added 11/13 --- PST
            , u.guest_play_account -- added 11/13
        from
           (
            select distinct
                  user_id
                , platform_id
                , date(activity_date) as day
                , row_number() over (partition by user_id order by activity_date) as login_rank
            from ww.login_activity --
            ) a
        join ww.dim_users u
            on a.user_id = u.user_id
            and a.day = date(u.createdate)
        where date(timestampadd(hour,-3,u.createdate)) >= '2018-10-09'
        and a.login_rank = 1
        and a.platform_id > 2 --IDs 1 and 2 are desktop and moweb, everything else is app
      order by (user_id) )x
      where guest_play_account = 0 or
      guest_play_account is null
      SEGMENTED BY hash(user_id)
ALL NODES KSAFE 1;



CREATE LOCAL TEMP TABLE app_installs
ON COMMIT PRESERVE ROWS DIRECT AS
      SELECT /*+ DIRECT, LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}') */
              k.idfv
            , date(k.install_timestamp) as install_date
            , case
                when k.campaign_name ilike '%test%' and
                (k.campaign_name ilike '%samsung%' or k.campaign_name ilike '%galaxy%') then k.network_name||' - Samsung TEST'
                else k.network_name
                end as network_name
            , case
                when k.network_name = 'Apple Search Ads' then k.adn_sub_campaign_id
                when k.campaign_name in ('Samsung_SamsungStore_Test','TestGalaxyStore') then split_part(k.adn_campaign_id,'-',4)
                else k.request_campaign_id
                end as ad_group_id --changed 9/17/19 to accomodate ASA and Samsung
            , CASE
                WHEN((k.network_name ilike '%smartlink%')
                    AND (campaign_id IN ('kosolitairetripeaks179552cdd213d1b1ec8dadb11e078f',
                                         'kosolitairetripeaks179552cdd213d1b1eff27d8be17f25',
                                         'kosolitaire-tripeaks-ios53a8762dc3a45f7f36992c7065',
                                         'kosolitaire-tripeaks-amazon542c346dd4e8d62409345f40e6',
                                         'kosolitairetripeaks179552cdd213d1b1e2ed740d2e8bfa',
                                         'kosolitairetripeaks179552cdd213d1b1e8c45df91957da',
                                         'kosolitaire-tripeaks-ios53a8762dc3a4559d6e0f025582',
                                         'kosolitaire-tripeaks-amazon542c346dd4e8d2134a9d909d97')))
                THEN('PLAYANEXTYAHOO')
                WHEN(k.network_name ilike '%amazon%fire%')
                THEN('AMAZONMEDIAGROUP')
                WHEN(k.network_name ilike '%wake%screen%')
                THEN('AMAZONMEDIAGROUP')
                WHEN((k.app_name ilike '%bingo%bash%')
                    AND (k.network_name ilike '%amazon%')
                    AND (k.campaign_name ilike '%wakescreen%'))
                THEN('AMAZONMEDIAGROUP')
                WHEN(k.network_name ilike '%mobilda%')
                THEN('SNAPCHAT')
                WHEN(k.network_name ilike '%snap%')
                THEN('SNAPCHAT')
                WHEN(k.network_name ilike '%applovin%')
                THEN('APPLOVIN')
                WHEN(k.network_name ilike '%bing%')
                THEN('BINGSEARCH')
                WHEN(k.network_name ilike '%supersonic%')
                THEN('IRONSOURCE')
                WHEN(k.network_name ilike '%iron%source%')
                THEN('IRONSOURCE')
                WHEN((k.network_name ilike '%pch%')
                    OR  (k.network_name ilike '%liquid%'))
                THEN('LIQUIDWIRELESS')
                WHEN(k.network_name ilike '%tresensa%')
                THEN('TRESENSA')
                WHEN(k.network_name ilike '%digital%turbine%')
                THEN('DIGITALTURBINE')
                WHEN(k.network_name ilike '%moloco%')
                THEN('MOLOCO')
                WHEN(k.network_name ilike '%adaction%')
                THEN('ADACTION')
                WHEN(k.network_name ilike '%google%')
                THEN('GOOGLE')
                WHEN(k.network_name ilike '%admob%')
                THEN('GOOGLE')
                WHEN(k.network_name ilike '%adwords%')
                THEN('GOOGLE')
                WHEN(k.network_name ilike '%dauup%android%')
                THEN('DAUUPNETWORK')
                WHEN(k.network_name ilike '%motive%')
                THEN('MOTIVEINTERACTIVE')
                WHEN(k.network_name ilike '%aarki%')
                THEN('AARKI')
                WHEN(k.network_name ilike '%unity%')
                THEN('UNITYADS')
                WHEN ((k.network_name ilike '%chartboost%')
                    AND (k.campaign_name ilike '%_dd_%'))
                THEN('CHARTBOOSTDIRECTDEAL')
                WHEN(((k.network_name ilike '%facebook%') or (k.network_name ilike '%instagram%'))
                AND (k.site_id ilike '%fbcoupon%')
                AND (DATE(k.install_day) <= '2018-12-30'))
                THEN('FACEBOOKCOUPON')
                WHEN(k.network_name ilike '%smartly%')
                THEN('SMARTLY')
                WHEN (k.network_name ilike '%sprinklr%instagram%')
                THEN('SPRINKLRINSTAGRAM')
                WHEN (k.network_name ilike '%instagram%sprinklr%')
                THEN('SPRINKLRINSTAGRAM')
                WHEN (k.network_name ilike '%sprinklr%facebook%')
                THEN('SPRINKLRFACEBOOK')
                WHEN (k.network_name ilike '%facebook%sprinklr%')
                THEN('SPRINKLRFACEBOOK')
                WHEN (k.network_name ilike '%sprinklr%yahoo%')
                THEN('SPRINKLRYAHOO')
                WHEN (k.network_name ilike '%yahoo%sprinklr%')
                THEN('SPRINKLRYAHOO')
                WHEN((k.network_name ilike '%facebook%')
                    AND (k.site_id ilike 'svl%'))
                THEN('STEALTHVENTURELABSFACEBOOK')
                WHEN ((k.network_name ilike '%facebook%')
                    AND (k.site_id ilike '[conacq]%'))
                THEN('CONSUMERACQUISITIONFACEBOOK')
                WHEN (k.network_name ilike '%sprinklr%')
                THEN('SPRINKLRFACEBOOK')
                WHEN ((k.network_name ilike '%instagram%')
                    AND (k.app_name ilike '%wof%'))
                THEN('FACEBOOK')
                WHEN ((k.network_name ilike '%instagram%')
                    AND (DATE(k.install_day)>='2015-12-10'))
                THEN('CONSUMERACQUISITIONINSTAGRAM')
                WHEN ((k.network_name ilike '%instagram%')
                    AND (DATE(k.install_day)<'2015-12-10'))
                THEN('INSTAGRAM')
                WHEN(k.network_name ilike '%dauup%facebook%')
                THEN('DAUUPFACEBOOK')
                WHEN(k.network_name ilike '%glispaandroid%')
                THEN('GLISPA')
                WHEN(k.network_name ilike '%apple%search%')
                THEN('APPLESEARCHADS')
                ELSE TRIM(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(k.network_name),
                    '- ANDROID|- IOS|- FACEBOOK|- AMAZON|- ANROID|– ANDROID|– IOS|– FACEBOOK|– AMAZON|– ANROID|-ANDROID|-IOS|-FACEBOOK|-AMAZON|-ANROID|–ANDROID|–IOS|–FACEBOOK|–AMAZON|–ANROID'
                    ,''),'[^A-Z0-9]',''))
                END AS kochava_network_name_clean
            ,  'WORLDWINNER'||'||'||
                CASE
                WHEN((k.network_name ilike '%smartlink%')
                    AND (campaign_id IN ('kosolitairetripeaks179552cdd213d1b1ec8dadb11e078f',
                                         'kosolitairetripeaks179552cdd213d1b1eff27d8be17f25',
                                         'kosolitaire-tripeaks-ios53a8762dc3a45f7f36992c7065',
                                         'kosolitaire-tripeaks-amazon542c346dd4e8d62409345f40e6',
                                         'kosolitairetripeaks179552cdd213d1b1e2ed740d2e8bfa',
                                         'kosolitairetripeaks179552cdd213d1b1e8c45df91957da',
                                         'kosolitaire-tripeaks-ios53a8762dc3a4559d6e0f025582',
                                         'kosolitaire-tripeaks-amazon542c346dd4e8d2134a9d909d97')))
                THEN('PLAYANEXTYAHOO')
                WHEN(k.network_name ilike '%amazon%fire%')
                THEN('AMAZONMEDIAGROUP')
                WHEN(k.network_name ilike '%wake%screen%')
                THEN('AMAZONMEDIAGROUP')
                WHEN((k.app_name ilike '%bingo%bash%')
                    AND (k.network_name ilike '%amazon%')
                    AND (k.campaign_name ilike '%wakescreen%'))
                THEN('AMAZONMEDIAGROUP')
                WHEN(k.network_name ilike '%mobilda%')
                THEN('SNAPCHAT')
                WHEN(k.network_name ilike '%snap%')
                THEN('SNAPCHAT')
                WHEN(k.network_name ilike '%applovin%')
                THEN('APPLOVIN')
                WHEN(k.network_name ilike '%bing%')
                THEN('BINGSEARCH')
                WHEN(k.network_name ilike '%supersonic%')
                THEN('IRONSOURCE')
                WHEN(k.network_name ilike '%iron%source%')
                THEN('IRONSOURCE')
                WHEN((k.network_name ilike '%pch%')
                    OR  (k.network_name ilike '%liquid%'))
                THEN('LIQUIDWIRELESS')
                WHEN(k.network_name ilike '%tresensa%')
                THEN('TRESENSA')
                WHEN(k.network_name ilike '%digital%turbine%')
                THEN('DIGITALTURBINE')
                WHEN(((k.network_name ilike '%facebook%') or (k.network_name ilike '%instagram%'))
                AND (k.site_id ilike '%fbcoupon%')
                AND (DATE(k.install_day) <= '2018-12-30'))
                THEN('FACEBOOKCOUPON')
                WHEN(k.network_name ilike '%smartly%')
                THEN('SMARTLY')
                WHEN(k.network_name ilike '%moloco%')
                THEN('MOLOCO')
                WHEN(k.network_name ilike '%adaction%')
                THEN('ADACTION')
                WHEN(k.network_name ilike '%google%')
                THEN('GOOGLE')
                WHEN(k.network_name ilike '%admob%')
                THEN('GOOGLE')
                WHEN(k.network_name ilike '%adwords%')
                THEN('GOOGLE')
                WHEN(k.network_name ilike '%dauup%facebook%')
                THEN('DAUUPFACEBOOK')
                WHEN(k.network_name ilike '%dauup%android%')
                THEN('DAUUPNETWORK')
                WHEN(k.network_name ilike '%motive%')
                THEN('MOTIVEINTERACTIVE')
                WHEN(k.network_name ilike '%aarki%')
                THEN('AARKI')
                WHEN(k.network_name ilike '%unity%')
                THEN('UNITYADS')
                WHEN ((k.network_name ilike '%chartboost%')
                    AND (k.campaign_name ilike '%_dd_%'))
                THEN('CHARTBOOSTDIRECTDEAL')
                WHEN (k.network_name ilike '%sprinklr%instagram%')
                THEN('SPRINKLRINSTAGRAM')
                WHEN (k.network_name ilike '%instagram%sprinklr%')
                THEN('SPRINKLRINSTAGRAM')
                WHEN (k.network_name ilike '%sprinklr%facebook%')
                THEN('SPRINKLRFACEBOOK')
                WHEN (k.network_name ilike '%facebook%sprinklr%')
                THEN('SPRINKLRFACEBOOK')
                WHEN (k.network_name ilike '%sprinklr%yahoo%')
                THEN('SPRINKLRYAHOO')
                WHEN (k.network_name ilike '%yahoo%sprinklr%')
                THEN('SPRINKLRYAHOO')
                WHEN((k.network_name ilike '%facebook%')
                    AND (LOWER(k.site_id) ilike 'svl%'))
                THEN('STEALTHVENTURELABSFACEBOOK')
                WHEN ((k.network_name ilike '%facebook%')
                    AND (LOWER(k.site_id) LIKE '[conacq]%'))
                THEN('CONSUMERACQUISITIONFACEBOOK')
                WHEN (k.network_name ilike '%sprinklr%')
                THEN('SPRINKLRFACEBOOK')
                WHEN ((k.network_name ilike '%instagram%')
                    AND (k.app_name ilike '%wof%'))
                THEN('FACEBOOK')
                WHEN ((k.network_name ilike '%instagram%')
                    AND (DATE(k.install_day)>='2015-12-10'))
                THEN('CONSUMERACQUISITIONINSTAGRAM')
                WHEN ((k.network_name ilike '%instagram%')
                    AND (DATE(k.install_day)<'2015-12-10'))
                THEN('INSTAGRAM')
                WHEN(k.network_name ilike '%glispaandroid%')
                THEN('GLISPA')
                WHEN(k.network_name ilike '%apple%search%')
                THEN('APPLESEARCHADS')
                ELSE TRIM(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(k.network_name),
                    '- ANDROID|- IOS|- FACEBOOK|- AMAZON|- ANROID|– ANDROID|– IOS|– FACEBOOK|– AMAZON|– ANROID|-ANDROID|-IOS|-FACEBOOK|-AMAZON|-ANROID|–ANDROID|–IOS|–FACEBOOK|–AMAZON|–ANROID'
                    ,''),'[^A-Z0-9]',''))
                END ||'||'||
                CASE
                    WHEN((k.network_name ilike '%google%') AND (k.model ilike '%IPAD%'))
                    THEN 'IPHONE'
                WHEN(((k.network_name ilike '%facebook%') or (k.network_name ilike '%instagram%'))
                AND (k.model ilike '%IPAD%'))
                THEN 'IPHONE'
                WHEN(k.model ilike '%ANDROID%')
                THEN 'ANDROID'
                WHEN(k.model ilike '%IPHONE%')
                THEN 'IPHONE'
                WHEN(k.model ilike '%IPAD%')
                THEN 'IPAD'
                WHEN((k.model ilike '%AMAZON%')
                    OR  (k.model ilike '%KINDLE%'))
                THEN 'AMAZON'
                WHEN(k.model ilike '%IPOD%')
                THEN 'IPHONE'
                WHEN(k.platform ilike '%android%')
                THEN 'ANDROID'
                WHEN((k.platform ilike '%amazon%')
                    OR  (k.platform ilike '%kindle%'))
                THEN 'AMAZON'
                WHEN(k.platform ilike '%iphone%')
                THEN 'IPHONE'
                WHEN(k.platform ilike '%ipad%')
                THEN 'IPAD'
                WHEN(k.platform ilike '%ipod%')
                THEN 'IPHONE'
                WHEN(k.campaign_id ilike 'kobingobashandroid%')
                THEN('ANDROID')
                WHEN(k.campaign_id ilike 'kobingobashhdios%')
                THEN('IPAD')
                WHEN(k.campaign_id ilike 'kobingobashios%')
                THEN('IPHONE')
                WHEN(k.campaign_id ilike 'kofreshdeckpokerandroid%')
                THEN('ANDROID')
                WHEN(k.campaign_id ilike 'kofreshdeckpokerios%')
                THEN('IPHONE')
                WHEN(k.campaign_id ilike 'kofreshdeckpokerkindle%')
                THEN('AMAZON')
                WHEN(k.campaign_id ilike 'kogo-poker-android%')
                THEN('ANDROID')
                WHEN(k.campaign_id ilike 'kogo-poker-kindle%')
                THEN('AMAZON')
                WHEN(k.campaign_id ilike 'kogsn-grand-casino-ios%')
                THEN('IPHONE')
                WHEN(k.campaign_id ilike 'kogsn-grand-casino%')
                THEN('ANDROID')
                WHEN(k.campaign_id ilike 'kogsncasinoamazon%')
                THEN('AMAZON')
                WHEN(k.campaign_id ilike 'kogsncasinoandroid%')
                THEN('ANDROID')
                WHEN(k.campaign_id ilike 'kogsncasinoios%')
                THEN('IPHONE')
                WHEN(k.campaign_id ilike 'komybingobash%')
                THEN('AMAZON')
                WHEN(k.campaign_id ilike 'koslotsbash%')
                THEN('IPAD')
                WHEN(k.campaign_id ilike 'koslotsbashandroid%')
                THEN('ANDROID')
                WHEN(k.campaign_id ilike 'koslotsoffunandroid%')
                THEN('ANDROID')
                WHEN(k.campaign_id ilike 'koslotsoffunios%')
                THEN('IPHONE')
                WHEN(k.campaign_id ilike 'koslotsoffunkindle%')
                THEN('AMAZON')
                WHEN(k.campaign_id ilike 'kosolitaire-tripeaks-amazon%')
                THEN('AMAZON')
                WHEN(k.campaign_id ilike 'kosolitaire-tripeaks-ios%')
                THEN('IPHONE')
                WHEN(k.campaign_id ilike 'kosolitairetripeaks%')
                THEN('ANDROID')
                WHEN(k.campaign_id ilike 'kowheel-of-fortune-slots-amazon%')
                THEN('AMAZON')
                WHEN(k.campaign_id ilike 'kowheel-of-fortune-slots-ios%')
                THEN('IPHONE')
                WHEN(k.campaign_id ilike 'kowheel-of-fortune-slots-android%')
                THEN('ANDROID')
                WHEN(k.campaign_id ilike 'kosmash-ios%')
                THEN('IPHONE')
                WHEN(k.campaign_id ilike 'kosmash-android%')
                THEN('ANDROID')
                WHEN(k.campaign_id ilike 'kosmash-amazon%')
                THEN('AMAZON')
                WHEN(k.model ilike '%IOS%')
                THEN 'IPHONE'
                WHEN(k.platform ilike '%ios%')
                THEN 'IPHONE'
                WHEN(LOWER(k.model) IN ('pc',
                                              'desktop',
                                              'web',
                                              'canvas',
                                              'fb_browser'))
                THEN('FBCANVAS')
                WHEN(LOWER(k.platform) IN ('pc',
                                                 'fb',
                                                 'desktop',
                                                 'web',
                                                 'canvas'))
                THEN('FBCANVAS')
                ELSE 'UNKNOWN'
                END ||'||'|| CAST(CAST(EXTRACT(EPOCH FROM DATE_TRUNC('day',k.install_day)) AS INTEGER) AS
                VARCHAR) AS join_field_kochava_to_spend
            , case
                when (date(k.first_ww_app_login) <= date(k.first_ww_reg) and date(k.install_timestamp) < '2018-10-09')
                    or (date(k.install_timestamp) >= '2018-10-09' and d.user_id is not null)
                    or (k.first_ww_reg is null) then 'New' else 'Legacy' end as install_type
            , sum(case when m.days_horizon = 90 then m.predicted_iap_revenue else 0 end) as ml_cef_90d
            , sum(case when m.days_horizon = 180 then m.predicted_iap_revenue else 0 end) as ml_cef_180d
            , count(distinct case when m.days_horizon = 90 then m.idfv end) as ml_devices_90d
            , count(distinct case when m.days_horizon = 180 then m.idfv end) as ml_devices_180d
        FROM gsnmobile.kochava_device_summary_phoenix k
        left join --check that first instance of user_id on idfv registered on app per ww.login_activity (10/9/18+ logic)
            (
            select
                  b.idfv
                , ac.user_id
            from
                first_idfv b
            left join
                ( --check to see if first user_id associated with idfv registered on app, and on the same idfv
                select distinct x.user_id
                from
                    (
                    select distinct
                          a.user_id
                        , platform_id
                        , activity_date
                        , row_number() over (partition by a.user_id order by activity_date) as login_rank
                    from ww.login_activity a
                    join ww.dim_users u
                    on a.user_id = u.user_id
                    where u.createdate >='2018-10-09'
                    ) x
                join phoenix.events_client e
                    on x.user_id = e.user_id
                    and timestampadd(hour,3,e.event_time) >= timestampadd(hour,-1,x.activity_date)
                    and timestampadd(hour,3,e.event_time) <= timestampadd(hour,1,x.activity_date) --look for idfv associated with user_id within first hour of registration
                where x.login_rank = 1 and x.platform_id > 2 --IDs 1 and 2 are desktop and moweb, everything else is app
                ) ac
                on b.user_id = ac.user_id
            ) d
                ON k.idfv = d.idfv
            left join
                (
                select * from
                        (
                        select distinct
                                  idfv
                                , days_horizon
                                , days_observation
                                , predicted_iap_revenue
                                , row_number() over (partition by idfv, days_horizon order by days_observation desc) as obs_rank
                        from gsnmobile.worldwinner_mobile_ltv_predictions_idfv_latest_by_days_horizon_days_observation_v
                        where model_version = 2
                        ) x
                where obs_rank = 1
                ) m
                on k.idfv = m.idfv
    group by 1,2,3,4,5,6,7
SEGMENTED BY hash(idfv)
ALL NODES KSAFE 1
;


CREATE LOCAL TEMP TABLE player_retention
ON COMMIT PRESERVE ROWS DIRECT AS

    select /*+ DIRECT, LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}') */
        distinct
              r.user_id
            , max(case when datediff(day,r.createdate,timestampadd(hour,-3,pg.start_time)) = 0 then 1 else 0 end) as player_retention_d0
            , max(case when datediff(day,r.createdate,timestampadd(hour,-3,pg.start_time)) = 1 then 1 else 0 end) as player_retention_d1
            , max(case when datediff(day,r.createdate,timestampadd(hour,-3,pg.start_time)) = 2 then 1 else 0 end) as player_retention_d2
            , max(case when datediff(day,r.createdate,timestampadd(hour,-3,pg.start_time)) = 3 then 1 else 0 end) as player_retention_d3
            , max(case when datediff(day,r.createdate,timestampadd(hour,-3,pg.start_time)) = 4 then 1 else 0 end) as player_retention_d4
            , max(case when datediff(day,r.createdate,timestampadd(hour,-3,pg.start_time)) = 5 then 1 else 0 end) as player_retention_d5
            , max(case when datediff(day,r.createdate,timestampadd(hour,-3,pg.start_time)) = 6 then 1 else 0 end) as player_retention_d6
            , max(case when datediff(day,r.createdate,timestampadd(hour,-3,pg.start_time)) = 7 then 1 else 0 end) as player_retention_d7
            , max(case when datediff(day,r.createdate,timestampadd(hour,-3,pg.start_time)) = 14 then 1 else 0 end) as player_retention_d14
            , max(case when datediff(day,r.createdate,timestampadd(hour,-3,pg.start_time)) = 30 then 1 else 0 end) as player_retention_d30
            , max(case when datediff(day,r.createdate,timestampadd(hour,-3,pg.start_time)) = 60 then 1 else 0 end) as player_retention_d60
            , max(case when datediff(day,r.createdate,timestampadd(hour,-3,pg.start_time)) = 90 then 1 else 0 end) as player_retention_d90
            , max(case when datediff(day,r.createdate,timestampadd(hour,-3,pg.start_time)) = 120 then 1 else 0 end) as player_retention_d120
            , max(case when datediff(day,r.createdate,timestampadd(hour,-3,pg.start_time)) = 150 then 1 else 0 end) as player_retention_d150
            , max(case when datediff(day,r.createdate,timestampadd(hour,-3,pg.start_time)) = 180 then 1 else 0 end) as player_retention_d180
            , max(case when datediff(day,r.createdate,timestampadd(hour,-3,pg.start_time)) = 360 then 1 else 0 end) as player_retention_d360
            , max(case when datediff(day,r.createdate,timestampadd(hour,-3,pg.start_time)) = 540 then 1 else 0 end) as player_retention_d540
        from app_registrations r
        join ww.played_games pg
            on r.user_id = pg.user_id
        group by 1
SEGMENTED BY hash(user_id)
ALL NODES KSAFE 1
;

CREATE LOCAL TEMP TABLE payer_retention
ON COMMIT PRESERVE ROWS DIRECT AS
            select /*+ DIRECT, LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}') */
                distinct
              r.user_id
            , max(case when datediff(day,r.real_money_date,timestampadd(hour,-3,it.trans_date)) = 0 then 1 else 0 end) as payer_retention_d0
            , max(case when datediff(day,r.real_money_date,timestampadd(hour,-3,it.trans_date)) = 1 then 1 else 0 end) as payer_retention_d1
            , max(case when datediff(day,r.real_money_date,timestampadd(hour,-3,it.trans_date)) = 2 then 1 else 0 end) as payer_retention_d2
            , max(case when datediff(day,r.real_money_date,timestampadd(hour,-3,it.trans_date)) = 3 then 1 else 0 end) as payer_retention_d3
            , max(case when datediff(day,r.real_money_date,timestampadd(hour,-3,it.trans_date)) = 4 then 1 else 0 end) as payer_retention_d4
            , max(case when datediff(day,r.real_money_date,timestampadd(hour,-3,it.trans_date)) = 5 then 1 else 0 end) as payer_retention_d5
            , max(case when datediff(day,r.real_money_date,timestampadd(hour,-3,it.trans_date)) = 6 then 1 else 0 end) as payer_retention_d6
            , max(case when datediff(day,r.real_money_date,timestampadd(hour,-3,it.trans_date)) = 7 then 1 else 0 end) as payer_retention_d7
            , max(case when datediff(day,r.real_money_date,timestampadd(hour,-3,it.trans_date)) = 14 then 1 else 0 end) as payer_retention_d14
            , max(case when datediff(day,r.real_money_date,timestampadd(hour,-3,it.trans_date)) = 30 then 1 else 0 end) as payer_retention_d30
            , max(case when datediff(day,r.real_money_date,timestampadd(hour,-3,it.trans_date)) = 60 then 1 else 0 end) as payer_retention_d60
            , max(case when datediff(day,r.real_money_date,timestampadd(hour,-3,it.trans_date)) = 90 then 1 else 0 end) as payer_retention_d90
            , max(case when datediff(day,r.real_money_date,timestampadd(hour,-3,it.trans_date)) = 120 then 1 else 0 end) as payer_retention_d120
            , max(case when datediff(day,r.real_money_date,timestampadd(hour,-3,it.trans_date)) = 150 then 1 else 0 end) as payer_retention_d150
            , max(case when datediff(day,r.real_money_date,timestampadd(hour,-3,it.trans_date)) = 180 then 1 else 0 end) as payer_retention_d180
            , max(case when datediff(day,r.real_money_date,timestampadd(hour,-3,it.trans_date)) = 360 then 1 else 0 end) as payer_retention_d360
            , max(case when datediff(day,r.real_money_date,timestampadd(hour,-3,it.trans_date)) = 540 then 1 else 0 end) as payer_retention_d540
        from app_registrations r
        join ww.internal_transactions it
            on r.user_id = it.user_id
        where it.trans_type = 'SIGNUP'
        and r.real_money_date is not null
        and it.trans_date < date(sysdate())
        group by 1
SEGMENTED BY hash(user_id)
ALL NODES KSAFE 1
;

CREATE LOCAL TEMP TABLE games_played
ON COMMIT PRESERVE ROWS DIRECT AS
            select /*+ DIRECT, LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}') */
user_id
, count(distinct game_id) as games_played
from ww.played_games
group by 1
SEGMENTED BY hash(user_id)
ALL NODES KSAFE 1
;
CREATE LOCAL TEMP TABLE app_retention
ON COMMIT PRESERVE ROWS DIRECT AS
        select /*+ DIRECT, LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}') */
            distinct
              r.user_id
            , max(case when datediff(day,r.createdate,e.event_day) = 0 then 1 else 0 end) as app_retention_d0
            , max(case when datediff(day,r.createdate,e.event_day) = 1 then 1 else 0 end) as app_retention_d1
            , max(case when datediff(day,r.createdate,e.event_day) = 2 then 1 else 0 end) as app_retention_d2
            , max(case when datediff(day,r.createdate,e.event_day) = 3 then 1 else 0 end) as app_retention_d3
            , max(case when datediff(day,r.createdate,e.event_day) = 4 then 1 else 0 end) as app_retention_d4
            , max(case when datediff(day,r.createdate,e.event_day) = 5 then 1 else 0 end) as app_retention_d5
            , max(case when datediff(day,r.createdate,e.event_day) = 6 then 1 else 0 end) as app_retention_d6
            , max(case when datediff(day,r.createdate,e.event_day) = 7 then 1 else 0 end) as app_retention_d7
            , max(case when datediff(day,r.createdate,e.event_day) = 14 then 1 else 0 end) as app_retention_d14
            , max(case when datediff(day,r.createdate,e.event_day) = 30 then 1 else 0 end) as app_retention_d30
            , max(case when datediff(day,r.createdate,e.event_day) = 60 then 1 else 0 end) as app_retention_d60
            , max(case when datediff(day,r.createdate,e.event_day) = 90 then 1 else 0 end) as app_retention_d90
            , max(case when datediff(day,r.createdate,e.event_day) = 120 then 1 else 0 end) as app_retention_d120
            , max(case when datediff(day,r.createdate,e.event_day) = 150 then 1 else 0 end) as app_retention_d150
            , max(case when datediff(day,r.createdate,e.event_day) = 180 then 1 else 0 end) as app_retention_d180
            , max(case when datediff(day,r.createdate,e.event_day) = 360 then 1 else 0 end) as app_retention_d360
            , max(case when datediff(day,r.createdate,e.event_day) = 540 then 1 else 0 end) as app_retention_d540
        from app_registrations r
        join phoenix.user_day e
            on r.user_id = e.user_id
        where e.event_day < date(sysdate())
        group by 1
        SEGMENTED BY hash(user_id)
ALL NODES KSAFE 1
;

CREATE LOCAL TEMP TABLE ltv
ON COMMIT PRESERVE ROWS DIRECT AS
        select /*+ DIRECT, LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}') */
 distinct
      user_id
    , reg_date
    , days_since_reg
    , ump
    , nar
from ww.ltv_calculations
where ump is not null
            SEGMENTED BY hash(user_id)
ALL NODES KSAFE 1
;

CREATE LOCAL TEMP TABLE lifetime_deposits
ON COMMIT PRESERVE ROWS DIRECT AS
        select /*+ DIRECT, LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}') */
                  u.user_id
                , count(distinct it.int_trans_id) as lifetime_deposits
            from ww.dim_users u
            join ww.internal_transactions it
                on u.user_id = it.user_id
            where it.trans_type ilike '%deposit%'
            group by 1
            SEGMENTED BY hash(user_id)
ALL NODES KSAFE 1
;



CREATE LOCAL TEMP TABLE final_base
ON COMMIT PRESERVE ROWS DIRECT AS
        select /*+ DIRECT, LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}') */
              case
                when i.kochava_network_name_clean in ('UNATTRIBUTED','LONGTAILPILOT') or i.kochava_network_name_clean is null then 'Unattributed'
                when i.kochava_network_name_clean in ('BRANCHIO','SMARTLINKS') and i.network_name not ilike '%samsung test%' then 'CRM'
                else 'Paid'
                end as acquisition_category
            , i.kochava_network_name_clean
            , i.network_name
            , case when i.kochava_network_name_clean in ('SMARTLY','APPLESEARCHADS') or i.network_name = 'SmartLinks - Samsung TEST' then i.ad_group_id else null end as ad_group_id --added ASA 9/17/19
            , i.join_field_kochava_to_spend
            , i.install_date as day
            , i.idfv
            , i.ml_cef_90d
            , i.ml_cef_180d
           -- , v.selected_group
            , 'APP' as platform
            , count(distinct case when i.install_type = 'Legacy' then i.idfv end) as installs_legacy
            , count(distinct case when i.install_type = 'New' then i.idfv end) as installs_new
            , count(distinct i.idfv) as installs_total
            , count(distinct r.user_id) as registrations
            , count(distinct g.user_id) as p1_games_completed
            , count(distinct case when g.games_played > 1 then g.user_id end) as p2_games_completed
            , count(distinct case when r.real_money_date is not null then r.user_id end) as ftds
            , count(distinct case when r.real_money_date is not null and datediff(day,i.install_date,r.real_money_date) = 0 and datediff(day,i.install_date,date(sysdate() - 1)) >= 0 then r.user_id end) as ftds_d0
            , count(distinct case when r.real_money_date is not null and datediff(day,i.install_date,r.real_money_date) <= 1 and datediff(day,i.install_date,r.real_money_date) >= 0 then r.user_id end) as ftds_d1
            , count(distinct case when r.real_money_date is not null and datediff(day,i.install_date,r.real_money_date) <= 7 and datediff(day,i.install_date,r.real_money_date) >= 0 then r.user_id end) as ftds_d7
            , count(distinct case when r.real_money_date is not null and datediff(day,i.install_date,r.real_money_date) <= 14 and datediff(day,i.install_date,r.real_money_date) >= 0 then r.user_id end) as ftds_d14
            , count(distinct case when r.real_money_date is not null and datediff(day,i.install_date,r.real_money_date) <= 30 and datediff(day,i.install_date,r.real_money_date) >= 0 then r.user_id end) as ftds_d30
            , count(distinct case when r.real_money_date is not null and datediff(day,i.install_date,r.real_money_date) <= 60 and datediff(day,i.install_date,r.real_money_date) >= 0 then r.user_id end) as ftds_d60
            , count(distinct case when r.real_money_date is not null and datediff(day,i.install_date,r.real_money_date) <= 90 and datediff(day,i.install_date,r.real_money_date) >= 0 then r.user_id end) as ftds_d90

            , count(distinct case when r.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) = 0  then l.user_id end) as umps_d0
            , count(distinct case when r.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 1 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.user_id end) as umps_d1
            , count(distinct case when r.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 7 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.user_id end) as umps_d7
            , count(distinct case when r.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 14 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.user_id end) as umps_d14
            , count(distinct case when r.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 30 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.user_id end) as umps_d30
            , count(distinct case when r.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 60 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.user_id end) as umps_d60
            , count(distinct case when r.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 90 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.user_id end) as umps_d90
            , count(distinct case when r.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 120 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.user_id end) as umps_d120
            , count(distinct case when r.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 150 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.user_id end) as umps_d150
            , count(distinct case when r.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 180 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.user_id end) as umps_d180
            , count(distinct case when r.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 360 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.user_id end) as umps_d360
            , count(distinct case when r.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 540 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.user_id end) as umps_d540
            , count(distinct case when r.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 720 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.user_id end) as umps_d720
            , count(distinct case when r.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 1080 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.user_id end) as umps_d1080

            , sum(case when r.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) = 0 then l.nar end) as nar_d0
            , sum(case when r.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 1 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.nar end) as nar_d1
            , sum(case when r.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 7 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.nar end) as nar_d7
            , sum(case when r.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 14 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.nar end) as nar_d14
            , sum(case when r.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 30 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.nar end) as nar_d30
            , sum(case when r.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 60 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.nar end) as nar_d60
            , sum(case when r.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 90 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.nar end) as nar_d90
            , sum(case when r.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 120 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.nar end) as nar_d120
            , sum(case when r.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 150 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.nar end) as nar_d150
            , sum(case when r.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 180 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.nar end) as nar_d180
            , sum(case when r.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 360 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.nar end) as nar_d360
            , sum(case when r.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 540 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.nar end) as nar_d540
            , sum(case when r.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 720 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.nar end) as nar_d720
            , sum(case when r.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 1080 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.nar end) as nar_d1080
            ---
            , count(distinct case when r.user_id is null and id.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) = 0 then l.user_id end) as umps_d0_logins
            , count(distinct case when r.user_id is null and id.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 1 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.user_id end) as umps_d1_logins
            , count(distinct case when r.user_id is null and id.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 7 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.user_id end) as umps_d7_logins
            , count(distinct case when r.user_id is null and id.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 14 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.user_id end) as umps_d14_logins
            , count(distinct case when r.user_id is null and id.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 30 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.user_id end) as umps_d30_logins
            , count(distinct case when r.user_id is null and id.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 60 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.user_id end) as umps_d60_logins
            , count(distinct case when r.user_id is null and id.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 90 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.user_id end) as umps_d90_logins
            , count(distinct case when r.user_id is null and id.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 120 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.user_id end) as umps_d120_logins
            , count(distinct case when r.user_id is null and id.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 150 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.user_id end) as umps_d150_logins
            , count(distinct case when r.user_id is null and id.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 180 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.user_id end) as umps_d180_logins
            , count(distinct case when r.user_id is null and id.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 360 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.user_id end) as umps_d360_logins
            , count(distinct case when r.user_id is null and id.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 540 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.user_id end) as umps_d540_logins
            , count(distinct case when r.user_id is null and id.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 720 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.user_id end) as umps_d720_logins
            , count(distinct case when r.user_id is null and id.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 1080 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.user_id end) as umps_d1080_logins
            , sum(case when r.user_id is null and id.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) = 0 then l.nar end) as nar_d0_logins
            , sum(case when r.user_id is null and id.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 1 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.nar end) as nar_d1_logins
            , sum(case when r.user_id is null and id.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 7 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.nar end) as nar_d7_logins
            , sum(case when r.user_id is null and id.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 14 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.nar end) as nar_d14_logins
            , sum(case when r.user_id is null and id.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 30 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.nar end) as nar_d30_logins
            , sum(case when r.user_id is null and id.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 60 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.nar end) as nar_d60_logins
            , sum(case when r.user_id is null and id.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 90 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.nar end) as nar_d90_logins
            , sum(case when r.user_id is null and id.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 120 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.nar end) as nar_d120_logins
            , sum(case when r.user_id is null and id.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 150 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.nar end) as nar_d150_logins
            , sum(case when r.user_id is null and id.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 180 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.nar end) as nar_d180_logins
            , sum(case when r.user_id is null and id.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 360 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.nar end) as nar_d360_logins
            , sum(case when r.user_id is null and id.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 540 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.nar end) as nar_d540_logins
            , sum(case when r.user_id is null and id.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 720 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.nar end) as nar_d720_logins
            , sum(case when r.user_id is null and id.user_id is not null and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) <= 1080 and datediff(day,i.install_date,timestampadd(day,l.days_since_reg,l.reg_date)) >= 0 then l.nar end) as nar_d1080_logins
            , count(distinct case when plr.player_retention_d0 > 0 then plr.user_id end) as player_retention_d0
            , count(distinct case when plr.player_retention_d1 > 0 then plr.user_id end) as player_retention_d1
            , count(distinct case when plr.player_retention_d2 > 0 then plr.user_id end) as player_retention_d2
            , count(distinct case when plr.player_retention_d3 > 0 then plr.user_id end) as player_retention_d3
            , count(distinct case when plr.player_retention_d4 > 0 then plr.user_id end) as player_retention_d4
            , count(distinct case when plr.player_retention_d5 > 0 then plr.user_id end) as player_retention_d5
            , count(distinct case when plr.player_retention_d6 > 0 then plr.user_id end) as player_retention_d6
            , count(distinct case when plr.player_retention_d7 > 0 then plr.user_id end) as player_retention_d7
            , count(distinct case when plr.player_retention_d14 > 0 then plr.user_id end) as player_retention_d14
            , count(distinct case when plr.player_retention_d30 > 0 then plr.user_id end) as player_retention_d30
            , count(distinct case when plr.player_retention_d60 > 0 then plr.user_id end) as player_retention_d60
            , count(distinct case when plr.player_retention_d90 > 0 then plr.user_id end) as player_retention_d90
            , count(distinct case when plr.player_retention_d120 > 0 then plr.user_id end) as player_retention_d120
            , count(distinct case when plr.player_retention_d150 > 0 then plr.user_id end) as player_retention_d150
            , count(distinct case when plr.player_retention_d180 > 0 then plr.user_id end) as player_retention_d180
            , count(distinct case when plr.player_retention_d360 > 0 then plr.user_id end) as player_retention_d360
            , count(distinct case when plr.player_retention_d540 > 0 then plr.user_id end) as player_retention_d540
            , count(distinct case when par.payer_retention_d0 > 0 then par.user_id end) as payer_retention_d0
            , count(distinct case when par.payer_retention_d1 > 0 then par.user_id end) as payer_retention_d1
            , count(distinct case when par.payer_retention_d2 > 0 then par.user_id end) as payer_retention_d2
            , count(distinct case when par.payer_retention_d3 > 0 then par.user_id end) as payer_retention_d3
            , count(distinct case when par.payer_retention_d4 > 0 then par.user_id end) as payer_retention_d4
            , count(distinct case when par.payer_retention_d5 > 0 then par.user_id end) as payer_retention_d5
            , count(distinct case when par.payer_retention_d6 > 0 then par.user_id end) as payer_retention_d6
            , count(distinct case when par.payer_retention_d7 > 0 then par.user_id end) as payer_retention_d7
            , count(distinct case when par.payer_retention_d14 > 0 then par.user_id end) as payer_retention_d14
            , count(distinct case when par.payer_retention_d30 > 0 then par.user_id end) as payer_retention_d30
            , count(distinct case when par.payer_retention_d60 > 0 then par.user_id end) as payer_retention_d60
            , count(distinct case when par.payer_retention_d90 > 0 then par.user_id end) as payer_retention_d90
            , count(distinct case when par.payer_retention_d120 > 0 then par.user_id end) as payer_retention_d120
            , count(distinct case when par.payer_retention_d150 > 0 then par.user_id end) as payer_retention_d150
            , count(distinct case when par.payer_retention_d180 > 0 then par.user_id end) as payer_retention_d180
            , count(distinct case when par.payer_retention_d360 > 0 then par.user_id end) as payer_retention_d360
            , count(distinct case when par.payer_retention_d540 > 0 then par.user_id end) as payer_retention_d540
            , count(distinct case when a.app_retention_d0 > 0 then a.user_id end) as app_retention_d0
            , count(distinct case when a.app_retention_d1 > 0 then a.user_id end) as app_retention_d1
            , count(distinct case when a.app_retention_d2 > 0 then a.user_id end) as app_retention_d2
            , count(distinct case when a.app_retention_d3 > 0 then a.user_id end) as app_retention_d3
            , count(distinct case when a.app_retention_d4 > 0 then a.user_id end) as app_retention_d4
            , count(distinct case when a.app_retention_d5 > 0 then a.user_id end) as app_retention_d5
            , count(distinct case when a.app_retention_d6 > 0 then a.user_id end) as app_retention_d6
            , count(distinct case when a.app_retention_d7 > 0 then a.user_id end) as app_retention_d7
            , count(distinct case when a.app_retention_d14 > 0 then a.user_id end) as app_retention_d14
            , count(distinct case when a.app_retention_d30 > 0 then a.user_id end) as app_retention_d30
            , count(distinct case when a.app_retention_d60 > 0 then a.user_id end) as app_retention_d60
            , count(distinct case when a.app_retention_d90 > 0 then a.user_id end) as app_retention_d90
            , count(distinct case when a.app_retention_d120 > 0 then a.user_id end) as app_retention_d120
            , count(distinct case when a.app_retention_d150 > 0 then a.user_id end) as app_retention_d150
            , count(distinct case when a.app_retention_d180 > 0 then a.user_id end) as app_retention_d180
            , count(distinct case when a.app_retention_d360 > 0 then a.user_id end) as app_retention_d360
            , count(distinct case when a.app_retention_d540 > 0 then a.user_id end) as app_retention_d540

            , count(distinct case when datediff(day,r.real_money_date,date(sysdate()) - 1) >= 0 then r.user_id end) as payer_retention_d0_denom
            , count(distinct case when datediff(day,r.real_money_date,date(sysdate()) - 1) >= 1 then r.user_id end) as payer_retention_d1_denom
            , count(distinct case when datediff(day,r.real_money_date,date(sysdate()) - 1) >= 2 then r.user_id end) as payer_retention_d2_denom
            , count(distinct case when datediff(day,r.real_money_date,date(sysdate()) - 1) >= 3 then r.user_id end) as payer_retention_d3_denom
            , count(distinct case when datediff(day,r.real_money_date,date(sysdate()) - 1) >= 4 then r.user_id end) as payer_retention_d4_denom
            , count(distinct case when datediff(day,r.real_money_date,date(sysdate()) - 1) >= 5 then r.user_id end) as payer_retention_d5_denom
            , count(distinct case when datediff(day,r.real_money_date,date(sysdate()) - 1) >= 6 then r.user_id end) as payer_retention_d6_denom
            , count(distinct case when datediff(day,r.real_money_date,date(sysdate()) - 1) >= 7 then r.user_id end) as payer_retention_d7_denom
            , count(distinct case when datediff(day,r.real_money_date,date(sysdate()) - 1) >= 14 then r.user_id end) as payer_retention_d14_denom
            , count(distinct case when datediff(day,r.real_money_date,date(sysdate()) - 1) >= 30 then r.user_id end) as payer_retention_d30_denom
            , count(distinct case when datediff(day,r.real_money_date,date(sysdate()) - 1) >= 60 then r.user_id end) as payer_retention_d60_denom
            , count(distinct case when datediff(day,r.real_money_date,date(sysdate()) - 1) >= 90 then r.user_id end) as payer_retention_d90_denom
            , count(distinct case when datediff(day,r.real_money_date,date(sysdate()) - 1) >= 120 then r.user_id end) as payer_retention_d120_denom
            , count(distinct case when datediff(day,r.real_money_date,date(sysdate()) - 1) >= 150 then r.user_id end) as payer_retention_d150_denom
            , count(distinct case when datediff(day,r.real_money_date,date(sysdate()) - 1) >= 180 then r.user_id end) as payer_retention_d180_denom
            , count(distinct case when datediff(day,r.real_money_date,date(sysdate()) - 1) >= 360 then r.user_id end) as payer_retention_d360_denom
            , count(distinct case when datediff(day,r.real_money_date,date(sysdate()) - 1) >= 540 then r.user_id end) as payer_retention_d540_denom
            , count(distinct case when i.ml_devices_90d = 1 then i.idfv end) as ml_devices_90d
            , count(distinct case when i.ml_devices_180d = 1 then i.idfv end) as ml_devices_180d
            , count(distinct case when ld.lifetime_deposits > 1 then r.user_id end) as second_deposits
        from app_installs i
        left join idfv_user_lookup id -- get all users on the idfv
            on i.idfv = id.idfv
        left join
            app_registrations r
            on id.user_id = r.user_id
        left join
            games_played g
            on r.user_id = g.user_id
        left join
            ltv l
            on id.user_id = l.user_id
        left join player_retention plr
            on r.user_id = plr.user_id
        left join payer_retention par
            on r.user_id = par.user_id
        left join app_retention a
            on r.user_id = a.user_id
        left join
            lifetime_deposits ld
            on r.user_id = ld.user_id
        where i.install_date < date(sysdate())
        group by 1,2,3,4,5,6,7,8,9,10;


INSERT /*+ DIRECT, LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}') */
    INTO ww.app_acquisition_ods

select /*+ DIRECT, LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}') */
      y.acquisition_category
    , y.kochava_network_name_clean
    , y.day
    , y.platform
    , y.installs_legacy
    , y.installs_new
    , y.installs_total
    , y.registrations
    , y.p1_games_completed
    , y.p2_games_completed
    , y.ftds
    , y.ftds_d0
    , y.ftds_d1
    , y.ftds_d7
    , y.ftds_d14
    , y.ftds_d30
    , y.ftds_d60
    , y.ftds_d90
    , y.umps_d0
    , y.umps_d1
    , y.umps_d7
    , y.umps_d14
    , y.umps_d30
    , y.umps_d60
    , y.umps_d90
    , y.umps_d120
    , y.umps_d150
    , y.umps_d180
    , y.umps_d360
    , y.umps_d540
    , y.umps_d720
    , y.umps_d1080
    , y.nar_d0
    , y.nar_d1
    , y.nar_d7
    , y.nar_d14
    , y.nar_d30
    , y.nar_d60
    , y.nar_d90
    , y.nar_d120
    , y.nar_d150
    , y.nar_d180
    , y.nar_d360
    , y.nar_d540
    , y.nar_d720
    , y.nar_d1080
    , y.umps_d0_logins
    , y.umps_d1_logins
    , y.umps_d7_logins
    , y.umps_d14_logins
    , y.umps_d30_logins
    , y.umps_d60_logins
    , y.umps_d90_logins
    , y.umps_d120_logins
    , y.umps_d150_logins
    , y.umps_d180_logins
    , y.umps_d360_logins
    , y.umps_d540_logins
    , y.umps_d720_logins
    , y.umps_d1080_logins
    , y.nar_d0_logins
    , y.nar_d1_logins
    , y.nar_d7_logins
    , y.nar_d14_logins
    , y.nar_d30_logins
    , y.nar_d60_logins
    , y.nar_d90_logins
    , y.nar_d120_logins
    , y.nar_d150_logins
    , y.nar_d180_logins
    , y.nar_d360_logins
    , y.nar_d540_logins
    , y.nar_d720_logins
    , y.nar_d1080_logins
    , y.player_retention_d0
    , y.player_retention_d1
    , y.player_retention_d2
    , y.player_retention_d3
    , y.player_retention_d4
    , y.player_retention_d5
    , y.player_retention_d6
    , y.player_retention_d7
    , y.player_retention_d14
    , y.player_retention_d30
    , y.player_retention_d60
    , y.player_retention_d90
    , y.player_retention_d120
    , y.player_retention_d150
    , y.player_retention_d180
    , y.player_retention_d360
    , y.player_retention_d540
    , y.payer_retention_d0
    , y.payer_retention_d1
    , y.payer_retention_d2
    , y.payer_retention_d3
    , y.payer_retention_d4
    , y.payer_retention_d5
    , y.payer_retention_d6
    , y.payer_retention_d7
    , y.payer_retention_d14
    , y.payer_retention_d30
    , y.payer_retention_d60
    , y.payer_retention_d90
    , y.payer_retention_d120
    , y.payer_retention_d150
    , y.payer_retention_d180
    , y.payer_retention_d360
    , y.payer_retention_d540
    , y.payer_retention_d0_denom
    , y.payer_retention_d1_denom
    , y.payer_retention_d2_denom
    , y.payer_retention_d3_denom
    , y.payer_retention_d4_denom
    , y.payer_retention_d5_denom
    , y.payer_retention_d6_denom
    , y.payer_retention_d7_denom
    , y.payer_retention_d14_denom
    , y.payer_retention_d30_denom
    , y.payer_retention_d60_denom
    , y.payer_retention_d90_denom
    , y.payer_retention_d120_denom
    , y.payer_retention_d150_denom
    , y.payer_retention_d180_denom
    , y.payer_retention_d360_denom
    , y.payer_retention_d540_denom
    , y.app_retention_d0
    , y.app_retention_d1
    , y.app_retention_d2
    , y.app_retention_d3
    , y.app_retention_d4
    , y.app_retention_d5
    , y.app_retention_d6
    , y.app_retention_d7
    , y.app_retention_d14
    , y.app_retention_d30
    , y.app_retention_d60
    , y.app_retention_d90
    , y.app_retention_d120
    , y.app_retention_d150
    , y.app_retention_d180
    , y.app_retention_d360
    , y.app_retention_d540
    , sum(coalesce(s.spend,ss.spend)) as spend
    , y.ad_group_id
    , s.adn_campaign_name
    , s.adn_sub_campaign_name
    , y.join_field_kochava_to_spend
    , y.ml_cef_90d
    , y.ml_cef_180d
    , y.ml_devices_90d
    , y.ml_devices_180d
    , y.second_deposits
    , sum(coalesce(s.clicks,ss.clicks)) as clicks
    , sum(coalesce(s.impressions,ss.impressions)) as impressions
    , split_part(y.join_field_kochava_to_spend,'||',3) as os --added 9/17/19
    from
 (
select
      x.acquisition_category
    , x.kochava_network_name_clean
    , case when x.network_name = 'SmartLinks - Samsung TEST' then x.network_name else x.kochava_network_name_clean end as network_name
    , x.ad_group_id
    , x.join_field_kochava_to_spend
    , x.day
    , x.platform
    , sum(x.installs_legacy) as installs_legacy
    , sum(x.installs_new) as installs_new
    , sum(x.installs_total) as installs_total
    , sum(x.registrations) as registrations
    , sum(x.p1_games_completed) as p1_games_completed
    , sum(x.p2_games_completed) as p2_games_completed
    , sum(x.ftds) as ftds
    , sum(x.ftds_d0) as ftds_d0
    , sum(x.ftds_d1) as ftds_d1
    , sum(x.ftds_d7) as ftds_d7
    , sum(x.ftds_d14) as ftds_d14
    , sum(x.ftds_d30) as ftds_d30
    , sum(x.ftds_d60) as ftds_d60
    , sum(x.ftds_d90) as ftds_d90
    , sum(x.umps_d0) as umps_d0
    , sum(x.umps_d1) as umps_d1
    , sum(x.umps_d7) as umps_d7
    , sum(x.umps_d14) as umps_d14
    , sum(x.umps_d30) as umps_d30
    , sum(x.umps_d60) as umps_d60
    , sum(x.umps_d90) as umps_d90
    , sum(x.umps_d120) as umps_d120
    , sum(x.umps_d150) as umps_d150
    , sum(x.umps_d180) as umps_d180
    , sum(x.umps_d360) as umps_d360
    , sum(x.umps_d540) as umps_d540
    , sum(x.umps_d720) as umps_d720
    , sum(x.umps_d1080) as umps_d1080
    , sum(x.nar_d0) as nar_d0
    , sum(x.nar_d1) as nar_d1
    , sum(x.nar_d7) as nar_d7
    , sum(x.nar_d14) as nar_d14
    , sum(x.nar_d30) as nar_d30
    , sum(x.nar_d60) as nar_d60
    , sum(x.nar_d90) as nar_d90
    , sum(x.nar_d120) as nar_d120
    , sum(x.nar_d150) as nar_d150
    , sum(x.nar_d180) as nar_d180
    , sum(x.nar_d360) as nar_d360
    , sum(x.nar_d540) as nar_d540
    , sum(x.nar_d720) as nar_d720
    , sum(x.nar_d1080) as nar_d1080
    , sum(x.umps_d0_logins) as umps_d0_logins
    , sum(x.umps_d1_logins) as umps_d1_logins
    , sum(x.umps_d7_logins) as umps_d7_logins
    , sum(x.umps_d14_logins) as umps_d14_logins
    , sum(x.umps_d30_logins) as umps_d30_logins
    , sum(x.umps_d60_logins) as umps_d60_logins
    , sum(x.umps_d90_logins) as umps_d90_logins
    , sum(x.umps_d120_logins) as umps_d120_logins
    , sum(x.umps_d150_logins) as umps_d150_logins
    , sum(x.umps_d180_logins) as umps_d180_logins
    , sum(x.umps_d360_logins) as umps_d360_logins
    , sum(x.umps_d540_logins) as umps_d540_logins
    , sum(x.umps_d720_logins) as umps_d720_logins
    , sum(x.umps_d1080_logins) as umps_d1080_logins
    , sum(x.nar_d0_logins) as nar_d0_logins
    , sum(x.nar_d1_logins) as nar_d1_logins
    , sum(x.nar_d7_logins) as nar_d7_logins
    , sum(x.nar_d14_logins) as nar_d14_logins
    , sum(x.nar_d30_logins) as nar_d30_logins
    , sum(x.nar_d60_logins) as nar_d60_logins
    , sum(x.nar_d90_logins) as nar_d90_logins
    , sum(x.nar_d120_logins) as nar_d120_logins
    , sum(x.nar_d150_logins) as nar_d150_logins
    , sum(x.nar_d180_logins) as nar_d180_logins
    , sum(x.nar_d360_logins) as nar_d360_logins
    , sum(x.nar_d540_logins) as nar_d540_logins
    , sum(x.nar_d720_logins) as nar_d720_logins
    , sum(x.nar_d1080_logins) as nar_d1080_logins
    , sum(x.player_retention_d0) as player_retention_d0
    , sum(x.player_retention_d1) as player_retention_d1
    , sum(x.player_retention_d2) as player_retention_d2
    , sum(x.player_retention_d3) as player_retention_d3
    , sum(x.player_retention_d4) as player_retention_d4
    , sum(x.player_retention_d5) as player_retention_d5
    , sum(x.player_retention_d6) as player_retention_d6
    , sum(x.player_retention_d7) as player_retention_d7
    , sum(x.player_retention_d14) as player_retention_d14
    , sum(x.player_retention_d30) as player_retention_d30
    , sum(x.player_retention_d60) as player_retention_d60
    , sum(x.player_retention_d90) as player_retention_d90
    , sum(x.player_retention_d120) as player_retention_d120
    , sum(x.player_retention_d150) as player_retention_d150
    , sum(x.player_retention_d180) as player_retention_d180
    , sum(x.player_retention_d360) as player_retention_d360
    , sum(x.player_retention_d540) as player_retention_d540
    , sum(x.payer_retention_d0) as payer_retention_d0
    , sum(x.payer_retention_d1) as payer_retention_d1
    , sum(x.payer_retention_d2) as payer_retention_d2
    , sum(x.payer_retention_d3) as payer_retention_d3
    , sum(x.payer_retention_d4) as payer_retention_d4
    , sum(x.payer_retention_d5) as payer_retention_d5
    , sum(x.payer_retention_d6) as payer_retention_d6
    , sum(x.payer_retention_d7) as payer_retention_d7
    , sum(x.payer_retention_d14) as payer_retention_d14
    , sum(x.payer_retention_d30) as payer_retention_d30
    , sum(x.payer_retention_d60) as payer_retention_d60
    , sum(x.payer_retention_d90) as payer_retention_d90
    , sum(x.payer_retention_d120) as payer_retention_d120
    , sum(x.payer_retention_d150) as payer_retention_d150
    , sum(x.payer_retention_d180) as payer_retention_d180
    , sum(x.payer_retention_d360) as payer_retention_d360
    , sum(x.payer_retention_d540) as payer_retention_d540
    , sum(x.payer_retention_d0_denom) as payer_retention_d0_denom
    , sum(x.payer_retention_d1_denom) as payer_retention_d1_denom
    , sum(x.payer_retention_d2_denom) as payer_retention_d2_denom
    , sum(x.payer_retention_d3_denom) as payer_retention_d3_denom
    , sum(x.payer_retention_d4_denom) as payer_retention_d4_denom
    , sum(x.payer_retention_d5_denom) as payer_retention_d5_denom
    , sum(x.payer_retention_d6_denom) as payer_retention_d6_denom
    , sum(x.payer_retention_d7_denom) as payer_retention_d7_denom
    , sum(x.payer_retention_d14_denom) as payer_retention_d14_denom
    , sum(x.payer_retention_d30_denom) as payer_retention_d30_denom
    , sum(x.payer_retention_d60_denom) as payer_retention_d60_denom
    , sum(x.payer_retention_d90_denom) as payer_retention_d90_denom
    , sum(x.payer_retention_d120_denom) as payer_retention_d120_denom
    , sum(x.payer_retention_d150_denom) as payer_retention_d150_denom
    , sum(x.payer_retention_d180_denom) as payer_retention_d180_denom
    , sum(x.payer_retention_d360_denom) as payer_retention_d360_denom
    , sum(x.payer_retention_d540_denom) as payer_retention_d540_denom
    , sum(x.app_retention_d0) as app_retention_d0
    , sum(x.app_retention_d1) as app_retention_d1
    , sum(x.app_retention_d2) as app_retention_d2
    , sum(x.app_retention_d3) as app_retention_d3
    , sum(x.app_retention_d4) as app_retention_d4
    , sum(x.app_retention_d5) as app_retention_d5
    , sum(x.app_retention_d6) as app_retention_d6
    , sum(x.app_retention_d7) as app_retention_d7
    , sum(x.app_retention_d14) as app_retention_d14
    , sum(x.app_retention_d30) as app_retention_d30
    , sum(x.app_retention_d60) as app_retention_d60
    , sum(x.app_retention_d90) as app_retention_d90
    , sum(x.app_retention_d120) as app_retention_d120
    , sum(x.app_retention_d150) as app_retention_d150
    , sum(x.app_retention_d180) as app_retention_d180
    , sum(x.app_retention_d360) as app_retention_d360
    , sum(x.app_retention_d540) as app_retention_d540
    , sum(x.ml_cef_90d) as ml_cef_90d
    , sum(x.ml_cef_180d) as ml_cef_180d
    , sum(x.ml_devices_90d) as ml_devices_90d
    , sum(x.ml_devices_180d) as ml_devices_180d
    , sum(x.second_deposits) as second_deposits
    from
        final_base x
group by 1,2,3,4,5,6,7
) y
    left join
        (
        select
              start_date
            , adn_campaign_name
            , adn_campaign_id
            , adn_sub_campaign_name
            , adn_sub_campaign_id
            , singular_campaign_id
            , sum(adn_cost) as spend
            , sum(adn_clicks) as clicks
            , sum(adn_impressions) as impressions
        from apis.singular_campaign_stats
        where app ilike '%worldwinner%'
        group by 1,2,3,4,5,6
        ) s
        on (case when y.kochava_network_name_clean in ('SMARTLY','APPLESEARCHADS') then y.ad_group_id = s.adn_sub_campaign_id else y.ad_group_id = s.adn_campaign_id end)
        and y.day = s.start_date
        and (y.kochava_network_name_clean in ('SMARTLY','APPLESEARCHADS') or y.network_name = 'SmartLinks - Samsung TEST') --added ASA and Samsung 9/17/19
    left join
        (
        select
              join_field_singular_to_kochava
            , sum(singular_adn_cost) as spend
            , sum(singular_adn_clicks) as clicks
            , sum(singular_adn_impressions) as impressions
        from gsnmobile.tableau_ua_singular_data
        where singular_app_clean = 'WORLDWINNER'
        group by 1
        ) ss
        on y.join_field_kochava_to_spend = ss.join_field_singular_to_kochava
        and y.kochava_network_name_clean not in ('SMARTLY','APPLESEARCHADS') --added ASA 9/17/19
        and y.network_name <> 'SmartLinks - Samsung TEST' --added Samsung 9/17/19
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40
,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80
,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115
,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142
-- NEW
,144,145,146,147
,148,149,150,151,152,
155; --added 9/17/19



ALTER TABLE ww.app_acquisition, ww.app_acquisition_ods RENAME TO app_acquisition_old, app_acquisition;
ALTER TABLE ww.app_acquisition_old RENAME TO app_acquisition_ods;
TRUNCATE TABLE ww.app_acquisition_ods;

SELECT analyze_statistics('ww.app_acquisition');
