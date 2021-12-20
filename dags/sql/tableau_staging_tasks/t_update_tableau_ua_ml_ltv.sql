MERGE /*+ direct,LABEL ('airflow-ua-ua_master_main_dag.tableau_subdag-t_update_tableau_ua_ml_ltv')*/
INTO
    gsnmobile.tableau_ua_ml_LTV MAIN
USING
    (
        SELECT
            REGEXP_REPLACE(UPPER(CASE WHEN(kds.app_name='PHOENIX') THEN('WORLDWINNER') ELSE(kds.app_name) END),'[^A-Z0-9]','') ||'||'||
            CASE
		WHEN((kds.network_name ilike '%smartlink%')
		    AND (campaign_id IN ('kosolitairetripeaks179552cdd213d1b1ec8dadb11e078f',
					 'kosolitairetripeaks179552cdd213d1b1eff27d8be17f25',
					 'kosolitaire-tripeaks-ios53a8762dc3a45f7f36992c7065',
					 'kosolitaire-tripeaks-amazon542c346dd4e8d62409345f40e6',
					 'kosolitairetripeaks179552cdd213d1b1e2ed740d2e8bfa',
					 'kosolitairetripeaks179552cdd213d1b1e8c45df91957da',
					 'kosolitaire-tripeaks-ios53a8762dc3a4559d6e0f025582',
					 'kosolitaire-tripeaks-amazon542c346dd4e8d2134a9d909d97')))
		THEN('PLAYANEXTYAHOO')
		WHEN(kds.network_name ilike '%amazon%fire%')
		THEN('AMAZONMEDIAGROUP')
		WHEN(kds.network_name ilike '%wake%screen%')
		THEN('AMAZONMEDIAGROUP')
		WHEN((kds.app_name ilike '%bingo%bash%')
		    AND (kds.network_name ilike '%amazon%')
		    AND (kds.campaign_name ilike '%wakescreen%'))
		THEN('AMAZONMEDIAGROUP')
		WHEN(kds.network_name ilike '%mobilda%')
		THEN('SNAPCHAT')
		WHEN(kds.network_name ilike '%snap%')
		THEN('SNAPCHAT')
		WHEN(kds.network_name ilike '%applovin%')
		THEN('APPLOVIN')
		WHEN(kds.network_name ilike '%bing%')
		THEN('BINGSEARCH')
		WHEN(kds.network_name ilike '%aura%ironsource%')
		THEN('IRONSOURCEPRELOAD')
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
		WHEN(((kds.network_name ilike '%facebook%') or (kds.network_name ilike '%instagram%'))
	    		AND (kds.site_id ilike '%fbcoupon%')
			AND (DATE(kds.install_day) < '2018-12-30'))
         	THEN('FACEBOOKCOUPON')
		WHEN((kds.network_name ilike '%facebook%')
           		AND (LOWER(kds.site_id) ilike '%creativetest%'))
        	THEN('CREATIVETESTFB')
		WHEN(kds.network_name ilike '%smartly%')
		THEN('SMARTLY')
		WHEN(kds.network_name ilike '%bidalgo%')
		THEN('BIDALGO')
		WHEN(kds.network_name ilike '%moloco%')
		THEN('MOLOCO')
		WHEN(kds.network_name ilike '%adaction%')
		THEN('ADACTION')
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
		    AND (kds.site_id ilike 'svl%'))
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
		    '- ANDROID|- IOS|- FACEBOOK|- AMAZON|- ANROID|– ANDROID|– IOS|– FACEBOOK|– AMAZON|– ANROID|-ANDROID|-IOS|-FACEBOOK|-AMAZON|-ANROID|–ANDROID|–IOS|–FACEBOOK|–AMAZON|–ANROID'
		    ,''),'[^A-Z0-9]',''))
	    END ||'||'||
            CASE
                WHEN((kds.network_name ilike '%google%') AND (kds.model ilike '%IPAD%')) 
        	THEN 'IPHONE' 
		WHEN(((kds.network_name ilike '%facebook%') or (kds.network_name ilike '%instagram%'))
		AND (kds.model ilike '%IPAD%')) 
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
            
            ) AS VARCHAR)  
            ||'||'|| CASE 
		WHEN (kds.network_name='unattributed' 
		      AND ((kds.last_click_network_name ilike '%digital%turbine%') 
			OR (kds.last_click_network_name ilike '%aura%ironsource%'))) 
		THEN 'true'
		ELSE 'false'
		END                                    AS join_field_mlltv_to_kochava,
            REGEXP_REPLACE(UPPER(CASE WHEN(kds.app_name='PHOENIX') THEN('WORLDWINNER') ELSE(kds.app_name) END),'[^A-Z0-9]','') AS kds_app_name,
            CASE
                WHEN((kds.network_name ilike '%google%') AND (kds.model ilike '%IPAD%')) 
        	THEN 'IPHONE' 
		WHEN(((kds.network_name ilike '%facebook%') or (kds.network_name ilike '%instagram%'))
		AND (kds.model ilike '%IPAD%')) 
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
            END AS kds_platform,
            CASE
		WHEN((kds.network_name ilike '%smartlink%')
		    AND (campaign_id IN ('kosolitairetripeaks179552cdd213d1b1ec8dadb11e078f',
					 'kosolitairetripeaks179552cdd213d1b1eff27d8be17f25',
					 'kosolitaire-tripeaks-ios53a8762dc3a45f7f36992c7065',
					 'kosolitaire-tripeaks-amazon542c346dd4e8d62409345f40e6',
					 'kosolitairetripeaks179552cdd213d1b1e2ed740d2e8bfa',
					 'kosolitairetripeaks179552cdd213d1b1e8c45df91957da',
					 'kosolitaire-tripeaks-ios53a8762dc3a4559d6e0f025582',
					 'kosolitaire-tripeaks-amazon542c346dd4e8d2134a9d909d97')))
		THEN('PLAYANEXTYAHOO')
		WHEN(kds.network_name ilike '%amazon%fire%')
		THEN('AMAZONMEDIAGROUP')
		WHEN(kds.network_name ilike '%wake%screen%')
		THEN('AMAZONMEDIAGROUP')
		WHEN((kds.app_name ilike '%bingo%bash%')
		    AND (kds.network_name ilike '%amazon%')
		    AND (kds.campaign_name ilike '%wakescreen%'))
		THEN('AMAZONMEDIAGROUP')
		WHEN(kds.network_name ilike '%mobilda%')
		THEN('SNAPCHAT')
		WHEN(kds.network_name ilike '%snap%')
		THEN('SNAPCHAT')
		WHEN(kds.network_name ilike '%applovin%')
		THEN('APPLOVIN')
		WHEN(kds.network_name ilike '%bing%')
		THEN('BINGSEARCH')
		WHEN(kds.network_name ilike '%aura%ironsource%')
		THEN('IRONSOURCEPRELOAD')
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
		WHEN(((kds.network_name ilike '%facebook%') or (kds.network_name ilike '%instagram%'))
	    		AND (kds.site_id ilike '%fbcoupon%')
			AND (DATE(kds.install_day) < '2018-12-30'))				   
         	THEN('FACEBOOKCOUPON')
		WHEN((kds.network_name ilike '%facebook%')
            		AND (LOWER(kds.site_id) ilike '%creativetest%'))
        	THEN('CREATIVETESTFB')
		WHEN(kds.network_name ilike '%smartly%')
		THEN('SMARTLY')
		WHEN(kds.network_name ilike '%bidalgo%')
		THEN('BIDALGO')
		WHEN(kds.network_name ilike '%moloco%')
		THEN('MOLOCO')
		WHEN(kds.network_name ilike '%adaction%')
		THEN('ADACTION')
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
		    AND (kds.site_id ilike 'svl%'))
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
		    '- ANDROID|- IOS|- FACEBOOK|- AMAZON|- ANROID|– ANDROID|– IOS|– FACEBOOK|– AMAZON|– ANROID|-ANDROID|-IOS|-FACEBOOK|-AMAZON|-ANROID|–ANDROID|–IOS|–FACEBOOK|–AMAZON|–ANROID'
		    ,''),'[^A-Z0-9]',''))
	    END 				AS kds_network_name,
	    CASE 
		WHEN (kds.network_name='unattributed' 
		      AND ((kds.last_click_network_name ilike '%digital%turbine%') 
			OR (kds.last_click_network_name ilike '%aura%ironsource%'))) 
		THEN true 
		ELSE false 
	    END AS kds_unattributed_preload,
            kds.install_day 			AS kds_install_day,
            COUNT(DISTINCT kds.synthetic_id) 	AS kds_install_cnt,
            COUNT(DISTINCT ml_ltv.synthetic_id)	AS ml_ltv_user_cnt, 
            COUNT(DISTINCT ml_ltv_v4.synthetic_id)	AS ml_ltv_v4_user_cnt, 
	    SUM(ml_ltv.predicted_iap_revenue)	AS ml_ltv_predicted_revenue, 
	    SUM(ml_ltv_v4.predicted_iap_revenue_v4)	AS ml_ltv_v4_predicted_revenue,
            SUM(ml_ltv.predicted_ad_revenue)	AS ml_ltv_predicted_ad_revenue 
         FROM
            gsnmobile.kochava_device_summary kds
        LEFT JOIN
        (
                select 'SOLITAIRE TRIPEAKS' as app, 
                	synthetic_id, 
			install_date, 
			predicted_iap_revenue, 
			predicted_ad_revenue 
                from 
			(select coalesce(i.synthetic_id, a.synthetic_id) as synthetic_id, 
				coalesce(i.install_date, a.install_date) as install_date, 
				coalesce(i.iap_rev,0) as predicted_iap_revenue, 
				coalesce(a.ad_rev,0) as predicted_ad_revenue 
			from 
				(select synthetic_id, 
					install_date, 
					predicted_iap_revenue as iap_rev 
				from tripeaksapp.ltv_predictions_device_latest_by_days_horizon_days_observation_model_version_v
				where synthetic_id not like 'UNMAPPED%'
				    and right(synthetic_id,4) <> '-tmp'
					and days_horizon=360
					and days_observation=7
					and model_version=6) i 
			full outer join 
				(select synthetic_id, 
					install_date, 
					predicted_total_ad_revenue as ad_rev 
				from tripeaksapp.ads_ltv_predictions_device_latest_by_days_horizon_days_observation_model_version_v 
				where synthetic_id not like 'UNMAPPED%'
				    and right(synthetic_id,4) <> '-tmp'
					and days_horizon=360 
					and days_observation=7 
					and model_version=1) a 
			using(synthetic_id) 
			group by 1,2,3,4 
			order by 1) iap_plus_ad_predictions 
                
		UNION ALL

                select 'WOF SLOTS' as app,
                	synthetic_id, 
			install_date, 
			predicted_iap_revenue, 
			0.000 as predicted_ad_revenue   
                from app_wofs.ltv_predictions_device_latest_by_days_horizon_days_observation_model_version_v
                where days_horizon=60
                AND  days_observation=7
                AND  model_version=1
		
		UNION ALL
                
                select 'PHOENIX' as app, 
                	idfv as synthetic_id, 
			install_date, 
			predicted_iap_revenue, 
			0.000 as predicted_ad_revenue   
                from gsnmobile.worldwinner_mobile_ltv_predictions_idfv_latest_by_days_horizon_days_observation_v
                where days_horizon=90
                AND  days_observation=7
                AND  model_version=2
                
         ) ml_ltv 
        ON
            kds.synthetic_id = ml_ltv.synthetic_id
        	AND kds.install_day = ml_ltv.install_date 
		AND kds.app_name = ml_ltv.app
        LEFT JOIN
		(select 'SOLITAIRE TRIPEAKS' as app, 
			synthetic_id, 
			install_date, 
			predicted_iap_revenue as predicted_iap_revenue_v4 
		from tripeaksapp.ltv_predictions_device_latest_by_days_horizon_days_observation_model_version_v
		where synthetic_id not like 'UNMAPPED%'
		    and right(synthetic_id,4) <> '-tmp'
			and days_horizon=360
			and days_observation=7
			and model_version=4
		group by 1,2,3,4
		order by 1,2) ml_ltv_v4 
        ON
            kds.synthetic_id = ml_ltv_v4.synthetic_id
        	AND kds.install_day = ml_ltv_v4.install_date 
		AND kds.app_name = ml_ltv_v4.app
        WHERE kds.app_name in ('WOF SLOTS','SOLITAIRE TRIPEAKS','PHOENIX')
        AND kds.install_day >=  date('{{ ds }}') -8
        GROUP BY 1,2,3,4,5,6) temp
ON
    (
        main.join_field_mlltv_to_kochava = temp.join_field_mlltv_to_kochava)
WHEN MATCHED
    THEN
UPDATE
SET
    kds_app_name =temp.kds_app_name,
    kds_platform= temp.kds_platform,
    kds_network_name =temp.kds_network_name,
    kds_unattributed_preload=temp.kds_unattributed_preload,
    kds_install_day = temp.kds_install_day,
    kds_install_cnt = temp.kds_install_cnt,
    ml_ltv_user_cnt = temp.ml_ltv_user_cnt,
    ml_ltv_v4_user_cnt = temp.ml_ltv_v4_user_cnt,
    ml_ltv_predicted_revenue = temp.ml_ltv_predicted_revenue,
    ml_ltv_v4_predicted_revenue = temp.ml_ltv_v4_predicted_revenue,
    ml_ltv_predicted_ad_revenue = temp.ml_ltv_predicted_ad_revenue 
WHEN NOT MATCHED
    THEN
INSERT
    (
        join_field_mlltv_to_kochava ,
        kds_app_name ,
        kds_platform,
        kds_network_name ,
	kds_unattributed_preload , 
        kds_install_day,
        kds_install_cnt,
        ml_ltv_user_cnt ,
        ml_ltv_v4_user_cnt ,
        ml_ltv_predicted_revenue ,
        ml_ltv_v4_predicted_revenue ,
        ml_ltv_predicted_ad_revenue
    )
    VALUES
    (
        temp.join_field_mlltv_to_kochava ,
        temp.kds_app_name ,
        temp.kds_platform,
        temp.kds_network_name ,
	temp.kds_unattributed_preload ,
        temp.kds_install_day,
        temp.kds_install_cnt,
        temp.ml_ltv_user_cnt ,
        temp.ml_ltv_v4_user_cnt ,
        temp.ml_ltv_predicted_revenue ,
        temp.ml_ltv_v4_predicted_revenue ,
        temp.ml_ltv_predicted_ad_revenue
    );

COMMIT;

-- SELECT PURGE_TABLE('gsnmobile.tableau_ua_ml_LTV');
