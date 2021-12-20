select drop_partition('airflow.snapshot_responsys_ww_pet_tags', '{{ ds }}');

INSERT   /*+ direct,LABEL ('airflow-skill-worldwinner_gsncom_daily_imports-t_prepare_responsys_ww_pet_tags')*/ INTO airflow.snapshot_responsys_ww_pet_tags
SELECT a.worldwinner_user_id
	,TAG_LAPSED_PCPLAT
	,TAG_LAPSED_PCGOLD_20GC
	,TAG_LAPSED_15GC
	,TAG_LAPSED_5GC
	,TAG_LAPSED_3500RP
	,TAG_NONPC20
	,TAG_NONPC15
	,TAG_NONPC10
	,TAG_NONPC5
	,TAG_SITEWIDE_TEST_NOPROMO
	,TAG_GR_250
	,TAG_GR_500
	,TAG_GR_1000
	,TAG_CASHGAMES_DEPOSIT
	,TAG_POGO_NONDEPOSITOR
	,TAG_INBOXDOLLARS_PP_REG
	,TAG_GSNMONEYPLAYERS
    ,'{{ ds }}' as ds
FROM ww.platform_uid_map a
LEFT JOIN (
	SELECT platform_id
		,MAX(CASE
				WHEN tag = 'Lapsed_PCPlat'
					THEN 'Y'
				ELSE 'N'
				END) AS TAG_LAPSED_PCPLAT
		,MAX(CASE
				WHEN tag = 'LAPSED_PCGOLD_20GC'
					THEN 'Y'
				ELSE 'N'
				END) AS TAG_LAPSED_PCGOLD_20GC
		,MAX(CASE
				WHEN tag = 'LAPSED_15GC'
					THEN 'Y'
				ELSE 'N'
				END) AS TAG_LAPSED_15GC
		,MAX(CASE
				WHEN tag = 'LAPSED_5GC'
					THEN 'Y'
				ELSE 'N'
				END) AS TAG_LAPSED_5GC
		,MAX(CASE
				WHEN tag = 'LAPSED_3500RP'
					THEN 'Y'
				ELSE 'N'
				END) AS TAG_LAPSED_3500RP
		,MAX(CASE
				WHEN tag = 'NONPC20'
					THEN 'Y'
				ELSE 'N'
				END) AS TAG_NONPC20
		,MAX(CASE
				WHEN tag = 'NONPC15'
					THEN 'Y'
				ELSE 'N'
				END) AS TAG_NONPC15
		,MAX(CASE
				WHEN tag = 'NONPC10'
					THEN 'Y'
				ELSE 'N'
				END) AS TAG_NONPC10
		,MAX(CASE
				WHEN tag = 'NONPC5'
					THEN 'Y'
				ELSE 'N'
				END) AS TAG_NONPC5
		,MAX(CASE
				WHEN tag = 'SITEWIDE_TEST_NOPROMO'
					THEN 'Y'
				ELSE 'N'
				END) AS TAG_SITEWIDE_TEST_NOPROMO
		,MAX(CASE
				WHEN tag = 'GR_250'
					THEN 'Y'
				ELSE 'N'
				END) AS TAG_GR_250
		,MAX(CASE
				WHEN tag = 'GR_500'
					THEN 'Y'
				ELSE 'N'
				END) AS TAG_GR_500
		,MAX(CASE
				WHEN tag = 'GR_1000'
					THEN 'Y'
				ELSE 'N'
				END) AS TAG_GR_1000
		,MAX(CASE
				WHEN tag = 'CASHGAMES_DEPOSIT'
					THEN 'Y'
				ELSE 'N'
				END) AS TAG_CASHGAMES_DEPOSIT
		,MAX(CASE
				WHEN tag = 'POGO_NONDEPOSITOR'
					THEN 'Y'
				ELSE 'N'
				END) AS TAG_POGO_NONDEPOSITOR
		,MAX(CASE
				WHEN tag = 'INBOXDOLLARS_PP_REG'
					THEN 'Y'
				ELSE 'N'
				END) AS TAG_INBOXDOLLARS_PP_REG
		,MAX(CASE
				WHEN tag = 'GSNMoneyPlayers'
					THEN 'Y'
				ELSE 'N'
				END) AS TAG_GSNMONEYPLAYERS
	FROM ww.platform_tags
	GROUP BY platform_id
	) b ON a.platform_id = b.platform_id
LEFT JOIN ww.dim_user_aux c ON a.worldwinner_user_id=c.user_id
LEFT JOIN ww.dim_users d ON a.worldwinner_user_id=d.user_id
WHERE worldwinner_user_id IS NOT NULL
    AND c.last_login >= (date('{{ tomorrow_ds }}')-365)
	AND d.email NOT ilike '%@deleted.local%'
	AND d.user_banned = 0
	AND d.email NOT ilike '%@donotreply.gsn.com%'
	AND d.email NOT IN (
		SELECT DISTINCT email
		FROM ww.email_suppression_list
	);
