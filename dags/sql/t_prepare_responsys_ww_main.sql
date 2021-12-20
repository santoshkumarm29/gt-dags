select drop_partition('airflow.snapshot_responsys_ww_main', '{{ ds }}');

INSERT   /*+ direct,LABEL ('airflow-skill-worldwinner_gsncom_daily_imports-t_prepare_responsys_ww_main')*/ INTO airflow.snapshot_responsys_ww_main
SELECT a.user_id AS CUSTOMER_ID_
	--Do not reply email for deleted users, so Responsys doesn't kick as invalid email address
	,CASE
		WHEN a.email ilike '%@deleted.local%'
			THEN a.user_id::VARCHAR || '@donotreply.gsn.com'
		ELSE a.email
		END AS EMAIL_ADDRESS_
	,'' AS 'EMAIL_FORMAT_'
	-- Deleted accounts need to be sent over to be suppressed, not filtered
	,CASE
		WHEN (email_tournament = 'Y' or email_win_notify = 'Y' or email_promo = 'Y' or email_in_chal = 'Y' or email_in_survive = 'Y' or email_transactional = 'Y')
      and a.email NOT ilike '%@deleted.local%' and a.user_banned = 0
			THEN 'I'
		ELSE 'O'
		END AS 'EMAIL_PERMISSION_STATUS_'
	,'' AS 'MOBILE_NUMBER_'
	,'' AS 'MOBILE_COUNTRY_'
	,'O' AS 'MOBILE_PERMISSION_STATUS_'
	,a.address1 AS 'POSTAL_STREET_1_'
	,a.address2 AS 'POSTAL_STREET_2_'
	,case when isutf8(a.city) then a.city else null end AS 'CITY_'
	,case when isutf8(a.STATE) then a.state else null end AS 'STATE_'
	,a.zip AS 'POSTAL_CODE_'
	,a.country AS 'COuNTRY_'
	,'' AS 'POSTAL_PERMISSION_STATUS_'
	,a.firstname AS 'FIRST_NAME'
	,a.lastname AS 'LAST_NAME'
	,CASE WHEN split_part(g.birthdate, '-', 1) = '0000' or split_part(g.birthdate, '-', 2) = '00' or split_part(g.birthdate, '-', 3) = '00' THEN ''
	 ELSE g.birthdate END AS 'BIRTHDATE'
	,'' AS 'WARMUP'
	,a.cobrand_id AS 'COBRAND_ID'
	,a.advertiser_id AS 'ADVERTISER_ID'
	,a.username AS 'USER_NAME_CASH'
	,null AS 'USER_NAME_CASINO'
	,gsn_user_id AS 'USER_ID_GSN'
	,platform_id AS 'PLATFORM_ID'
	,email_tournament AS 'CGWEB_OPT_IN_TOURN'
	,email_win_notify AS 'CGWEB_OPT_IN_WIN_NOTIFY'
	,email_promo AS 'CGWEB_OPT_IN_PROMO'
	,email_in_chal AS 'CGWEB_OPT_IN_CHAL'
	,email_in_survive AS 'CGWEB_OPT_IN_SURVIVE'
	,'' AS 'CC_OPT_IN'
    , '{{ ds }}' as ds
FROM ww.dim_users a
LEFT JOIN ww.platform_uid_map e ON a.user_id = e.worldwinner_user_id
LEFT JOIN ww.dim_profiles_private g ON a.user_id = g.user_id
LEFT JOIN (
	SELECT user_id
		,CASE
			WHEN sum(email_tournament) = 1
				THEN 'Y'
			ELSE 'N'
			END AS email_tournament
		,CASE
			WHEN sum(email_win_notify) = 1
				THEN 'Y'
			ELSE 'N'
			END AS email_win_notify
		,CASE
			WHEN sum(email_promo) = 1
				THEN 'Y'
			ELSE 'N'
			END AS email_promo
		,CASE
			WHEN sum(email_in_chal) = 1
				THEN 'Y'
			ELSE 'N'
			END AS email_in_chal
		,CASE
			WHEN sum(email_in_survive) = 1
				THEN 'Y'
			ELSE 'N'
			END AS email_in_survive
		,CASE
			WHEN sum(email_transactional) = 1
				THEN 'Y'
			ELSE 'N'
			END AS email_transactional
	FROM (
		SELECT DISTINCT user_id
			,CASE
				WHEN (
						category_id = 1
						AND value = 1
						)
					THEN 1
				ELSE NULL
				END AS email_tournament
			,CASE
				WHEN (
						category_id = 2
						AND value = 1
						)
					THEN 1
				ELSE NULL
				END AS email_win_notify
			,CASE
				WHEN (
						category_id = 3
						AND value = 1
						)
					THEN 1
				ELSE NULL
				END AS email_promo
			,CASE
				WHEN (
						category_id = 4
						AND value = 1
						)
					THEN 1
				ELSE NULL
				END AS email_in_chal
			,CASE
				WHEN (
						category_id = 5
						AND value = 1
						)
					THEN 1
				ELSE NULL
				END AS email_in_survive
			,CASE
				WHEN (
						category_id = 6
						AND value = 1
						)
					THEN 1
				ELSE NULL
				END AS email_transactional
		FROM ww.email_preferences
		ORDER BY 1
			,2
		) a
	GROUP BY 1
	) jjj ON a.user_id = jjj.user_id
LEFT JOIN ww.dim_user_aux h ON a.user_id = h.user_id
WHERE
	a.email NOT ilike '%@donotreply.gsn.com%'
	AND h.last_login >= (date('{{ tomorrow_ds }}')-365);

