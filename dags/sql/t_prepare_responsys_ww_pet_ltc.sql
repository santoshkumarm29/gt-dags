select drop_partition('airflow.snapshot_responsys_ww_pet_ltc', '{{ ds }}');


INSERT /*+ direct,LABEL ('airflow-skill-worldwinner_gsncom_daily_imports-t_prepare_responsys_ww_pet_ltc')*/
INTO airflow.snapshot_responsys_ww_pet_ltc
select distinct
    u.user_id as customer_id
	, case when l.category = 'low' then 'Y' else 'N' end as LOW_LTC
	, case when l.category = 'medium' then 'Y' else 'N' end as MED_LTC
	, case when l.category = 'high' then 'Y' else 'N' end as HIGH_LTC
	, '{{ ds }}' as ds
from ww.ltc_predictions_veteran_latest_by_predict_date_v l
join ww.dim_users u on l.user_id = u.user_id
JOIN ww.dim_user_aux y ON l.user_id = y.user_id
where u.user_banned_date is null --exclude banned users
and l.predict_date = date('{{ tomorrow_ds }}')
and y.last_login >= (date('{{ tomorrow_ds }}')-365)
and u.user_banned = 0;

