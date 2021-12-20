select drop_partition('airflow.snapshot_responsys_ww_pet_unsub', '{{ ds }}');

insert   /*+ direct,LABEL ('airflow-skill-worldwinner_gsncom_daily_imports-t_prepare_responsys_ww_pet_unsub')*/ INTO airflow.snapshot_responsys_ww_pet_unsub
SELECT a.user_id AS CUSTOMER_ID,
       CASE WHEN a.email ilike '%@deleted.local%'
            THEN a.user_id::VARCHAR || '@donotreply.gsn.com'
            ELSE a.email
       END as email
       ,'{{ ds }}' as ds
FROM   ww.dim_users a
JOIN   ww.dim_user_aux b
using (user_id)
WHERE  a.email NOT ilike '%@donotreply.gsn.com%'
AND    a.user_banned = 0
AND    b.last_login >= date('{{ tomorrow_ds }}') - 365;
