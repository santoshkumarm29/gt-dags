select drop_partition('airflow.snapshot_responsys_ww_pet_universe', '{{ ds }}');

insert   /*+ direct,LABEL ('airflow-skill-worldwinner_gsncom_daily_imports-t_prepare_responsys_ww_pet_universe.')*/ INTO airflow.snapshot_responsys_ww_pet_universe
select c.user_id
,case when UNIVERSE_NEWMEMBER0TO30D=1 then 'Y' else 'N' end as UNIVERSE_NMP
,case when UNIVERSE_SPRITE=1 then 'Y' else 'N' end as UNIVERSE_SPRITE
,case when UNIVERSE_DRPEPPER=1 then 'Y' else 'N' end as UNIVERSE_DRPEPPER
,case when UNIVERSE_ROOTBEER=1 then 'Y' else 'N' end as UNIVERSE_ROOTBEER
,case when UNIVERSE_GINGERALE=1 then 'Y' else 'N' end as UNIVERSE_GINGERALE
,case when UNIVERSE_COKE=1 then 'Y' else 'N' end as UNIVERSE_COKE
,case when UNIVERSE_FRESCA=1 then 'Y' else 'N' end as UNIVERSE_FRESCA
,case when UNIVERSE_7UP=1 then 'Y' else 'N' end as UNIVERSE_7UP
,case when UNIVERSE_FPUEONLY=1 then 'Y' else 'N' end as UNIVERSE_FPUEONLY
,case when UNIVERSE_FANTA=1 then 'Y' else 'N' end as UNIVERSE_FANTA
,case when UNIVERSE_NESTEA=1 then 'Y' else 'N' end as UNIVERSE_NESTEA
,case when UNIVERSE_PEPSI=1 then 'Y' else 'N' end as UNIVERSE_PEPSI
,case when UNIVERSE_MOXIE=1 then 'Y' else 'N' end as UNIVERSE_MOXIE
,case when UNIVERSE_CRUSH=1 then 'Y' else 'N' end as UNIVERSE_CRUSH
,case when UNIVERSE_HAPPYHOUR=1 then 'Y' else 'N' end as UNIVERSE_HAPPYHOUR
,case when UNIVERSE_ROCKSTAR=1 then 'Y' else 'N' end as UNIVERSE_ROCKSTAR
,case when UNIVERSE_PCMEMBER=1 then 'Y' else 'N' end as UNIVERSE_PCMEMBER
,case when UNIVERSE_PCSILVER=1 then 'Y' else 'N' end as UNIVERSE_PCSILVER
,case when UNIVERSE_PCGOLD=1 then 'Y' else 'N' end as UNIVERSE_PCGOLD
,case when UNIVERSE_PCPLATINUM=1 then 'Y' else 'N' end as UNIVERSE_PCPLATINUM
,case when UNIVERSE_GSNPRACTICEPLAYERS=1 then 'Y' else 'N' end as UNIVERSE_GSNPRACTICEPLAYERS
,case when UNIVERSE_GSNCASHPLAYERS=1 then 'Y' else 'N' end as UNIVERSE_GSNCASHPLAYERS
,'{{ ds }}' as ds
from(
SELECT user_id
    ,universe
    ,bitstring
    ,substring(bitstring, 2, 1) AS UNIVERSE_GSNCASHPLAYERS
    ,substring(bitstring, 3, 1) AS UNIVERSE_GSNPRACTICEPLAYERS
    ,substring(bitstring, 4, 1) AS UNIVERSE_PCPLATINUM
    ,substring(bitstring, 5, 1) AS UNIVERSE_PCGOLD
    ,substring(bitstring, 6, 1) AS UNIVERSE_PCSILVER
    ,substring(bitstring, 7, 1) AS UNIVERSE_PCMEMBER
    ,substring(bitstring, 8, 1) AS UNIVERSE_HAPPYHOUR
    ,substring(bitstring, 9, 1) AS UNIVERSE_CRUSH
    ,substring(bitstring,10, 1) AS UNIVERSE_MOXIE
    ,substring(bitstring,11, 1) AS UNIVERSE_NESTEA
    ,substring(bitstring,12, 1) AS UNIVERSE_DRPEPPER
    ,substring(bitstring,13, 1) AS UNIVERSE_ALPHA
    ,substring(bitstring,14, 1) AS UNIVERSE_REMOTEPARTNERSONLY
    ,substring(bitstring,15, 1) AS UNIVERSE_FPUEONLY
    ,substring(bitstring,16, 1) AS UNIVERSE_7UP
    ,substring(bitstring,17, 1) AS UNIVERSE_GINGERALE
    ,substring(bitstring,18, 1) AS UNIVERSE_FANTA
    ,substring(bitstring,19, 1) AS UNIVERSE_ROOTBEER
    ,substring(bitstring,20, 1) AS UNIVERSE_COKE
    ,substring(bitstring,21, 1) AS UNIVERSE_SPRITE
    ,substring(bitstring,22, 1) AS UNIVERSE_PEPSI
    ,substring(bitstring,23, 1) AS UNIVERSE_FRESCA
    ,substring(bitstring,24, 1) AS UNIVERSE_ROCKSTAR
    ,substring(bitstring,25, 1) AS UNIVERSE_PREMIER
    ,substring(bitstring,26, 1) AS UNIVERSE_EMPLOYEESONLY
    ,substring(bitstring,27, 1) AS UNIVERSE_DOMESTIC
    ,substring(bitstring,28, 1) AS UNIVERSE_INTERNATIONAL
    ,substring(bitstring,29, 1) AS UNIVERSE_QA
    ,substring(bitstring,31, 1) AS UNIVERSE_NEWMEMBER0TO30D
    ,substring(bitstring,32, 1) AS UNIVERSE_GUEST
    ,email


FROM (
    SELECT user_id
        ,universe
        ,LPAD(TO_BITSTRING(HEX_TO_BINARY(TO_HEX(universe)))::varchar, 32, '0') AS bitstring
        ,email
    FROM ww.dim_users
    WHERE user_banned = 0
    ) a
)C
left join ww.dim_user_aux b on c.user_id=b.user_id
where last_login>=(date('{{ tomorrow_ds }}')-365)
and c.email NOT ilike '%@deleted.local%'
	AND c.email NOT ilike '%@donotreply.gsn.com%'
	AND c.email NOT IN (
		SELECT DISTINCT email
		FROM ww.email_suppression_list
		)
