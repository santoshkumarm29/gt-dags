select drop_partition('airflow.snapshot_responsys_ww_pet_universe2', '{{ ds }}');

insert   /*+ direct,LABEL ('airflow-skill-worldwinner_gsncom_daily_imports-t_prepare_responsys_ww_pet_universe2')*/
INTO airflow.snapshot_responsys_ww_pet_universe2
select c.user_id
,case WHEN UNIVERSE_GUEST=1 then 'Y' else 'N' end as UNIVERSE_GUEST
,case WHEN UNIVERSE_NEWMEMBERLT50GAMES=1 then 'Y' else 'N' end as UNIVERSE_NMP
,case WHEN UNIVERSE_ADHOC7=1 then 'Y' else 'N' end as UNIVERSE_ADHOC7
,case WHEN UNIVERSE_QA=1 then 'Y' else 'N' end as UNIVERSE_QA
,case WHEN UNIVERSE_INTERNATIONAL=1 then 'Y' else 'N' end as UNIVERSE_INTERNATIONAL
,case WHEN UNIVERSE_DOMESTIC=1 then 'Y' else 'N' end as UNIVERSE_DOMESTIC
,case WHEN UNIVERSE_EMPLOYEESONLY=1 then 'Y' else 'N' end as UNIVERSE_EMPLOYEESONLY
,case WHEN UNIVERSE_PREMIER=1 then 'Y' else 'N' end as UNIVERSE_PREMIER
,case WHEN UNIVERSE_MODERN=1 then 'Y' else 'N' end as UNIVERSE_MODERN
,case WHEN UNIVERSE_LEGACY=1 then 'Y' else 'N' end as UNIVERSE_LEGACY
,case WHEN UNIVERSE_WEBACQUIRED=1 then 'Y' else 'N' end as UNIVERSE_WEBACQUIRED
,case WHEN UNIVERSE_APPACQUIRED=1 then 'Y' else 'N' end as UNIVERSE_APPACQUIRED
,case WHEN UNIVERSE_PLAYSWEBANDAPP=1 then 'Y' else 'N' end as UNIVERSE_PLAYSWEBANDAPP
,case WHEN UNIVERSE_BETA=1 then 'Y' else 'N' end as UNIVERSE_BETA
,case WHEN UNIVERSE_PAB=1 then 'Y' else 'N' end as UNIVERSE_PAB
,case WHEN UNIVERSE_ADHOC1=1 then 'Y' else 'N' end as UNIVERSE_ADHOC1
,case WHEN UNIVERSE_ADHOC2=1 then 'Y' else 'N' end as UNIVERSE_ADHOC2
,case WHEN UNIVERSE_MIDDLE=1 then 'Y' else 'N' end as UNIVERSE_MIDDLE
,case WHEN UNIVERSE_ADHOC3=1 then 'Y' else 'N' end as UNIVERSE_ADHOC3
,case WHEN UNIVERSE_ADHOC4=1 then 'Y' else 'N' end as UNIVERSE_ADHOC4
,case WHEN UNIVERSE_ADHOC5=1 then 'Y' else 'N' end as UNIVERSE_ADHOC5
,case WHEN UNIVERSE_ADHOC6=1 then 'Y' else 'N' end as UNIVERSE_ADHOC6
,case WHEN UNIVERSE_LAPSEDCASH=1 then 'Y' else 'N' end as UNIVERSE_LAPSEDCASH
,case WHEN UNIVERSE_PCMEMBER=1 then 'Y' else 'N' end as UNIVERSE_PCMEMBER
,case WHEN UNIVERSE_PCSILVER=1 then 'Y' else 'N' end as UNIVERSE_PCSILVER
,case WHEN UNIVERSE_PCGOLD=1 then 'Y' else 'N' end as UNIVERSE_PCGOLD
,case WHEN UNIVERSE_PCPLATINUM=1 then 'Y' else 'N' end as UNIVERSE_PCPLATINUM
,case WHEN UNIVERSE_GSNPRACTICEPLAYERS=1 then 'Y' else 'N' end as UNIVERSE_GSNPRACTICEPLAYERS
,case WHEN UNIVERSE_GSNCASHPLAYERS=1 then 'Y' else 'N' end as UNIVERSE_GSNCASHPLAYERS
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
    ,substring(bitstring, 8, 1) AS UNIVERSE_LAPSEDCASH
    ,substring(bitstring, 9, 1) AS UNIVERSE_ADHOC6
    ,substring(bitstring,10, 1) AS UNIVERSE_ADHOC5
    ,substring(bitstring,11, 1) AS UNIVERSE_ADHOC4
    ,substring(bitstring,12, 1) AS UNIVERSE_ADHOC3
    ,substring(bitstring,15, 1) AS UNIVERSE_MIDDLE
    ,substring(bitstring,16, 1) AS UNIVERSE_ADHOC2
    ,substring(bitstring,17, 1) AS UNIVERSE_ADHOC1
    ,substring(bitstring,18, 1) AS UNIVERSE_PAB
    ,substring(bitstring,19, 1) AS UNIVERSE_BETA
    ,substring(bitstring,20, 1) AS UNIVERSE_PLAYSWEBANDAPP
    ,substring(bitstring,21, 1) AS UNIVERSE_APPACQUIRED
    ,substring(bitstring,22, 1) AS UNIVERSE_WEBACQUIRED
    ,substring(bitstring,23, 1) AS UNIVERSE_LEGACY
    ,substring(bitstring,24, 1) AS UNIVERSE_MODERN
    ,substring(bitstring,25, 1) AS UNIVERSE_PREMIER
    ,substring(bitstring,26, 1) AS UNIVERSE_EMPLOYEESONLY
    ,substring(bitstring,27, 1) AS UNIVERSE_DOMESTIC
    ,substring(bitstring,28, 1) AS UNIVERSE_INTERNATIONAL
    ,substring(bitstring,29, 1) AS UNIVERSE_QA
    ,substring(bitstring,30, 1) AS UNIVERSE_ADHOC7
    ,substring(bitstring,31, 1) AS UNIVERSE_NEWMEMBERLT50GAMES
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
