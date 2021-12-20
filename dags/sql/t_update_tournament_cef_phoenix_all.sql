DROP TABLE if exists phoenix.tournament_cef_ods;

CREATE TABLE phoenix.tournament_cef_ods
AS
SELECT /*+ LABEL ('airflow-skill-etl_phoenix_tournaments-t_update_tournament_cef_phoenix_all')*/
       event_day,
       user_id,
       tournament_id,
       cef,
       int_trans_id,
       event_time,
       ww_platform,
       idfv,
       platform,
       idfa,
       country,
       os,
       model,
       hardware,
       device,
       user_name,
       synthetic_id,
       login_type from
(SELECT b.event_day,
       b.user_id,
       product_id AS tournament_id,
       cef,
       int_trans_id,
       b.event_time,
       case when b.device is null then 'desktop'
        when b.device is not null and (b.platform not in ('skill-app','skill-solrush') or b.platform is null) then 'moweb'
        when b.device is not null and b.platform in ('skill-app','skill-solrush') then 'app'
        else 'unknown' end as ww_platform,
       nvl (a.idfv,d.idfv) AS idfv,
       a.platform AS platform,
       idfa AS idfa,
       country AS country,
       os AS os,
       model AS model,
       hardware AS hardware,
       a.device AS device,
       user_name AS user_name,
       synthetic_id AS synthetic_id,
       login_type AS login_type,
       ROW_NUMBER() over (PARTITION BY b.user_id, int_trans_id order by d.last_active_timestamp desc) as rn
FROM (SELECT user_id,
             int_trans_id,
             product_id,
             trans_date AT TIME ZONE 'America/New_York' AT TIME ZONE 'America/Los_Angeles' AS event_time,
             DATE (trans_date AT TIME ZONE 'America/New_York' AT TIME ZONE 'America/Los_Angeles') AS event_day,
             amount*-1 AS cef,
             device,
             platform
      FROM ww.internal_transactions a
        LEFT JOIN ww.clid_combos_ext b ON a.client_id = b.client_id
      WHERE trans_type = 'SIGNUP'
      AND   DATE (trans_date AT TIME ZONE 'America/New_York' AT TIME ZONE 'America/Los_Angeles') between date('{{ ds }}') - 3 and date('{{ ds }}') + 1) b
  LEFT OUTER JOIN (SELECT event_day,
                          idfv,
                          user_id::INT AS user_id,
                          event_time,
                          platform,
                          idfa,
                          country,
                          operating_system as os,
                          model,
                          hardware,
                          device,
                          user_name,
                          synthetic_id,
                          login_type
                   FROM phoenix.events_client
                   WHERE event_day  between date('{{ ds }}') - 4 and date('{{ ds }}') + 1) a
               ON a.user_id = b.user_id
              AND (a.event_time interpolate previous value b.event_time)
            JOIN
            phoenix.dim_device_mapping z
            on b.user_id = z.id
            and z.id_type = 'user_id'
            and b.event_time >= z.event_time
            JOIN phoenix.devices d
            on z.idfv = d.idfv) x
           where rn = 1
ORDER BY event_day SEGMENTED BY hash(tournament_id,event_day) ALL NODES;

DELETE/*+ direct,LABEL ('airflow-skill-etl_phoenix_tournaments-t_update_tournament_cef_phoenix_all')*/
FROM phoenix.tournament_cef
WHERE event_day >= date('{{ds}}') -3;

INSERT /*+ direct,LABEL ('airflow-skill-etl_phoenix_tournaments-t_update_tournament_cef_phoenix_all')*/INTO phoenix.tournament_cef
SELECT *
FROM phoenix.tournament_cef_ods
WHERE event_day >= date('{{ds}}') -3;

COMMIT;

DROP TABLE phoenix.tournament_cef_ods CASCADE;

-- SELECT PURGE_TABLE('phoenix.tournament_cef');

