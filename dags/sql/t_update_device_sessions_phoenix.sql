truncate table phoenix.device_sessions_ods;

INSERT /*+ direct,LABEL ('airflow-skill-etl_phoenix_activity-t_update_device_sessions_phoenix')*/ INTO phoenix.device_sessions_ods
SELECT event_day,
       idfv,
       ROW_NUMBER() OVER (partition by event_day,idfv ORDER BY session) AS session,
       start_ts,
       end_ts,
       duration_seconds,
       CASE WHEN lower(device_model) like 'iphone%'
           or lower(device_model) like 'ipad%'
           or lower(device_model) like 'ipod%'
           or lower(device_model) like 'imac%'
           or lower(device_model) like 'macbook%'
           or device_model is null
       then 'ios'
       WHEN lower(device_model) like 'kf%'
           or lower(device_model) like 'kindle%'
       then 'amazon'
       else 'android' end as platform,
       CASE WHEN lower(device_model) like 'iphone%' then 'iphone'
       WHEN lower(device_model) like 'ipad%'  then 'ipad'
       WHEN lower(device_model) like 'ipod%' then 'ipod'
       WHEN lower(device_model) like 'imac%'
           or lower(device_model) like 'macbook%' then 'apple laptop'
       else null end as device_type,
       substr(device_model,1,80) as device_model
FROM (SELECT min(event_day) as event_day,
             idfv,
             SESSION,
             MIN(event_time) AS start_ts,
             MAX(event_time) AS end_ts,
             timestampdiff('s',MIN(event_time),MAX(event_time)) AS duration_seconds,
             MAX(device_model) as device_model
      FROM (SELECT DISTINCT event_day,
                   event_time,
                   idfv,
                   1 + conditional_true_event(event_time - lag (event_time) > '15 minutes') OVER (w) AS SESSION,
                   device_model
            FROM phoenix.events_client
            WHERE event_day >= date(TIMESTAMPADD('hour', -24, TO_TIMESTAMP('{{ execution_date }}', 'YYYY-MM-DD HH:MI:SS') AT TIME ZONE 'UTC' AT TIMEZONE 'America/Los_Angeles'))
            and event_time between
            TIMESTAMPADD('hour', -24, TO_TIMESTAMP('{{ execution_date }}', 'YYYY-MM-DD HH:MI:SS') AT TIME ZONE 'UTC' AT TIMEZONE 'America/Los_Angeles')
            and
            TIMESTAMPADD('hour', 3, TO_TIMESTAMP('{{ execution_date }}', 'YYYY-MM-DD HH:MI:SS') AT TIME ZONE 'UTC' AT TIMEZONE 'America/Los_Angeles') WINDOW w
            AS
            (PARTITION BY idfv
            ORDER BY event_time ASC)) a
      GROUP BY
               2,
               3
      HAVING timestampdiff ('s',MIN(event_time),MAX(event_time)) > 0) b;

update /*+ direct,LABEL ('airflow-skill-etl_phoenix_activity-t_update_device_sessions_phoenix')*/
phoenix.device_sessions tgt
set
        end_ts = src.end_ts,
        duration_s = timestampdiff('s',tgt.start_ts,src.end_ts)
        from phoenix.device_sessions_ods src
where tgt.event_day >= date(TIMESTAMPADD('hour', -24, TO_TIMESTAMP('{{ execution_date }}', 'YYYY-MM-DD HH:MI:SS') AT TIME ZONE 'UTC' AT TIMEZONE 'America/Los_Angeles')) - 2
and tgt.idfv = src.idfv
      and (src.start_ts >= tgt.start_ts and timestampdiff('minute', tgt.end_ts, src.start_ts) < 15)
and (src.end_ts >= tgt.end_ts);



insert /*+ direct,LABEL ('airflow-skill-etl_phoenix_activity-t_update_device_sessions_phoenix')*/  into phoenix.device_sessions
select src.*
from phoenix.device_sessions_ods src
left join phoenix.device_sessions tgt
      on tgt.idfv = src.idfv
      and (src.start_ts >= tgt.start_ts and timestampdiff('minute', tgt.end_ts, src.start_ts) < 15)
      and tgt.event_day >= date(TIMESTAMPADD('hour', -24, TO_TIMESTAMP('{{ execution_date }}', 'YYYY-MM-DD HH:MI:SS') AT TIME ZONE 'UTC' AT TIMEZONE 'America/Los_Angeles')) - 2
and (src.end_ts >= tgt.end_ts)
where tgt.event_day is null;

COMMIT;
-- SELECT PURGE_TABLE('phoenix.device_sessions');


