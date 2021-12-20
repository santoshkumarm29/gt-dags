DROP TABLE if exists phoenix.device_day_ods;

CREATE TABLE phoenix.device_day_ods 
(
  event_day                DATE,
  idfv                     VARCHAR(80),
  first_active_timestamp   datetime,
  last_active_timestamp    datetime,
  sessions                 INT,
  play_duration_s          INT,
  platform     VARCHAR(10),
  device_type  VARCHAR(24),
  device_model  VARCHAR(80)
)
ORDER BY idfv,
         event_day;

INSERT /*+ direct,LABEL ('airflow-skill-etl_phoenix_activity-t_update_device_day_phoenix')*/INTO phoenix.device_day_ods
SELECT event_day,
       idfv,
       MIN(start_ts) AS first_active_timestamp,
       MAX(end_ts) AS last_active_timestamp,
       COUNT(DISTINCT session) AS sessions,
       SUM(duration_s) AS play_duration_s,
       MAX(platform) as platform,
       MAX(device_type) as device_type,
       MAX(device_model) as device_model
FROM phoenix.device_sessions
WHERE event_day BETWEEN DATE ('{{ds}}') - 1 AND DATE ('{{ds}}')
GROUP BY 1,
         2;

DELETE/*+ direct,LABEL ('airflow-skill-etl_phoenix_activity-t_update_device_day_phoenix')*/
FROM phoenix.device_day
WHERE event_day BETWEEN DATE ('{{ds}}') -1 AND DATE ('{{ds}}');

INSERT /*+ direct,LABEL ('airflow-skill-etl_phoenix_activity-t_update_device_day_phoenix')*/ INTO phoenix.device_day
SELECT *
FROM phoenix.device_day_ods;

COMMIT;

DROP TABLE phoenix.device_day_ods;

-- SELECT PURGE_TABLE('phoenix.device_day');

