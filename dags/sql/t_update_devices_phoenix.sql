TRUNCATE TABLE phoenix.devices;

INSERT /*+ direct,LABEL ('airflow-skill-etl_phoenix_activity-t_update_devices_phoenix')*/
INTO phoenix.devices
(
  idfv,
  install_day,
  install_timestamp,
  platform,
  device_type,
  device_model,
  last_active_timestamp
)
SELECT idfv,
       MIN(event_day) AS install_day,
       MIN(first_active_timestamp) AS install_timestamp,
       MAX(platform) as platform,
       MAX(device_type) as device_type,
       MAX(device_model) as device_model,
       MAX(last_active_timestamp) as last_active_timestamp
FROM phoenix.device_day d
--can use this and remove truncate, but trying to get most recent device info
--WHERE event_day >= DATE ('{{ds}}') -1
--AND   NOT EXISTS (SELECT 1 FROM phoenix.devices i WHERE i.idfv = d.idfv)
GROUP BY 1;
