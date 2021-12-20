{{
    config(
        materialized='incremental',
        alias='dim_device_mapping_raw',
        incremental_strategy='delete+insert'
    )
}}

SELECT *
FROM (
  SELECT MIN(event_time) AS event_time,
         nvl(idfv,'N/A') as idfv,
         nvl(user_id,'N/A') AS user_id,
         nvl(idfa,'N/A') AS idfa,
         nvl(android_id,'N/A') AS android_id,
         nvl(install_id,'N/A') AS install_id,
         nvl(synthetic_id,'N/A') AS synthetic_id
  FROM phoenix.events_client
  WHERE event_day >= date(sysdate()-1)
  GROUP BY 2,
           3,
           4,
           5,
           6,
           7) x
WHERE (idfv,
nvl (user_id,'N/A'),
nvl (idfa,'N/A'),
nvl(android_id,'N/A'),
nvl(install_id,'N/A'),
nvl(synthetic_id,'N/A'))
NOT IN
(SELECT idfv,
       user_id,
       idfa,
       android_id,
       install_id,
       synthetic_id
FROM {{this}})