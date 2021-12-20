INSERT /*+direct,LABEL ('airflow-skill-etl_phoenix_frequent_jobs-t_update_dim_device_mapping')*/ INTO phoenix.dim_device_mapping_raw
SELECT *
FROM (
  SELECT MIN(event_time) AS event_time,
         idfv,
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
FROM phoenix.dim_device_mapping_raw);

INSERT /*+direct,LABEL ('airflow-skill-etl_phoenix_frequent_jobs-t_update_dim_device_mapping')*/
INTO phoenix.dim_device_mapping
SELECT  *
FROM (SELECT idfv,
             user_id AS id,
             'user_id' AS id_type,
             MIN(event_time) AS event_time
      FROM phoenix.dim_device_mapping_raw
      WHERE user_id <> 'N/A'
      AND   idfv <> 'N/A'
      AND   DATE (event_time) >= DATE (SYSDATE () -1)
      GROUP BY 1,
               2
      UNION ALL
      SELECT idfv,
             idfa AS id,
             'idfa' AS id_type,
             MIN(event_time) AS event_time
      FROM phoenix.dim_device_mapping_raw
      WHERE idfa <> 'N/A'
      AND   idfv <> 'N/A'
      AND   DATE (event_time) >= DATE (SYSDATE () -1)
      GROUP BY 1,
               2
      UNION ALL
      SELECT idfv,
             android_id AS id,
             'android_id' AS id_type,
             MIN(event_time) AS event_time
      FROM phoenix.dim_device_mapping_raw
      WHERE android_id <> 'N/A'
      AND   idfv <> 'N/A'
      AND   DATE (event_time) >= DATE (SYSDATE () -1)
      GROUP BY 1,
               2
      UNION ALL
      SELECT idfv,
             install_id AS id,
             'install_id' AS id_type,
             MIN(event_time) AS event_time
      FROM phoenix.dim_device_mapping_raw
      WHERE install_id <> 'N/A'
      AND   idfv <> 'N/A'
      AND   DATE (event_time) >= DATE (SYSDATE () -1)
      GROUP BY 1,
               2
      UNION ALL
      SELECT idfv,
             synthetic_id AS id,
             'synthetic_id' AS id_type,
             MIN(event_time) AS event_time
      FROM phoenix.dim_device_mapping_raw
      WHERE synthetic_id <> 'N/A'
      AND   idfv <> 'N/A'
      AND   DATE (event_time) >= DATE (SYSDATE () -1)
      GROUP BY 1,
               2) x
WHERE (idfv,id,id_type) NOT IN (SELECT idfv, id, id_type FROM phoenix.dim_device_mapping);