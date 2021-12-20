{{
    config(
        materialized='incremental',
        alias='dim_device_mapping',
        incremental_strategy='delete+insert'
    )
}}

SELECT  *
FROM (SELECT idfv,
             user_id AS id,
             'user_id' AS id_type,
             MIN(event_time) AS event_time
      FROM {{ ref('t_update_dim_device_mapping_raw') }}
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
      FROM {{ ref('t_update_dim_device_mapping_raw') }}
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
      FROM {{ ref('t_update_dim_device_mapping_raw') }}
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
      FROM {{ ref('t_update_dim_device_mapping_raw') }}
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
      FROM {{ ref('t_update_dim_device_mapping_raw') }}
      WHERE synthetic_id <> 'N/A'
      AND   idfv <> 'N/A'
      AND   DATE (event_time) >= DATE (SYSDATE () -1)
      GROUP BY 1,
               2) x
WHERE (idfv,id,id_type) NOT IN (SELECT idfv, id, id_type FROM {{this}})