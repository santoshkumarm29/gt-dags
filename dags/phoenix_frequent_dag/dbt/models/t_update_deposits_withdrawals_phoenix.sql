{{
    config(
        materialized='incremental',
        unique_key='event_id',
        alias='deposits_withdrawals'
    )
}}

SELECT
       idfv,
       event_day,
       event_time,
       event_name,
       num_attr1 AS amount,
       'client' AS event_source,
       platform,
       idfa,
       country,
       operating_system as os,
       model,
       hardware,
       device,
       user_id,
       user_name,
       synthetic_id,
       login_type,
       event_id
FROM phoenix.events_client
WHERE event_name IN ('cashWithdrawal','cashDeposit')
{% if is_incremental() %}
  AND event_time >= sysdate - 1
  AND   event_id not in (select event_id from {{ this }})
{% endif %}
