INSERT /*+ direct,LABEL ('airflow-skill-etl_phoenix_frequent_jobs-t_update_deposits_withdrawals_phoenix')*/
INTO phoenix.deposits_withdrawals
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
       operating_system,
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
--AND   event_time >= (SELECT ifnull(MAX(event_time),'2017-07-11') - 7
--                    FROM phoenix.deposits_withdrawals)
AND event_time >= sysdate - 1
AND   event_id not in (select event_id from phoenix.deposits_withdrawals);
