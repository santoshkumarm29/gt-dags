DROP TABLE if exists phoenix.tournament_entry_nar_ods;

CREATE TABLE phoenix.tournament_entry_nar_ods
AS
SELECT /*+ LABEL ('airflow-skill-etl_phoenix_tournaments-t_update_tournament_winnings_phoenix')*/tournament_close_day,
       event_day AS tournament_entry_day,
       e.user_id,
       w.tournament_id,
       e.cef,
       w.total_tournament_winnings,
       w.total_tournament_entries,
       w.total_tournament_winnings / w.total_tournament_entries AS avg_winnings_per_entry,
       e.cef -(w.total_tournament_winnings / w.total_tournament_entries) AS entry_nar,
       e.int_trans_id,
       tournament_close_time,
       event_time AS tournament_entry_time,
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
       login_type
FROM (SELECT product_id AS tournament_id,
             SUM(CASE WHEN trans_type = 'WINNINGS' THEN amount ELSE 0 END) AS total_tournament_winnings,
             COUNT(DISTINCT CASE WHEN trans_type = 'SIGNUP' THEN int_trans_id ELSE NULL END) AS total_tournament_entries,
             MAX(case when trans_type = 'WINNINGS' then trans_date AT TIME ZONE 'America/New_York' AT TIME ZONE 'America/Los_Angeles' else null end) AS tournament_close_time,
             MAX(case when trans_type = 'WINNINGS' then DATE (trans_date AT TIME ZONE 'America/New_York' AT TIME ZONE 'America/Los_Angeles') else null end) AS tournament_close_day
      FROM ww.internal_transactions
      WHERE trans_type IN ('SIGNUP','WINNINGS')
      AND   trans_date >= DATE ('{{ds}}') - 30
      GROUP BY 1) w
  JOIN phoenix.tournament_cef e ON w.tournament_id = e.tournament_id
  and e.event_day >= DATE ('{{ds}}') - 30
  where tournament_close_day is not null
  and ww_platform = 'app'
ORDER BY tournament_close_day SEGMENTED BY hash(tournament_id,tournament_close_day) ALL NODES;

DELETE/*+ direct,LABEL ('airflow-skill-etl_phoenix_tournaments-t_update_tournament_winnings_phoenix')*/
FROM phoenix.tournament_entry_nar
WHERE tournament_close_day >= date('{{ds}}') -7;

INSERT /*+ direct,LABEL ('airflow-skill-etl_phoenix_tournaments-t_update_tournament_winnings_phoenix')*/INTO phoenix.tournament_entry_nar
SELECT *
FROM phoenix.tournament_entry_nar_ods
WHERE tournament_close_day >= date('{{ds}}') -7;

COMMIT;

DROP TABLE phoenix.tournament_entry_nar_ods;

-- SELECT PURGE_TABLE('phoenix.tournament_entry_nar');