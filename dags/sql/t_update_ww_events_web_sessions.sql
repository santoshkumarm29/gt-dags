---events ww sessions
---turn ww.played_games into sessions
drop table if exists ww.events_web_sessions_ods;



select /*+ direct,LABEL ('airflow-skill-ww_events_web_sessions-t_update_ww_events_web_sessions')*/
  user_id,
  session,
  min(event_time) as start_time,
  max(event_time) as end_time,
  timestampdiff('second',min(event_time),max(event_time)) as duration_seconds
into ww.events_web_sessions_ods
from (
    select
      user_id,
      start_time as event_time,
      1+conditional_true_event(start_time - lag(start_time) > '20 minutes')
        over(partition by user_id order by start_time asc) as session
    from ww.played_games
    where start_time >= '2013-01-01'
) x
group by 1,2;

truncate table ww.events_web_sessions;

insert /*+ direct,LABEL ('airflow-skill-ww_events_web_sessions-t_update_ww_events_web_sessions')*/ into ww.events_web_sessions
select * from ww.events_web_sessions_ods;

drop table if exists ww.events_web_sessions_ods;