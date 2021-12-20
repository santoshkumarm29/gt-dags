DELETE /*+ direct,LABEL ('airflow-skill-etl_phoenix_activity-t_update_user_day')*/
from phoenix.user_day where event_day = date('{{ ds }}');


INSERT /*+ direct,LABEL ('airflow-skill-etl_phoenix_activity-t_update_user_day')*/ INTO phoenix.user_day
SELECT
event_day,
user_id,
min(event_time) as start_ts,
max(event_time) as end_ts
from
phoenix.events_client
where user_id is not null
and event_day = date('{{ ds }}')
group by 1,2;