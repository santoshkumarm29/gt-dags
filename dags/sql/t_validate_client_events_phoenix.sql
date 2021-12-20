select /*+ LABEL ('airflow-skill-worldwinner_gsncom_daily_imports-t_validate_client_events_phoenix')*/
1 as valid
from
phoenix.events_client
where event_day = date('{{ tomorrow_ds }}')
limit 1;
