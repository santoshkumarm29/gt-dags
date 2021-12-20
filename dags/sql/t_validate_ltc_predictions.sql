select  /*+ LABEL ('airflow-skill-worldwinner_gsncom_daily_imports-t_validate_ltc_predictions')*/ 1 as valid
from ww.ltc_predictions_historical
where predict_date = date('{{ tomorrow_ds }}')
limit 1;