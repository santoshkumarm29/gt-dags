from airflow import DAG
from common.operators.vertica_operator import VerticaOperator
from datetime import datetime, timedelta
from common.operators.gsn_sensor import GsnSqlSensor

def tableau_dag(parent_dag_name, child_dag_name, start_date, schedule_interval):
    sub_dag_tableau = DAG(
        '%s.%s' % (parent_dag_name, child_dag_name),
        schedule_interval=schedule_interval,
        start_date=start_date,
    )

    t_validate_singular_campaign_stats = GsnSqlSensor(
        task_id='t_validate_singular_campaign_stats',
        conn_id='vertica_conn',
        resourcepool='default',
        sql='sql/tableau_staging_tasks/t_validate_singular_campaign_stats.sql',
        sla=timedelta(hours=1),
        poke_interval=600,
        timeout=3600,
        dag=sub_dag_tableau
    )

    t_tableau_ua_ml_ltv = VerticaOperator(
        task_id='t_tableau_ua_ml_ltv',
        vertica_conn_id='vertica_conn',
        resourcepool='default',
        sql='sql/tableau_staging_tasks/t_update_tableau_ua_ml_ltv.sql',
        sla=timedelta(minutes=45),
        dag=sub_dag_tableau
    )

    t_update_tableau_ua_gsn_daily_spend = VerticaOperator(
        task_id='t_update_tableau_ua_gsn_daily_spend',
        vertica_conn_id='vertica_conn',
        resourcepool='default',
        sql='sql/tableau_staging_tasks/t_update_tableau_ua_gsn_daily_spend.sql',
        sla=timedelta(minutes=45),
        dag=sub_dag_tableau
    )

    t_update_tableau_ua_kochava = VerticaOperator(
        task_id='t_update_tableau_ua_kochava',
        vertica_conn_id='vertica_conn',
        resourcepool='default',
        sql='sql/tableau_staging_tasks/t_update_tableau_ua_kochava.sql',
        sla=timedelta(minutes=45),
        dag=sub_dag_tableau
    )

    t_update_tableau_ua_payer_retention = VerticaOperator(
        task_id='t_update_tableau_ua_payer_retention',
        vertica_conn_id='vertica_conn',
        resourcepool='default',
        sql='sql/tableau_staging_tasks/t_update_tableau_ua_payer_retention.sql',
        sla=timedelta(minutes=45),
        dag=sub_dag_tableau
    )

    t_update_tableau_us_singular_data = VerticaOperator(
        task_id='t_update_tableau_us_singular_data',
        vertica_conn_id='vertica_conn',
        resourcepool='default',
        sql='sql/tableau_staging_tasks/t_update_tableau_us_singular_data.sql',
        sla=timedelta(minutes=45),
        dag=sub_dag_tableau
    )
    t_validate_singular_campaign_stats >> t_tableau_ua_ml_ltv
    t_validate_singular_campaign_stats >> t_update_tableau_ua_gsn_daily_spend
    t_validate_singular_campaign_stats >> t_update_tableau_ua_kochava
    t_validate_singular_campaign_stats >> t_update_tableau_ua_payer_retention
    t_validate_singular_campaign_stats >> t_update_tableau_us_singular_data

    return sub_dag_tableau
