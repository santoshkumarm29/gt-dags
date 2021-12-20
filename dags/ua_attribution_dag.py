"""DAG that updates UA tables

"""

from airflow import DAG
from common.operators.vertica_operator import VerticaOperator
from common.operators.gsn_sensor import GsnSqlSensor
from datetime import datetime, timedelta
from common.dag_utils import is_airflow


def ua_attribution_dag(parent_dag_name, child_dag_name, start_date, schedule_interval):
    sub_dag_ua_attribution = DAG(
        '%s.%s' % (parent_dag_name, child_dag_name),
        schedule_interval=schedule_interval,
        start_date=start_date,
    )

    # t_validate_client_data = GsnSqlSensor(
    #     task_id='t_validate_client_data',
    #     conn_id='vertica_conn',
    #     resourcepool='default',
    #     sql='sql/ua_attribution_tasks/t_validate_client_data.sql',
    #     sla=timedelta(hours=2),
    #     poke_interval=600,
    #     timeout=7200,
    #     dag=sub_dag_ua_attribution
    # )

    # t_validate_kochava_data = GsnSqlSensor(
    #     task_id='t_validate_kochava_data',
    #     conn_id='vertica_conn',
    #     resourcepool='default',
    #     sql='sql/ua_attribution_tasks/t_validate_kochava_data.sql',
    #     sla=timedelta(hours=1),
    #     poke_interval=600,
    #     timeout=7200,
    #     dag=sub_dag_ua_attribution
    # )

    # t_validate_dim_device_hardware = GsnSqlSensor(
    #     task_id='t_validate_dim_device_hardware',
    #     conn_id='vertica_conn',
    #     resourcepool='default',
    #     sql='sql/ua_attribution_tasks/t_validate_dim_device_hardware.sql',
    #     sla=timedelta(hours=1),
    #     poke_interval=600,
    #     timeout=7200,
    #     dag=sub_dag_ua_attribution
    # )

    t_update_kochava_device_summary = VerticaOperator(
        task_id='t_update_kochava_device_summary',
        vertica_conn_id='vertica_conn',
        resourcepool='default',
        sql='sql/ua_attribution_tasks/t_update_kochava_device_summary.sql',
        dag=sub_dag_ua_attribution
    )

    t_update_ad_roas_kochava = VerticaOperator(
        task_id='t_update_ad_roas_kochava',
        vertica_conn_id='vertica_conn',
        resourcepool='default',
        sql='sql/ua_attribution_tasks/t_update_ad_roas_kochava.sql',
        dag=sub_dag_ua_attribution
    )

    t_update_phoenix_ua = VerticaOperator(
        task_id='t_update_phoenix_ua',
        vertica_conn_id='vertica_conn',
        resourcepool='default',
        sql='sql/ua_attribution_tasks/t_update_phoenix_ua.sql',
        sla=timedelta(hours=0, minutes=45),
        dag=sub_dag_ua_attribution
    )

    # t_validate_dim_device_hardware >> t_update_kochava_device_summary
    # t_validate_kochava_data >> t_update_kochava_device_summary
    # t_validate_client_data >> t_update_kochava_device_summary
    t_update_kochava_device_summary >> t_update_ad_roas_kochava
    t_update_ad_roas_kochava >> t_update_phoenix_ua

    return sub_dag_ua_attribution
