"""DAG that updates UA tables

"""

from airflow import DAG

from common.operators.vertica_operator import VerticaOperator
from common.operators.gsn_sensor import GsnSqlSensor
from datetime import timedelta


def phoenix_dag(parent_dag_name, child_dag_name, start_date, schedule_interval):
    sub_dag_phoenix = DAG(
        '%s.%s' % (parent_dag_name, child_dag_name),
        schedule_interval=schedule_interval,
        start_date=start_date,
    )

    t_validate_kochava_data = GsnSqlSensor(
        task_id='t_validate_kochava_data',
        conn_id='vertica_conn',
        resourcepool='default',
        sql='sql/phoenix_tasks/t_validate_kochava_data.sql',
        sla=timedelta(hours=1),
        poke_interval=120,
        timeout=3600,
        retries=2,
        dag=sub_dag_phoenix
    )

    t_validate_deposits_withdrawals = GsnSqlSensor(
        task_id='t_validate_deposits_withdrawals',
        conn_id='vertica_conn',
        resourcepool='default',
        sql='sql/phoenix_tasks/t_validate_deposits_withdrawals.sql',
        sla=timedelta(hours=1),
        poke_interval=120,
        timeout=3600,
        retries=2,
        dag=sub_dag_phoenix
    )

    t_validate_device_day = GsnSqlSensor(
        task_id='t_validate_device_day',
        conn_id='vertica_conn',
        resourcepool='default',
        sql='sql/phoenix_tasks/t_validate_device_day.sql',
        sla=timedelta(hours=1),
        poke_interval=120,
        timeout=3600,
        retries=2,
        dag=sub_dag_phoenix
    )

    t_validate_devices = GsnSqlSensor(
        task_id='t_validate_devices',
        conn_id='vertica_conn',
        resourcepool='default',
        sql='sql/phoenix_tasks/t_validate_devices.sql',
        sla=timedelta(hours=1),
        poke_interval=120,
        timeout=3600,
        retries=2,
        dag=sub_dag_phoenix
    )

    t_validate_nar = GsnSqlSensor(
        task_id='t_validate_nar',
        conn_id='vertica_conn',
        resourcepool='default',
        sql='sql/phoenix_tasks/t_validate_nar.sql',
        sla=timedelta(hours=1),
        poke_interval=120,
        timeout=3600,
        retries=2,
        dag=sub_dag_phoenix
    )

    t_update_kochava_device_summary_phoenix = VerticaOperator(
        task_id='t_update_kochava_device_summary_phoenix',
        vertica_conn_id='vertica_conn',
        resourcepool='default',
        sql='sql/phoenix_tasks/t_update_kochava_device_summary_phoenix.sql',
        dag=sub_dag_phoenix
    )

    t_update_kochava_device_summary_phoenix_extended = VerticaOperator(
        task_id='t_update_kochava_device_summary_phoenix_extended',
        vertica_conn_id='vertica_conn',
        resourcepool='default',
        sql='sql/phoenix_tasks/t_update_kochava_device_summary_phoenix_extended.sql',
        dag=sub_dag_phoenix
    )

    t_update_ad_roas_kochava_phoenix = VerticaOperator(
        task_id='t_update_ad_roas_kochava_phoenix',
        vertica_conn_id='vertica_conn',
        resourcepool='default',
        sql='sql/phoenix_tasks/t_update_ad_roas_kochava_phoenix.sql',
        dag=sub_dag_phoenix
    )

    t_validate_deposits_withdrawals >> t_update_kochava_device_summary_phoenix
    t_validate_nar >> t_update_kochava_device_summary_phoenix
    t_validate_device_day >> t_update_kochava_device_summary_phoenix
    t_validate_kochava_data >> t_update_kochava_device_summary_phoenix
    t_validate_devices >> t_update_kochava_device_summary_phoenix

    t_update_kochava_device_summary_phoenix >> t_update_ad_roas_kochava_phoenix
    t_update_kochava_device_summary_phoenix >> t_update_kochava_device_summary_phoenix_extended


    return sub_dag_phoenix
