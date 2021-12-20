import pytest
from datetime import datetime
from airflow.models import TaskInstance
from airflow.hooks.base_hook import BaseHook
from airflow.models import Connection
from ..operators.vertica_operator import VerticaOperator

from vertica_python import Connection as v_conn

DEFAULT_DATE = datetime(2021, 1, 1)

def test_success(test_dag, mocker):
  """Tests that the operator works succesfully"""

  mocker.patch.object(
        BaseHook,
        "get_connection",
        return_value=Connection(schema="tcp", host="localhost:8080"),
  )

  def mock_conn():
    return v_conn

  def mock_response(parsed_sql,any):
    return '1'

  mocker.patch('hooks.vertica_hook.VerticaHook.get_conn',mock_conn)
  mocker.patch('hooks.vertica_hook.VerticaHook.run',mock_response)

  task = VerticaOperator(task_id='t_update_kochava_device_summary',
    vertica_conn_id='any_conn',
    resourcepool='default',
    sql='sql/ua_attribution_tasks/t_update_kochava_device_summary.sql',
    dag=test_dag)
  ti = TaskInstance(task=task, execution_date=datetime.now())
  result = task.execute(None)
  assert result is None