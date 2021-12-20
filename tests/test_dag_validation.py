import pytest
import sys

from airflow.models import DagBag

sys.path.append("../plugins")

def test_no_import_errors():
  dag_bag = DagBag(dag_folder='../dags/', include_examples=False)
  assert len(dag_bag.import_errors) == 0, "No Import Failures"
