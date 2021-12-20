import datetime

import pytest
from airflow import DAG

@pytest.fixture
def test_dag():
    return DAG(
        'test_dag',
        default_args={'owner': 'airflow', 'start_date': datetime.datetime(2018, 1, 1)},
        schedule_interval=datetime.timedelta(days=1),
    )