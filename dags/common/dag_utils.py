import logging
import re
from os import environ

import airflow.settings
from airflow.models import DagModel

_gsn_airflowc_service = 'airflowc-service'
_gsn_airflowb_service = 'data-products-airflowb-service'

def pause_dags(regex_string, *args, **kwargs):
    return set_dags_pause(regex_string, True)


def unpause_dags(regex_string, *args, **kwargs):
    return set_dags_pause(regex_string, False)


def set_dags_pause(regex_string, pause=True):
    logging.info("set_dags_pause matching regex_string={regex_string},pause={pause}".format(**locals()))
    regex = re.compile(regex_string)

    session = airflow.settings.Session()
    dags = session.query(DagModel).all()

    for dag in dags:
        if regex.match(dag.dag_id):
            logging.info("pausing dag={dag}".format(**locals()))
            dag.is_paused = pause

    session.commit()

def is_airflow():
    if 'SERVICE_NAME' in environ and environ['SERVICE_NAME'] == _gsn_airflowc_service:
   # if environ['SERVICE_NAME'] == _gsn_airflowc_service:
        return True
    else:
        return False

def which_airflow():
    if is_airflow():
        return _gsn_airflowc_service
    else:
        return _gsn_airflowb_service
