# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import sqlparse

from common.hooks.vertica_hook import VerticaHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class VerticaOperator(BaseOperator):
    """
    Executes sql code in a specific Vertica database

    :param vertica_conn_id: reference to a specific Vertica database
    :type vertica_conn_id: string
    :param sql: the sql code to be executed
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#b4e0ff'

    @apply_defaults
    def __init__(self, sql, vertica_conn_id='vertica_default', driver_type='vertica_python', resourcepool=None, *args,
                 **kwargs):
        super(VerticaOperator, self).__init__(*args, **kwargs)
        self.vertica_conn_id = vertica_conn_id
        self.resourcepool = resourcepool
        self.driver_type = driver_type
        self.sql = sql

    def execute(self, context):
        logging.info("vertica_conn_id={self.vertica_conn_id}".format(**locals()))
        logging.info("driver_type={self.driver_type}".format(**locals()))
        logging.info('Executing: ' + str(self.sql))
        hook = VerticaHook(vertica_conn_id=self.vertica_conn_id, resourcepool=self.resourcepool,
                           driver_type=self.driver_type)
        # hook.run([x.strip() for x in self.sql.split(';') if x])
        parsed_sql = sqlparse.split(sqlparse.format(self.sql, strip_comments=False, strip_whitespace=True))
        hook.run(parsed_sql)
