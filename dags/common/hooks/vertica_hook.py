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
#

from airflow.hooks.dbapi_hook import DbApiHook

import logging
import ssl


class VerticaHook(DbApiHook):
    '''
    Interact with Vertica.
    '''

    conn_name_attr = 'vertica_conn_id'
    default_conn_name = 'vertica_default'
    supports_autocommit = True

    def __init__(self, resourcepool=None, runtimecap_seconds=None, *args, **kwargs):
        # flag to determine which driver to use, ('pyodbc', 'vertica_python')
        self.resourcepool = resourcepool
        self.runtimecap_seconds = runtimecap_seconds
        self.driver_type = kwargs.get('driver_type', 'vertica_python')
        logging.getLogger().info('VerticaHook(driver_type="{}")'.format(self.driver_type))
        if self.driver_type not in ('pyodbc', 'vertica_python'):
            raise ValueError('unsupported driver type: {}'.format(self.driver_type))
        super().__init__(*args, **kwargs)
    
    def get_conn(self):
        """
        Returns verticaql connection object
        """
        logging.getLogger().info('Getting connection {}'.format(self.vertica_conn_id))
        conn = self.get_connection(self.vertica_conn_id)
        logging.getLogger().info('Getting config...')
        conn_config = self.get_connection_config(conn, self.driver_type)
        if self.driver_type == 'pyodbc':
            from pyodbc import connect
            conn = connect('DSN=VerticaDSN', **conn_config)
        elif self.driver_type == 'vertica_python':
            logging.getLogger().info('self.driver_type == vertica_python')
            from vertica_python import connect
            conn = connect(**conn_config)
        conn.cursor().execute('SET SESSION resource_pool={resourcepool}'.format(
            resourcepool=self.resourcepool if self.resourcepool else 'default'))
        if self.runtimecap_seconds:
            conn.cursor().execute("SET SESSION runtimecap '{runtimecap_seconds} s'".format(
                runtimecap_seconds=self.runtimecap_seconds))
        return conn

    @staticmethod
    def get_connection_config(conn, driver_type='vertica_python'):
        logger = logging.getLogger()
        conn_config = dict(
            host=conn.host or 'localhost',
            database=conn.schema,
            user=conn.login,
            password=conn.password or '',
        )
        if not conn.port:
            conn_config["port"] = 5433
        else:
            conn_config["port"] = int(conn.port)
        
        if driver_type not in ('pyodbc', 'vertica_python'):
            raise ValueError('unsupported driver type: {}'.format(driver_type))
        
        if 'read_timeout' in conn.extra_dejson:
            logger.info('Using extra param read_timeout: {}'.format(conn.extra_dejson['read_timeout']))
            if driver_type == 'pyodbc':
                conn_config['timeout'] = conn.extra_dejson['read_timeout']
            elif driver_type == 'vertica_python':
                conn_config['read_timeout'] = conn.extra_dejson['read_timeout']
        
        if 'ssl' in conn.extra_dejson:
            if driver_type == 'pyodbc':
                conn_config['sslmode'] = 'require'
                logger.info('pyodbc using sslmode: require')
            elif driver_type == 'vertica_python':
                ssl_param = conn.extra_dejson['ssl']
                logger.info('Using extra param ssl: {}'.format(ssl_param))
                if ssl_param and ssl_param not in ('t', 'True', 'true'):
                    ssl_context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
                    conn_config['ssl'] = ssl_context

        if 'unicode_error' in conn.extra_dejson:
            if driver_type == 'pyodbc':
                logger.warning('pyodbc does not support unicode_error.  Ignoring this connection parameter.')
            elif driver_type == 'vertica_python':
                logger.info('Using extra param unicode_error: {}'.format(conn.extra_dejson['unicode_error']))
                conn_config['unicode_error'] = conn.extra_dejson['unicode_error']

        return conn_config
