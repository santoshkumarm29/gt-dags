from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import os
import logging
from common.hooks.ssh_hook import SSHHook
from common.hooks.vertica_hook import VerticaHook
from airflow.models import Variable

class SFTPSQLResultsOperator(BaseOperator):
    """
    Executes SQL, then sends sftps a csv of the results
    """
    template_fields = ('sql', 'destination_path')
    template_ext = ('.sql',)
    logger = logging.getLogger()

    @apply_defaults
    def __init__(self, sql, vertica_conn_id, ssh_remote_host, ssh_username, ssh_rsa_key_str, destination_path, compression, resourcepool=None, *args, **kwargs):
        super(SFTPSQLResultsOperator, self).__init__(*args, **kwargs)
        self.vertica_conn_id = vertica_conn_id
        self.ssh_remote_host = ssh_remote_host
        self.ssh_username = ssh_username
        self.ssh_rsa_key_str = ssh_rsa_key_str
        self.destination_path = destination_path
        self.filename = os.path.basename(destination_path)
        self.sql = sql
        self.compression = compression
        self.resourcepool = resourcepool

    def execute(self, context):
        ssh_hook = SSHHook(remote_host=self.ssh_remote_host, username=self.ssh_username,
                           rsa_key_str=Variable.get(self.ssh_rsa_key_str))
        sql_hook = VerticaHook(vertica_conn_id=self.vertica_conn_id, resourcepool=self.resourcepool)
        self.logger.info("Executing SQL")
        df = sql_hook.get_pandas_df(self.sql)
        self.logger.info("Received results")
        temp_filename = '/tmp/' + self.filename
        self.logger.info("Writing to file")
        df.to_csv(temp_filename, index=False, compression=self.compression, encoding='utf-8')
        self.logger.info("Establishing SSL connection")
        ssh_client = ssh_hook.get_conn()
        self.logger.info("Opening SFTP")
        sftp_client = ssh_client.open_sftp()
        self.logger.info("Sending File")
        sftp_client.put(temp_filename, self.destination_path)
        self.logger.info("File Sent")