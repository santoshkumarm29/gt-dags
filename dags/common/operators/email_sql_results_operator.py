from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from common.email_utils import send_email
import os
import logging
from common.hooks.vertica_hook import VerticaHook


class EmailSQLResultsOperator(BaseOperator):
    """
    Executes SQL, then sends the results as an email.
    """
    template_fields = ('sql', 'subject', 'filename')
    template_ext = ('.sql',)
    valid_styles = ('table', 'attachment')
    logger = logging.getLogger()

    @apply_defaults
    def __init__(self, sql, vertica_conn_id, source, subject, to_addresses=[], cc_addresses=[], bcc_addresses=[], style='table', filename='', compression=None, resourcepool=None, *args, **kwargs):
        super(EmailSQLResultsOperator, self).__init__(*args, **kwargs)
        self.vertica_conn_id = vertica_conn_id
        self.sql = sql
        self.source = source
        self.to_addresses = to_addresses
        self.cc_addresses = cc_addresses
        self.bcc_addresses = bcc_addresses
        self.subject = subject
        self.filename = filename
        self.compression = compression
        self.resourcepool = resourcepool
        if style in self.valid_styles:
            self.style = style
        else:
            raise ValueError("Invalid style, must be in {}".format(",".join(self.valid_styles)))

    def execute(self, context):
        self.logger.info("Executing SQL")
        hook = VerticaHook(vertica_conn_id=self.vertica_conn_id, resourcepool=self.resourcepool)
        df = hook.get_pandas_df(self.sql)
        self.logger.info("Received results")
        if self.style == 'table':
            self.logger.info("Writing to HTML")
            html = df.to_html(index=False)
            self.logger.info("Sending email - to:" + str(self.to_addresses) + " cc:" + str(self.cc_addresses) + " bcc:" + str(self.bcc_addresses))
            send_email(source=self.source, subject=self.subject, message=html, to_addresses=self.to_addresses, cc_addresses=self.cc_addresses, bcc_addresses=self.bcc_addresses)
        elif self.style == 'attachment':
            temp_filename = '/tmp/' + self.filename
            self.logger.info("Writing to file")
            df.to_csv(temp_filename, index=False, compression=self.compression, encoding='utf-8')
            self.logger.info("Sending email - to:" + str(self.to_addresses) + " cc:" + str(self.cc_addresses) + " bcc:" + str(self.bcc_addresses))
            send_email(source=self.source, subject=self.subject, message="", to_addresses=self.to_addresses, cc_addresses=self.cc_addresses, bcc_addresses=self.bcc_addresses, attachments=(temp_filename,))
        self.logger.info("Email Sent")