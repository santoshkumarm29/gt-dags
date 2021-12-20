from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from tempfile import NamedTemporaryFile
import logging

from common.hooks.vertica_hook import VerticaHook


class IndexedMultiDFGetterToS3Operator(BaseOperator):
    """Operator which takes a list of python functions returning dataframes, and combines the results into a csv file on
    S3. All queries must contain the index column, and this is the only column they may share. Getter functions must
    take two arguments: getter_function(ds, conn) where ds is the date string from the airflow context, and conn is a
    database connection. Useful when processing is necessary which is difficult to do in sql.

    :param features_file: The name of the output file, including bucket and full prefix. Templatized.
    :param features_index_name: The name of the shared index column.
    :param getter_list: A list of python functions to be combined.
    :param vertica_conn_id: Vertica connection id.
    :param s3_conn_id: S3 connection id
    :param getter_params: A dict of any extra parameters you want to pass to your getter functions in getter_list.
                          Make sure those functions accept *args, **kwargs if you are passing unneeded params from the DAG.
    """
    template_fields = ('features_file',)
    template_ext = ('.sql',)
    ui_color = '#a8764b'

    def __init__(self, features_file, features_index_name, getter_list, vertica_conn_id, s3_conn_id,
                 getter_params=None, resourcepool=None, *args, **kwargs):
        super(IndexedMultiDFGetterToS3Operator, self).__init__(*args, **kwargs)
        self.features_file = features_file
        self.features_index_name = features_index_name
        self.getter_list = getter_list
        self.s3_conn_id = s3_conn_id
        self.vertica_conn_id = vertica_conn_id
        if getter_params is None:
            self.getter_params = {}
        else:
            self.getter_params = getter_params
        self.resourcepool = resourcepool

    def execute(self, context):
        logging.info("Running {} functions".format(len(self.getter_list)))
        s3_hook = S3Hook()
        # try:
        #     # FIXME: remove this try/except when we are fully migrated to the new ECS-based Airflow
        #     s3_hook = S3Hook(s3_conn_id=self.s3_conn_id)
        # except TypeError as e:
        #     # Use default credentials
        #     s3_hook = S3Hook()
        vertica_hook = VerticaHook(
            vertica_conn_id=self.vertica_conn_id,
            resourcepool=self.resourcepool,
        )
        conn = vertica_hook.get_conn()
        getter_params = self.getter_params
        dfs = [getter(context['ds'], conn, **getter_params).set_index(self.features_index_name) for getter in self.getter_list]
        output_df = dfs[0]
        if len(dfs) > 1:
            logging.info("Joining data frames")
            output_df = output_df.join(other=dfs[1:])
        with NamedTemporaryFile('w+') as local_file:
            logging.info("Writing to S3: {}".format(self.features_file))
            output_df.to_csv(local_file)
            local_file.flush()
            s3_hook.load_file(filename=local_file.name, key=self.features_file, replace=True)
