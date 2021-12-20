"""Base class for applying a machine learning model to a features dataframe.

"""
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import BaseOperator
from airflow.models import Variable
import csv
from datetime import date, datetime, timedelta
import io
import joblib
import logging
import os
import pandas as pd
from tempfile import NamedTemporaryFile

from common.hooks.vertica_hook import VerticaHook


class MLPredictionOperator(BaseOperator):
    """Base operator for applying a machine learning model to a features dataframe and create predictions.
    
    The operator depends on a features dataframe created in a previous step as well as a pickled model file.
    
    """
    template_fields = ('features_s3_filepath', )
    template_ext = ()
    
    def __init__(self, features_s3_filepath, model_s3_filepaths,
                 s3_conn_id, vertica_conn_id, table_name, copy_columns,
                 player_id_column_name, resourcepool=None,
                 *args, **kwargs):
        """
        
        :param features_s3_filepath: The name of the input file, including bucket and full prefix. Templatized.
                                     This must contain all features used by the model, as well as the index column,
                                     and no more.
        :param model_s3_filepaths:   List of S3 filepaths of pickled models, including bucket and full prefix.
                                     The object pickled should be a dictionary with key 'model', with value being a
                                     scikit-learn model implementing the `predict_proba` method for classification tasks
                                     or `predict` for regression or other tasks.
                                     Other keys may contain meta-data, but are not mandatory.
        :param s3_conn_id:           S3 connection id.  Used for fetching the features dataframe and the pickled model.
        :param vertica_conn_id:      Vertica connection id.
        :param table_name:           The name of the table results are to be inserted into.
        :param copy_columns:         A list of column names in the target database.  This should be fixed for a
                                     given subclass.
        :param player_id_column_name:The name of the player identifier in the feature queries.
        """
        super(MLPredictionOperator, self).__init__(*args, **kwargs)
        self.features_s3_filepath = features_s3_filepath
        self.model_s3_filepaths = model_s3_filepaths
        self.s3_conn_id = s3_conn_id
        self.vertica_conn_id = vertica_conn_id
        self.table_name = table_name
        self.copy_columns = copy_columns
        self.player_id_column_name = player_id_column_name
        self.resourcepool = resourcepool

    def get_features_df_from_s3(self, use_filesystem=True):
        logging.info("Retrieving features: {}".format(self.features_s3_filepath))
        try:
            # FIXME: remove this try/except when we are fully migrated to the new ECS-based Airflow
            s3_hook = S3Hook(s3_conn_id=self.s3_conn_id)
        except TypeError as e:
            # Use default credentials
            s3_hook = S3Hook()
        features_key = s3_hook.get_key(self.features_s3_filepath)
        if use_filesystem:
            with NamedTemporaryFile('w+b') as local_features:
                logging.info("Loading Features: {}".format(local_features.name))
                try:
                    features_key.get_contents_to_filename(local_features.name)
                except AttributeError as e:
                    # boto3 with Airflow 1.9
                    features_key.download_file(local_features.name)
                df = pd.read_csv(local_features.name, dtype={self.player_id_column_name: str})
        else:
            logging.info("Loading features file into memory: {}".format(self.features_s3_filepath))
            file_stream = io.BytesIO()
            try:
                features_key.get_contents_to_file(file_stream)
            except AttributeError:
                # boto3 with Airflow 1.9
                features_key.download_fileobj(file_stream)
            df = pd.read_csv(file_stream, dtype={self.player_id_column_name: str})
        return df

    def get_other_data_from_s3(self, filepath, use_filesystem=True):
        logging.info("Retrieving File: {}".format(filepath))
        try:
            # FIXME: remove this try/except when we are fully migrated to the new ECS-based Airflow
            s3_hook = S3Hook(s3_conn_id=self.s3_conn_id)
        except TypeError as e:
            # Use default credentials
            s3_hook = S3Hook()
        features_key = s3_hook.get_key(filepath)
        if use_filesystem:
            with NamedTemporaryFile('w+b') as local_features:
                logging.info("Loading File: {}".format(local_features.name))
                try:
                    features_key.get_contents_to_filename(local_features.name)
                except AttributeError as e:
                    # boto3 with Airflow 1.9
                    features_key.download_file(local_features.name)
                df = pd.read_csv(local_features.name)
        else:
            logging.info("Loading file into memory: {}".format(filepath))
            file_stream = io.BytesIO()
            try:
                features_key.get_contents_to_file(file_stream)
            except AttributeError:
                # boto3 with Airflow 1.9
                features_key.download_fileobj(file_stream)
            df = pd.read_csv(file_stream)
        return df

    def get_model_dct_from_s3(self, model_s3_filepath, use_filesystem=True):
        logging.info("Retrieving model: {}".format(model_s3_filepath))
        try:
            # FIXME: remove this try/except when we are fully migrated to the new ECS-based Airflow
            s3_hook = S3Hook(s3_conn_id=self.s3_conn_id)
        except TypeError as e:
            # Use default credentials
            s3_hook = S3Hook()
        model_key = s3_hook.get_key(model_s3_filepath)
        if use_filesystem:
            model_name = os.path.basename(model_s3_filepath)
            with NamedTemporaryFile(mode='w+b', suffix='_{}'.format(model_name)) as local_model:
                logging.info("Loading Model: {}".format(local_model.name))
                try:
                    model_key.get_contents_to_filename(local_model.name)
                except AttributeError:
                    # boto3 with Airflow 1.9
                    model_key.download_file(local_model.name)
                model_dct = joblib.load(local_model.name)
        else:
            logging.info("Loading model into memory: {}".format(model_s3_filepath))
            file_stream = io.BytesIO()
            try:
                model_key.get_contents_to_file(file_stream)
            except AttributeError:
                # boto3 with Airflow 1.9
                model_key.download_fileobj(file_stream)
            model_dct = joblib.load(file_stream)
        return model_dct

    def write_predictions_to_db(self, df, column_names):
        logging.info("Writing results to DB")
        vertica_hook = VerticaHook(vertica_conn_id=self.vertica_conn_id, resourcepool=self.resourcepool)
        logging.info(df.dtypes)
        csv_buf = io.StringIO()
        df.to_csv(csv_buf, header=False, columns=column_names, index=False,
                  encoding='utf-8', quoting=csv.QUOTE_MINIMAL, escapechar='\\')
        csv_buf.seek(0)
        conn = vertica_hook.get_conn()
        cursor = conn.cursor()
        cursor.copy("COPY {table_name} ({column_string}) FROM stdin DELIMITER ',' ABORT ON ERROR"
                    .format(table_name=self.table_name, column_string=",".join(self.copy_columns)), csv_buf.getvalue())
        cursor.close()
        conn.close()

    @classmethod
    def get_model_s3_path(cls, max_date, date_column_name, model_s3_filepaths):
        """Given the execution date and the list of available models, determine the most recent model in the past.

        :return: Full filepath to S3 model file.
        """
        logging.info('Finding most recent model previous to: {}'.format(max_date))
        logging.info('List of available models:')
        for model_s3_filepath in model_s3_filepaths:
            logging.info('\t{}'.format(model_s3_filepath))
        available_models_df = cls.get_available_models_df(model_s3_filepaths)
        logging.info(available_models_df)
        try:
            dct = (
                available_models_df[available_models_df[date_column_name] <= max_date]
                .sort_values(date_column_name, ascending=False)
                .iloc[0]
                .to_dict()
            )
            return dct['model_full_filepath']
        except IndexError as e:
            logging.warning('Unable to find a model previous to: {}'.format(max_date))
            return None

    @classmethod
    def get_available_models_df(cls, model_s3_filepaths):
        """Return a dataframe representing all the available models.

        """
        data = []
        for model_filepath in model_s3_filepaths:
            model_info = cls.get_model_info_from_filename(model_filepath)
            if model_info:
                data.append(model_info)
        return pd.DataFrame(data)

    @staticmethod
    def get_model_info_from_filename(model_filepath):
        raise NotImplementedError('Subclasses should implement this.')

    @staticmethod
    def get_bucket_name():
        return Variable.get("general_s3_bucket_name")

    @staticmethod
    def get_predict_date_from_ds(ds):
        """Given `ds` from the task context, return the predict date to use.
        This will evaluate to `tomorrow_ds` which is simple enough, but it is included
        as a method for consistency.  The value for predict_date is set to `tomorrow_ds`
        so that we will make predictions for the latest days of data.
        Assuming the interval for the DAG is daily, the execution date `ds` evaluates to
        the beginning of the interval, and `tomorrow_ds` evaluates to the end of the
        interval, or the same calendar date as the time the job is actually triggered.
        It is important for all date window calculations to use this as the reference.

        :param ds: A string object for the execution date in the form 'YYYY-MM-DD'.
        :return: `date` object to use as the predict date.
        """
        return datetime.strptime(ds, '%Y-%m-%d').date() + timedelta(days=1)
