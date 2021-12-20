"""Airflow operator for Segmentation tasks.
"""
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import BaseOperator

from datetime import date, datetime, timedelta
import logging
import pathlib
import pandas as pd
import numpy as np
import re
import six
from uuid import uuid4
from pandas.api.types import CategoricalDtype

import os
import io
import joblib
from tempfile import NamedTemporaryFile

import csv
from common.hooks.vertica_hook import VerticaHook

from common.operators.ml_prediction_operator import MLPredictionOperator

class MLSegmentsOperator(MLPredictionOperator):
    """Operator specifically for Segmentation tasks.
    """
    ui_color = '#9f48ba'

    def __init__(self, features_s3_filepath, model_s3_filepaths,
                 s3_conn_id, vertica_conn_id, table_name,
                 player_id_column_name,
                 *args, **kwargs):
        super(MLSegmentsOperator, self).__init__(
            features_s3_filepath,
            model_s3_filepaths,
            s3_conn_id, vertica_conn_id,
            table_name,
            copy_columns=['subject_id',
                          'subject_id_type',
                          'predict_date',
                          'days_lookback',
                          'cluster_type',
                          'cluster',
                          'features',
                          'updated_at',
                          'model_version',
                          'model_trained_date',
                          'algorithm',
                          'model_filepath',
                          'uuid'],
            player_id_column_name=player_id_column_name,
            *args,
            **kwargs
        )
        logging.info('MLSegmentsOperator: model_s3_filepaths: {}'.format(model_s3_filepaths))

    def execute(self, context):
        df_og = self.get_features_df_from_s3().set_index(self.player_id_column_name)

        # or include this metadata in the model file so no custom calculations are necessary
        model_s3_filepath = self.get_model_s3_path(
            max_date=context['execution_date'].date(),
            date_column_name='report_date',
            model_s3_filepaths=self.model_s3_filepaths,
        )

        model_dct = self.get_model_dct_from_s3(model_s3_filepath)

        # single dictionary was serialized, where 'model' is a dictionary of all models for the prediction job.
        dict_of_model_dicts = model_dct['model']
        app = model_dct['app']
        model_version = model_dct['model_version']

        for k, model_dct in dict_of_model_dicts.items():
            df = df_og.copy()
            cluster_type = model_dct['cluster_type']
            model_trained_date = model_dct['model_trained_date']
            algorithm = model_dct['algorithm_name']
            filter_query = model_dct['filter_query']
            days_lookback = int(model_dct['n_days_lookback'])
            features = model_dct['features']
            features = '_!_'.join(features)

            predict_date = MLSegmentsOperator.get_predict_date_from_ds(context['ds'])
            logging.info("Making Predictions for predict_date='{}' segment_name={}"
                         .format(predict_date, cluster_type))

            predict_function = self.get_prediction_function(app, model_version)

            ## Filter the dataset
            if filter_query is not None:
                df = df.query(filter_query)

            ## special filters...
            if cluster_type in ['monthly_tournament_preference', 'lifetime_tournament_preference',
                                'monthly_time_of_day_preference', 'lifetime_time_of_day_preference']:
                df = df[df[model_dct['features']].sum(1) != 0]

            preds = predict_function(df, model_dct)

            # *HACK* manual cluster assignments, usually single point rules.
            # To prevent multiple predictions per player, ML predictions should be filtering out data that's shown here.
            if cluster_type == 'monthly_app_session_preference':
                df2 = df_og.query("pct_app < 0.01")
                df3 = df_og.query("pct_app >= 0.99")
                preds += ['tier 5 - None'] * len(df2)
                preds += ['tier 1 - All'] * len(df3)
                df = pd.concat([df, df2, df3], axis=0)
            elif cluster_type == 'monthly_web_session_preference':
                df2 = df_og.query("pct_web < 0.01")
                df3 = df_og.query("pct_web >= 0.99")
                preds += ['tier 5 - None'] * len(df2)
                preds += ['tier 1 - All'] * len(df3)
                df = pd.concat([df, df2, df3], axis=0)
            elif cluster_type == 'monthly_app_cef_preference':
                df2 = df_og.query("APP < 0.01")
                df3 = df_og.query("APP >= 0.99")
                preds += ['tier 5 - None'] * len(df2)
                preds += ['tier 1 - All'] * len(df3)
                df = pd.concat([df, df2, df3], axis=0)
            elif cluster_type == 'monthly_desktop_cef_preference':
                df2 = df_og.query("DESKTOP < 0.01")
                df3 = df_og.query("DESKTOP >= 0.99")
                preds += ['tier 5 - None'] * len(df2)
                preds += ['tier 1 - All'] * len(df3)
                df = pd.concat([df, df2, df3], axis=0)
            elif cluster_type == 'monthly_moweb_cef_preference':
                df2 = df_og.query("MOWEB < 0.01")
                df3 = df_og.query("MOWEB >= 0.99")
                preds += ['tier 5 - None'] * len(df2)
                preds += ['tier 1 - All'] * len(df3)
                df = pd.concat([df, df2, df3], axis=0)

            df.loc[:, 'cluster'] = preds
            df.loc[:, 'subject_id_type'] = self.player_id_column_name
            df.loc[:, 'predict_date'] = predict_date
            df.loc[:, 'days_lookback'] = days_lookback
            df.loc[:, 'cluster_type'] = cluster_type
            df.loc[:, 'model_trained_date'] = model_trained_date
            df.loc[:, 'features'] = features
            df.loc[:, 'algorithm'] = algorithm
            df.loc[:, 'updated_at'] = datetime.now()  # FIXME: timezone?
            df.loc[:, 'model_version'] = model_version
            df.loc[:, 'model_filepath'] = model_s3_filepath
            df.loc[:, 'uuid'] = [uuid4() for _ in range(len(df))]
            ## force this to be int, sometimes was passing as float (I think this is unnecessary?)
            df.loc[:, 'days_lookback'] = df.loc[:, 'days_lookback'].astype(int)
            df = df.reset_index()

            df = df[[self.player_id_column_name,
                     'subject_id_type',
                     'predict_date',
                     'days_lookback',
                     'cluster_type',
                     'cluster',
                     'features',
                     'updated_at',
                     'model_version',
                     'model_trained_date',
                     'algorithm',
                     'model_filepath',
                     'uuid']]

            column_names = [self.player_id_column_name,
                            'subject_id_type',
                            'predict_date',
                            'days_lookback',
                            'cluster_type',
                            'cluster',
                            'features',
                            'updated_at',
                            'model_version',
                            'model_trained_date',
                            'algorithm',
                            'model_filepath',
                            'uuid']

            self.write_predictions_to_db(df, column_names=column_names)

    ## Hack, quick way to deserialze a model, without needing to load any objects
    @staticmethod
    def get_model_dct_from_s3_static(model_s3_filepath, s3_conn_id, use_filesystem=True):
        logging.info("Retrieving model: {}".format(model_s3_filepath))
        try:
            # FIXME: remove this try/except when we are fully migrated to the new ECS-based Airflow
            s3_hook = S3Hook(s3_conn_id=s3_conn_id)
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

    # Need to overload this method for "quoting=csv.QUOTE_NONE" - reason: some data has escaped ',' characters
    def write_predictions_to_db(self, df, column_names):
        logging.info("Writing results to DB")
        vertica_hook = VerticaHook(vertica_conn_id=self.vertica_conn_id, resourcepool=self.resourcepool)
        logging.info(df.dtypes)
        csv_buf = io.StringIO()
        df.to_csv(csv_buf, header=False, columns=column_names, index=False,
                  encoding='utf-8', quoting=csv.QUOTE_NONE, escapechar='\\')
        csv_buf.seek(0)
        conn = vertica_hook.get_conn()
        cursor = conn.cursor()
        cursor.copy("COPY {table_name} ({column_string}) FROM stdin DELIMITER ',' ABORT ON ERROR"
                    .format(table_name=self.table_name, column_string=",".join(self.copy_columns)), csv_buf.getvalue())
        cursor.close()
        conn.close()


    ## TODO: Refactor into common code
    @staticmethod
    def get_model_info_from_filename(filename):
        """Parse the model filename and return a dictionary with model metadata.
        If `filename` does not match the regular expression pattern, None is returned.

        For example, here's a model filename:
        worldwinner_segments_model_v1_2019-01-01.pkl
        :param filename:    May be a filename or complete S3 filepath.
        :returns:           A dictionary with keys:
                                - app_name
                                - model_version
                                - report_date: The perspective from which this model was trained.
                                - model_filepath: The path to the S3 object.  Does not include bucket name.
                                - model_full_filepath: Path to the S3 object including protocol and bucket name.
                            If no match, None is returned.
        """
        pattern_str = r"""
        .*?/?                     # non-greedy match anything possibly preceding the app_name that ends with a slash
        (?P<app_name>.+)
        _segments_model_
        v(?P<model_version>\d+)
        _
        (?P<report_date>[\d-]+)
        \.pkl
        """
        p = pathlib.Path(filename)
        filename = p.parts[-1]
        pattern = re.compile(pattern_str, re.VERBOSE)
        m = pattern.match(filename)
        if m is not None:
            app_name = m.group('app_name')
            model_version = int(m.group('model_version'))
            report_date = datetime.strptime(m.group('report_date'), '%Y-%m-%d').date()
            model_full_filepath = 's3://{bucket_name}/models/segments/{app_name}/v{model_version}/{filename}'
            model_full_filepath = model_full_filepath.format(
                bucket_name=MLSegmentsOperator.get_bucket_name(),
                app_name=app_name,
                model_version=model_version,
                filename=filename
            )
            return {
                'app_name': app_name,
                'model_version': model_version,
                'report_date': report_date,
                'model_filepath': filename,
                'model_full_filepath': model_full_filepath,
            }
        return None

    ## TODO: in the future, this will also take cluster_type as a parameter, and include any special pre/post processing
    def get_prediction_function(self, app, model_version):
        ## prediction + preprocessing specifically worldwinner segments v1
        def predict_ww_v1(df, model_dct):
            model = model_dct['model']
            features = model_dct['features']
            transforms = model_dct['transforms']
            descriptions = model_dct['descriptions']
            assert len(transforms) == len(features), "length of transforms must equal length of features"

            filter_query = model_dct['filter_query']
            if filter_query is not None:
                df = df.query(filter_query)

            features_arr = df[features].copy()
            for feature, transform in zip(features, transforms):
                if model_dct['scalers'] is not None:
                    if ('standard_scaler' not in model_dct['scalers']) & ('min_max_scaler' not in model_dct['scalers']):
                        standard_scaler = model_dct['scalers'][feature]['standard_scaler']
                        min_max_scaler = model_dct['scalers'][feature]['min_max_scaler']
                        transform = re.sub('fit_transform', 'transform', transform)
                        transform = re.sub('MinMaxScaler\(\)', 'min_max_scaler', transform)
                        transform = re.sub('StandardScaler\(\)', 'standard_scaler', transform)
                        transform = re.sub('lambda x:', 'lambda x, min_max_scaler, standard_scaler:', transform)
                        print(feature, transform)
                        features_arr[feature] = eval(transform)(features_arr[feature], min_max_scaler, standard_scaler)
                    else:
                        features_arr[feature] = eval(transform)(features_arr[feature])
                else:
                    features_arr[feature] = eval(transform)(features_arr[feature])
            X = features_arr

            preds = model.predict(X)
            preds = [descriptions[x] for x in preds]
            return (preds)

        if (app == 'worldwinner') and (model_version == 1):
            return predict_ww_v1
        else:
            logging.info("this app/model_version hasn't been setup for preprocessing yet!")
            assert False, "this app/model_version hasn't been setup for preprocessing yet!"

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
        return datetime.strptime(ds, '%Y-%m-%d').date() + timedelta(days=7)

