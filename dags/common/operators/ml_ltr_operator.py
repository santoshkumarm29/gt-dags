"""Airflow operator for LTR tasks.
"""
from datetime import date, datetime, timedelta
import logging
import pathlib
import pandas as pd
import re
import six
from uuid import uuid4
from pandas.api.types import CategoricalDtype

from common.operators.ml_prediction_operator import MLPredictionOperator


class MLLTROperator(MLPredictionOperator):
    """Operator specifically for LTR Binary Classification tasks.
    """
    ui_color = '#9f48ba'

    def __init__(self, features_s3_filepath, model_s3_filepaths,
                 s3_conn_id, vertica_conn_id, table_name,
                 player_id_column_name,
                 *args, **kwargs):
        super(MLLTROperator, self).__init__(
            features_s3_filepath,
            model_s3_filepaths,
            s3_conn_id, vertica_conn_id,
            table_name,
            ## Using days_observations rather than n_days_observation to be consistent with LTV table schema
            copy_columns=['subject_id',
                          'subject_id_type',
                          'predict_date',
                          'install_date',
                          'days_observation',
                          'days_horizon',
                          'played_mobile',
                          'iap_revenue',
                          'predicted_probability',
                          'category',
                          'updated_at',
                          'model_version',
                          'model_filepath',
                          'uuid'],
            player_id_column_name=player_id_column_name,
            *args,
            **kwargs
        )
        logging.info('MLLTROperator: model_s3_filepaths: {}'.format(model_s3_filepaths))

    def execute(self, context):
        df = self.get_features_df_from_s3()
        # FIXME: max_date needs to take into consideration n_days_observation
        # exec_date - timedelta(days=n_days_observation)
        # or include this metadata in the model file so no custom calculations are necessary
        model_s3_filepath = self.get_model_s3_path(
            max_date=context['execution_date'].date(),
            date_column_name='report_date',
            model_s3_filepaths=self.model_s3_filepaths,
        )
        model_dct = self.get_model_dct_from_s3(model_s3_filepath)

        clf = model_dct['model']
        app = model_dct['app']
        model_type = model_dct['model_type']
        model_version = model_dct['model_version']
        n_days_observation = model_dct['n_days_observation']
        n_days_horizon = model_dct['n_days_horizon']
        predict_date = MLLTROperator.get_predict_date_from_ds(context['ds'])
        install_date = MLLTROperator.get_install_date(predict_date, n_days_observation)
        logging.info("Making Predictions for predict_date='{}', install_date='{}', n_days_observation={}"
                     .format(predict_date, install_date, n_days_observation))
        #features = model_dct['features']
        #logging.info("Features used: {}".format(features))

        preprocessing_function = self.get_preprocessing_function(app, model_version, model_type)

        df['subject_id_type'] = self.player_id_column_name
        df['predict_date'] = predict_date
        df['install_date'] = install_date
        df['days_observation'] = n_days_observation
        df['days_horizon'] = n_days_horizon
        df['predicted_probability'], df['category'] = clf.predict_proba(df, preprocessing_function)
        df['updated_at'] = datetime.now()  # FIXME: timezone?
        df['model_version'] = model_version
        df['model_filepath'] = model_s3_filepath
        df['uuid'] = [uuid4() for _ in range(len(df))]

        df = df[[self.player_id_column_name,
                 'subject_id_type',
                 'predict_date',
                 'install_date',
                 'days_observation',
                 'days_horizon',
                 'played_mobile',
                 'iap_revenue',
                 'predicted_probability',
                 'category',
                 'updated_at',
                 'model_version',
                 'model_filepath',
                 'uuid']]

        column_names = [self.player_id_column_name,
                        'subject_id_type',
                        'predict_date',
                        'install_date',
                        'days_observation',
                        'days_horizon',
                        'played_mobile',
                        'iap_revenue',
                        'predicted_probability',
                        'category',
                        'updated_at',
                        'model_version',
                        'model_filepath',
                        'uuid']

        self.write_predictions_to_db(df, column_names=column_names)

    ## TODO: Refactor into common code
    @staticmethod
    def get_model_info_from_filename(filename):
        """Parse the model filename and return a dictionary with model metadata.
        If `filename` does not match the regular expression pattern, None is returned.

        For example, here's a model filename:
        worldwinner_ltr_model_v1_7_45_2019-01-01.pkl
        :param filename:    May be a filename or complete S3 filepath.
        :returns:           A dictionary with keys:
                                - app_name
                                - model_version
                                - n_days_observation
                                - n_days_horizon
                                - report_date: The perspective from which this model was trained.
                                - model_filepath: The path to the S3 object.  Does not include bucket name.
                                - model_full_filepath: Path to the S3 object including protocol and bucket name.
                            If no match, None is returned.
        """
        pattern_str = r"""
        .*?/?                     # non-greedy match anything possibly preceding the app_name that ends with a slash
        (?P<app_name>.+)
        _ltr_model_
        v(?P<model_version>\d+)
        _
        (?P<n_days_observation>\d+)
        _
        (?P<n_days_horizon>\d+)
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
            n_days_observation = int(m.group('n_days_observation'))
            n_days_horizon = int(m.group('n_days_horizon'))
            report_date = datetime.strptime(m.group('report_date'), '%Y-%m-%d').date()
            model_full_filepath = 's3://{bucket_name}/models/ltr/{app_name}/v{model_version}/{filename}'
            model_full_filepath = model_full_filepath.format(
                bucket_name=MLLTROperator.get_bucket_name(),
                app_name=app_name,
                model_version=model_version,
                filename=filename
            )
            return {
                'app_name': app_name,
                'model_version': model_version,
                'n_days_observation': n_days_observation,
                'n_days_horizon': n_days_horizon,
                'report_date': report_date,
                'model_filepath': filename,
                'model_full_filepath': model_full_filepath,
            }
        return None

    @staticmethod
    def get_install_date(predict_date, n_days_observation):
        """Provide a single method to determine the install date based on the execution date
        and `n_days_observation`.  It's not complicated, but it's important to be consistent.
        :param predict_date: The execution date the prediction is run.
        :param n_days_observation: The number of days of observation.
        :return: The install date as a `date` object.
        """
        if isinstance(predict_date, six.string_types):
            predict_date = datetime.strptime(predict_date, '%Y-%m-%d').date()
        # Add a buffer of one day to ensure all installs have full days of data
        install_date = predict_date - timedelta(days=n_days_observation + 1)
        return install_date

    def get_preprocessing_function(self, app, model_version, model_type):

        ## Preprocessing specifically worldwinner ltr registration based v1
        ## All of the individual preprocessing functions
        def preprocess_df_ww_ltr_reg_v1(df, categorical_mapping):
            df['partnertag_int'] = [(re.sub('\D+', '', x)) for x in df['partnertag'].astype(str)]
            df['partnertag_int'] = df['partnertag_int'].fillna(-999)
            df.loc[df['partnertag_int'] == '', 'partnertag_int'] = -999
            df['partnertag_int'] = df['partnertag_int'].astype(int)

            obj_to_catcode_cols = ['advertiser_id', 'username_suffix']
            df[obj_to_catcode_cols] = df[obj_to_catcode_cols].fillna('missing')

            print('mapping categoricals...')
            for column_name, categories in categorical_mapping.items():
                print("   * " + column_name)
                category = CategoricalDtype(categories=categories, ordered=True)
                df[column_name + '_int'] = df[column_name].astype(category)
                df[column_name + '_int'] = df[column_name + '_int'].cat.codes

            df = df.drop(['year', 'month', 'day_of_week', 'day_of_month', 'day_of_year',
                          'iso_week', 'quarter', 'days_since_2008',
                          'advertiser_id', 'partnertag', 'username_suffix'], 1)
            return (df, categorical_mapping)

        ## Preprocessing specifically worldwinner ltr first time deposit based v1
        def preprocess_df_ww_ltr_ftd_v1(df, categorical_mapping):
            df['zip_int'] = [(re.sub('\D+', '', x)) for x in df['zip'].astype(str)]
            df['zip_int'] = df['zip_int'].fillna(-999)
            df.loc[df['zip_int'] == '', 'zip_int'] = -999
            df['zip_int'] = df['zip_int'].astype(int)

            df['partnertag_int'] = [(re.sub('\D+', '', x)) for x in df['partnertag'].astype(str)]
            df['partnertag_int'] = df['partnertag_int'].fillna(-999)
            df.loc[df['partnertag_int'] == '', 'partnertag_int'] = -999
            df['partnertag_int'] = df['partnertag_int'].astype(int)

            obj_to_catcode_cols = ['advertiser_id', 'username_suffix', 'country', 'state', 'city']
            df[obj_to_catcode_cols] = df[obj_to_catcode_cols].fillna('missing')

            print('mapping categoricals...')
            for column_name, categories in categorical_mapping.items():
                print("   * " + column_name)
                category = CategoricalDtype(categories=categories, ordered=True)
                df[column_name + '_int'] = df[column_name].astype(category)
                df[column_name + '_int'] = df[column_name + '_int'].cat.codes

            df = df.drop(['year', 'month', 'day_of_week', 'day_of_month', 'day_of_year',
                          'iso_week', 'quarter', 'days_since_2008',
                          'advertiser_id', 'partnertag', 'username_suffix', 'zip', 'country', 'state', 'city'], 1)
            return (df, categorical_mapping)

        if (app == 'worldwinner') and (model_version == 1) and (model_type == 'ltr_reg'):
            return preprocess_df_ww_ltr_reg_v1
        elif (app == 'worldwinner') and (model_version == 1) and (model_type == 'ltr_ftd'):
            return preprocess_df_ww_ltr_ftd_v1
        else:
            logging.info("this app/model_version hasn't been setup for preprocessing yet!")
            assert False, ""