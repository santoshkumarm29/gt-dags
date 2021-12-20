"""Airflow operator for LTC classification tasks.

"""
from datetime import date, datetime, timedelta
import logging
import pandas as pd
import pathlib
import re
from uuid import uuid4

from common.operators.ml_prediction_operator import MLPredictionOperator #copied


class MLLTCOperator(MLPredictionOperator):
    """Operator specifically for LTC classification tasks.
    
    """
    ui_color = '#ee5813'

    def __init__(self, features_s3_filepath, model_s3_filepaths,
                 s3_conn_id, vertica_conn_id, table_name, copy_columns,
                 player_id_column_name,
                 *args, **kwargs):
        super(MLLTCOperator, self).__init__(features_s3_filepath, model_s3_filepaths,
                                            s3_conn_id, vertica_conn_id, table_name, copy_columns,
                                            player_id_column_name,
                                            *args, **kwargs)
    
    def execute(self, context):
        df = self.get_features_df_from_s3()
        model_s3_filepath = self.get_model_s3_path(
            max_date=context['execution_date'].date(),
            date_column_name='predict_date',
            model_s3_filepaths=self.model_s3_filepaths,
        )
        logging.info('Using model file: {}'.format(model_s3_filepath))
        model_dct = self.get_model_dct_from_s3(model_s3_filepath)
        
        clf = model_dct['model']
        model_version = model_dct['model_version']
        category_bins = model_dct['category_bins']
        category_labels = model_dct['category_labels']
        target_class = int(model_dct['target_class'])

        predict_date = MLLTCOperator.get_predict_date_from_ds(context['ds'])
        logging.info("Making Predictions for predict_date '{}'".format(predict_date))
        features = model_dct['features']
        logging.info("Features used: {}".format(features))
        df['p_churn'] = clf.predict_proba(df[features])[:, list(clf.classes_).index(target_class)]
        df['category'] = pd.cut(df['p_churn'], category_bins, labels=category_labels)
        df['predict_type'] = 'veteran'
        df['predict_date'] = predict_date
        df['updated_at'] = datetime.now()  # FIXME: timezone?
        df['model_version'] = model_version
        df['model_filepath'] = model_s3_filepath
        df['uuid'] = [uuid4() for _ in range(len(df))]
        
        column_names = [
            self.player_id_column_name,
            'p_churn',
            'category',
            'predict_type',
            'predict_date',
            'updated_at',
            'model_version',
            'model_filepath',
            'uuid',
        ]
        self.write_predictions_to_db(df, column_names=column_names)

    @staticmethod
    def get_model_info_from_filename(filename):
        """Parse the model filename and return a dictionary with model metadata.
        If `filename` does not match the regular expression pattern, None is returned.

        For example, here's a model filename:
        gsncasino_ltc_veteran_model_v1_c30_a30_i30_2017-07-01.pkl
        
        :param filename:    May be a filename or complete S3 filepath.
        :returns:           A dictionary with keys:
                                - app_name
                                - model_version
                                - n_days_churn
                                - predict_date: The perspective from which this model was trained.
                                - model_filepath: The path to the S3 object.  Does not include bucket name.
                                - model_full_filepath: Path to the S3 object including protocol and bucket name.
                            If no match, None is returned.
        """
        pattern_str = r"""
        .*?/?                     # non-greedy match anything possibly preceding the app_name that ends with a slash
        (?P<app_name>\w+)
        _ltc_veteran_model_
        v(?P<model_version>\d+)
        _
        c(?P<n_days_churn>\d+)
        _
        a(?P<n_days_activity>\d+)
        _
        i(?P<n_min_days_since_install>\d+)
        _
        (?P<predict_date>[\d-]+)
        \.pkl
        """
        p = pathlib.Path(filename)
        filename = p.parts[-1]
        pattern = re.compile(pattern_str, re.VERBOSE)
        m = pattern.match(filename)
        if m is not None:
            app_name = m.group('app_name')
            model_version = int(m.group('model_version'))
            n_days_churn = int(m.group('n_days_churn'))
            n_days_activity = int(m.group('n_days_activity'))
            n_min_days_since_install = int(m.group('n_min_days_since_install'))
            predict_date = datetime.strptime(m.group('predict_date'), '%Y-%m-%d').date()
            model_full_filepath = 's3://{bucket_name}/models/ltc/{app_name}/v{model_version}/{filename}'
            model_full_filepath = model_full_filepath.format(
                bucket_name=MLLTCOperator.get_bucket_name(),
                app_name=app_name,
                model_version=model_version,
                filename=filename
            )
            return {
                'app_name': app_name,
                'model_version': model_version,
                'n_days_churn': n_days_churn,
                'n_days_activity': n_days_activity,
                'n_min_days_since_install': n_min_days_since_install,
                'predict_date': predict_date,
                'model_filepath': filename,
                'model_full_filepath': model_full_filepath,
            }
        return None
