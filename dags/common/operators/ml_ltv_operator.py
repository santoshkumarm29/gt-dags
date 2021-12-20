"""Airflow operator for LTV tasks.

"""
from datetime import date, datetime, timedelta
from pandas.api.types import CategoricalDtype
import logging
import pathlib
import re
import six
import pandas as pd
import numpy as np
from uuid import uuid4

from common.operators.ml_prediction_operator import MLPredictionOperator


class MLLTVOperator(MLPredictionOperator):
    """Operator specifically for LTV regression tasks.

    """
    ui_color = '#6cba48'

    def __init__(self, features_s3_filepath, model_s3_filepaths,
                 s3_conn_id, vertica_conn_id, table_name,
                 player_id_column_name,
                 *args, add_features_to_output=False, **kwargs):
        super(MLLTVOperator, self).__init__(
            features_s3_filepath,
            model_s3_filepaths,
            s3_conn_id, vertica_conn_id,
            table_name,
            copy_columns=[
                'subject_id',
                'subject_id_type',
                'predict_date',
                'install_date',
                'days_observation',
                'days_horizon',
                'predicted_iap_revenue',
                'updated_at',
                'model_version',
                'model_filepath',
                'uuid',
            ],
            player_id_column_name=player_id_column_name,
            *args,
            **kwargs
        )
        self.add_features_to_output = add_features_to_output
        # logging.info('MLLTVOperator: model_s3_filepaths: {}'.format(model_s3_filepaths))

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
        ## Fix for bash ltv v4: change index name `id` to `user_device_id`
        if (model_dct['model_version'] == 4) & (model_dct['app'] in ['bash', 'bingobash']):
            df.loc[:, self.player_id_column_name] = df.loc[:, 'id']

        if (model_dct['model_version'] == 4) & (model_dct['app'] == 'organic_bingobash'):
            df.loc[:, self.player_id_column_name] = df.loc[:, 'id']
            if "network_name" in df.columns:
                df = df[~df.network_name.isin(['unattributed', 'ORGANIC', 'GSN X-Promo'])]
                df = df.drop(["network_name"], axis=1)
        
        clf = model_dct['model']
        model_version = model_dct['model_version']
        app = model_dct['app']
        n_days_observation = model_dct['n_days_observation']
        n_days_horizon = model_dct['n_days_horizon']
        predict_date = MLLTVOperator.get_predict_date_from_ds(context['ds'])
        install_date = MLLTVOperator.get_install_date(predict_date, n_days_observation)
        logging.info("Making Predictions for predict_date='{}', install_date='{}', n_days_observation={}"
                     .format(predict_date, install_date, n_days_observation))

        categorical_mapping = None
        if 'categorical_mapping' in model_dct:
           categorical_mapping = model_dct['categorical_mapping']

        features = None
        if 'features' in model_dct:
            features = model_dct['features']
            logging.info("Features used: {}".format(features))

            # hotfix for when 'weight' and 'target' are in the list of features
            # these columns are used for training the models
            for column in ['weight', 'target']:
                if column in features:
                    logging.info('Removing {} from the list of features'.format(column))
                    features.remove(column)

        df = self.preprocess_df(df, app, model_version, n_days_horizon, categorical_mapping, features)

        df['subject_id_type'] = self.player_id_column_name
        df['predict_date'] = predict_date
        df['install_date'] = install_date
        df['n_days_observation'] = n_days_observation
        df['n_days_horizon'] = n_days_horizon

        ## hotfix for tripeaks backfill 7 to 360 ... only backfill ironsource from 2019-11-01
        ## UPDATE: hotfix backfill complete, commenting this out effective starting on 2020-01-23
        #if ((n_days_observation == 7) & (n_days_horizon == 360) & (model_version == 6) & (app == 'tripeaks')):
        #    df = df[
        #        #(df['session_count'] == 1) &
        #        (df['campaign_type_mix'] == 1) &
        #        ((df['platform_device_type_ipad'] == 1) | (df['platform_device_type_iphone'] == 1))
        #        #(df['install_country_tier_us'] == 1) &
        #        #(df['time_to_first_quest_s'] == 604800) &
        #        #(df['session_duration_s'] <= 200)
        #    ]
        if len(df) == 0:
            logging.info('No installs matching the filter criteria found for today')
            return(0)
        else:
            # create a list of the feature columns to insert into the output table
            if 'features' in model_dct:
                feature_columns = model_dct['features']
            else:
                feature_columns = df.columns.tolist()

            df = self.append_predictions_column(df, model_dct, clf)

        df['updated_at'] = datetime.now()  # FIXME: timezone?
        df['model_version'] = model_version
        df['model_filepath'] = model_s3_filepath
        df['uuid'] = [uuid4() for _ in range(len(df))]

        column_names = [
            self.player_id_column_name,
            'subject_id_type',
            'predict_date',
            'install_date',
            'n_days_observation',
            'n_days_horizon',
            'predicted_iap_revenue',
            'updated_at',
            'model_version',
            'model_filepath',
            'uuid',
        ]

        if self.add_features_to_output:
            for feature_column in feature_columns:
                if feature_column not in column_names:
                    column_names.append(feature_column)

            # copy statement will fail for column names that start with a digit
            to_remove = ["n_days_horizon", "n_days_observations"]
            for column_name in column_names:
                if column_name[0].isdigit():
                    to_remove.append(column_name)

            self.copy_columns = list(set(self.copy_columns).union(set(column_names)))

            for column_name in to_remove:
                if column_name in self.copy_columns:
                    self.copy_columns.remove(column_name)

            df.rename(inplace=True, columns={
                "id": "subject_id",
                "n_days_horizon": "days_horizon",
                "n_days_observation": "days_observation"
            })

        if self.add_features_to_output:
            for column in ["n_days_observation", "n_days_horizon"]:
                if column in self.copy_columns:
                    self.copy_columns.remove(column)
            self.write_predictions_to_db(df[self.copy_columns], column_names=self.copy_columns)
        else:
            self.write_predictions_to_db(df, column_names=column_names)

    @staticmethod
    def get_model_info_from_filename(filename):
        """Parse the model filename and return a dictionary with model metadata.
        If `filename` does not match the regular expression pattern, None is returned.
        
        For example, here's a model filename:
        tripeaks_ltv_model_v3_7_360_2017-01-01.pkl

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
        (?P<app_name>\w+)
        _ltv_model_
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
            ## Hotfix for inconsistent naming convention
            if app_name == 'ww_mobile':
                app_name = 'worldwinner_mobile'
            model_version = int(m.group('model_version'))
            n_days_observation = int(m.group('n_days_observation'))
            n_days_horizon = int(m.group('n_days_horizon'))
            report_date = datetime.strptime(m.group('report_date'), '%Y-%m-%d').date()
            model_full_filepath = 's3://{bucket_name}/models/ltv/{app_name}/v{model_version}/{filename}'
            model_full_filepath = model_full_filepath.format(
                bucket_name=MLLTVOperator.get_bucket_name(),
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

    @staticmethod
    def append_predictions_column(df, model_dct, clf):
        """
        :param df: the inference dataframe, which will have a prediction column appended to it
        :param model_dct: the dictionary the model was pickled with
        :param clf: the model
        :return: input df with an appended 'predicted_iap_revenue' column
        """
        ## If features is in the model_dct, then all models in the dictionary must be able to predict using these features.
        try:
            df['predicted_iap_revenue'] = clf.predict(df)
        except:
            if 'features' in model_dct:
                df['predicted_iap_revenue'] = clf.predict(df[model_dct['features']])
            ## Else, the model must be able to make predictions on the provided df - i.e. features are passed to the model object
            else:
                df['predicted_iap_revenue'] = clf.predict(df)

        ## If multipliers_list exists in the dictionary keys, predict residual + multipliers, then add observed revenue.
        if 'multipliers_list' in list(model_dct.keys()):
            ## Model already predicted residual, now add in multiplier revenue
            df['predicted_iap_revenue'] = df['predicted_iap_revenue'] + \
                                          df['iap_revenue'] * np.sum(
                                              [multiplier for multiplier in model_dct['multipliers_list']]
                                          )
            ## Clip lower at 0
            df.loc[df['predicted_iap_revenue'] < 0, 'predicted_iap_revenue'] = 0
            ## Add in observed revenue
            df.loc[:, 'predicted_iap_revenue'] = df['iap_revenue'] + df['predicted_iap_revenue']
            df.loc[:, 'predicted_iap_revenue'] = df.loc[:, 'predicted_iap_revenue'] - df.loc[:,
                                                                                      'predicted_iap_revenue'].quantile(
                .001)
        ## Else if this is a residual prediction (cutoffs_list exists in residual models)
        elif 'cutoffs_list' in list(model_dct.keys()):
            ## Clip lower at 0
            df.loc[df['predicted_iap_revenue'] < 0, 'predicted_iap_revenue'] = 0
            df.loc[:, 'predicted_iap_revenue'] = df['iap_revenue'] + df['predicted_iap_revenue']
            ## Subtract a low quantile prediction to remove possible bias, then reclip at 0
            df.loc[:, 'predicted_iap_revenue'] = df.loc[:, 'predicted_iap_revenue'] - df.loc[:,
                                                                                      'predicted_iap_revenue'].quantile(
                .001)
            df.loc[df['predicted_iap_revenue'] < 0, 'predicted_iap_revenue'] = 0
        return(df)

    def preprocess_df(self, df, app, model_version, n_days_horizon, categorical_mapping, features):
        ## All of the individual preprocessing functions

        ## Preprocessing specifically for ww mobile ltv v2
        def _preprocess_training_df_ww_mobile_ltv_v2(df, categorical_mapping=categorical_mapping, n_days_horizon=0):
            df['n_days_horizon'] = n_days_horizon
            ## Tidy up email endings
            print('tidying emails...')
            df['email_domain_company'] = [x[-1 * list(x[::-1] + '.').index('.'):] for x in
                                          df['email_domain_company'].fillna('.')]
            df['email_domain_ending'] = [x[-1 * list(x[::-1] + '.').index('.'):] for x in
                                         df['email_domain_ending'].fillna('.')]

            ## Encode Zipcode with PCA of tax information (two components found to explain 95% of variance)
            print('merging zipcode data...')

            def _is_number(s):
                try:
                    float(s)
                    return True
                except TypeError:
                    return False
                except ValueError:
                    return False

            df.loc[([not _is_number(x) for x in df.zip]) & (~df.zip.isnull()), 'zip'] = '99999'
            df.loc[[not _is_number(x) for x in df.zip], 'zip'] = '00000'
            df.zip = df.zip.fillna('00000')
            df.zip = df.zip.astype(int)

            zipcode_data_path = "s3://{bucket_name}/supplemental_data/tax_pca.csv".format(
                bucket_name=MLLTVOperator.get_bucket_name()
            )
            tax_data = self.get_other_data_from_s3(filepath=zipcode_data_path)
            df = pd.merge(
                df.reset_index(),
                tax_data,
                how='left',
                on='zip'
            ).set_index(['idfv', 'n_days_observation', 'n_days_horizon'])
            ## Check that it joined properly
            assert 'ZIPCODE_PCA_1' in list(df.columns.values), "tax data did not join properly"

            ## Additional Features
            print('adding additional features...')
            df.loc[:, 'feature_sparsity'] = df.isnull().sum(1) / df.shape[1]
            df['username_contains_year'] = df.numbers_in_username.isin([str(x) for x in np.arange(1910, 2019)]) * 1
            df['email_username_contains_year'] = df.numbers_in_email_username.isin(
                [str(x) for x in np.arange(1910, 2019)]) * 1

            ## ---------- Object to categorical/integer encoding ---------- ##
            print('converting objects to categorical encoding....')
            obj_to_catcode_cols = ['username_suffix',
                                   'email_domain_ending',
                                   'email_domain_company',
                                   'network_name',
                                   'platform',
                                   'model',
                                   'category',
                                   'brand',
                                   'gender',
                                   'age',
                                   'country',
                                   'advertiser_id'
                                   ]
            df[obj_to_catcode_cols] = df[obj_to_catcode_cols].fillna('missing')

            print('mapping categoricals...')
            for column_name, categories in categorical_mapping.items():
                print("   * " + column_name)
                category = CategoricalDtype(categories=categories, ordered=True)
                df[column_name + '_int'] = df[column_name].astype(category)
                df[column_name + '_int'] = df[column_name + '_int'].cat.codes

            ## ---------- Object to float ---------- ##
            print('converting objects to float...')
            obj_to_float_cols = ['numbers_in_username', 'numbers_in_email_username']
            for col in obj_to_float_cols:
                df[col] = [float(x) if _is_number(x) else None for x in df[col]]
                df[col] = df[col].astype(float)

            ## Make sure there's a column named 'iap_revenue' and a column named 'network_name'
            iap_revenue_column = [c for c in df if (c.endswith('iap_revenue')) & (c.startswith('day'))][0]
            df.loc[:, 'iap_revenue'] = df.loc[:, iap_revenue_column]
            ## We didn't drop network_name column, so we're good there.
            ## reset index so we can store idfv
            df = df.reset_index()
            print('Done preprocessing!')
            return (df)

        ## Preprocessing specifically for bingobash ltv v2 and v3
        def _preprocess_training_df_bingobash_v2_v3(df, categorical_mapping=categorical_mapping, n_days_horizon=0):
            # Map to int (sorted by historical install volume descending)
            categorical_mapping = {
                'network_name': ['unattributed', 'TapJoy - Android', 'AppLike - Android',
                                 'Digital Turbine Media - Android', 'Google AdMob - Android', 'Facebook',
                                 'Adquant - Facebook', 'ironSource - Android', 'TapJoy - iOS', 'Fyber - Android',
                                 'Target Circle - Android', 'MundoMedia', 'GSN X-Promo', 'Supersonic - Android',
                                 'Google Adwords', 'Chartboost - Android', 'LifeStreet - iOS',
                                 'Liquid, Powered by PCH', 'Apple Search Ads', 'Sprinklr - Facebook', 'Fyber - iOS',
                                 'Supersonic - iOS', 'ironSource - iOS', 'NativeX - Android', 'Applovin - iOS',
                                 'Target Circle - iOS', 'Cheetah Mobile - Android', 'Motive Interactive',
                                 'Pinsight - Android', 'AdColony - iOS', 'Jump Ramp Games', 'AppLovin - Android',
                                 'Manage - iOS', 'Amazon - Fire', 'Tresensa', 'YeahMobi', 'CrossInstall - iOS',
                                 'AdColony - Android', 'Vungle - Android', 'Bidalgo - Facebook', 'AppLike - iOS',
                                 'UnityAds - Android', 'Chartboost - iOS', 'Helium Holdings - iOS', 'Vungle - iOS',
                                 'Blue Track Media', 'Google AdMob - iOS', 'Adquant - Instagram',
                                 'Volo Media - Android', 'CrossInstall - Android', 'CrossChannel - iOS', 'Matomy',
                                 'Manage - Android', 'Instagram', 'Fluent', 'LifeStreet - Android',
                                 'Nanigans - Facebook', 'OfferToro - iOS', 'NativeX - iOS', 'Google Search - iOS',
                                 'HeyZap - iOS', 'AppFlood - iOS', 'Yahoo - Gemini', 'Yep Ads - iOS',
                                 'AppFlood - Android', 'InMobi - iOS', 'UnityAds - iOS', 'Blind Ferret - iOS',
                                 'BillyMob - Android', 'Koneo - Android', 'Blue Track Media - iOS',
                                 'Blue Track Media - Android', 'OfferToro - Android', 'Blind Ferret - Android',
                                 'Koneo - iOS', 'HeyZap - Android', 'Liftoff iOS', 'Persona.ly - Android',
                                 'youAPPi - Android', 'InMobi - Android', 'Long Tail Pilot', 'FeedMob - Android',
                                 'Hang My Ads - iOS', 'Persona.ly - iOS', 'Hang My Ads - Android', 'FeedMob - iOS',
                                 'youAPPi - iOS', 'iAd', 'RadiumOne – Engage', 'Curate Mobile - Android',
                                 'Appreciate - iOS', 'Creative Clicks - Android', 'Media Fair Play - Android',
                                 'Media Fair Play - iOS', 'Bidalgo - Instagram', 'CrossChannel - Android',
                                 'Appreciate - Android', 'PowerInbox - General', 'Pinterest - iOS',
                                 'MobilityWare - iOS', 'Curate Mobile - iOS', 'Propeller Ads', 'MeetTab - Android',
                                 'MeetTab - iOS', 'Flurry', 'SmartLinks', 'ironSource Legacy - iOS',
                                 'Creative Clicks - iOS', 'adMobo - Android'
                                 ],
                'session_country': ['unknown', 'us', 'gb', 'ca', 'au', 'de', 'fr', 'br', 'th', 'nl', 'es', 'ph',
                                    'it', 'my', 'mx', 'in', 'ru', 'tr', 'nz', 'dk', 'u\'', 'be', 'pl', 'ar', 'ro',
                                    'jp', 'pt', 've', 'za', 'id', 'ch', 'tw', 'vn', 'cl', 'eg', 'co', 'sg', 'se',
                                    'no', 'bg', 'il', 'ie', 'hu', 'cz', 'rs', 'lb', 'kr', 'pe', 'hk', 'cn', 'gr',
                                    'ae', 'at', 'ua', 'ec', 'pr', 'sa', 'hr', 'fi', 'uy', 'pk', 'lt', 'sk', 'ba',
                                    'do', 'cr', 'kw', 'jm', 'bd', 'pa', 'kh', 'iq', 'mt', 'al', 'lv', 'si', 'cy',
                                    'ee', 'lu', 'tt', 'is', 'ge', 'kz', 'mk', 'mo', 'az', 'as', 'ma', 'qa', 'jo',
                                    'la', 'gu', 'dz', 'np', 'by', 'bh', 'bo', 'mu', 'bs', 'gt', 'am', 'mm', 'af',
                                    'nc', 'hn', 'pf', 'tn', 'me', 'ad', 'ng', 'md', 'sv', '41', 'ao', 'ir', 'bn',
                                    're', 'bb', 'ax', 'py', 'lk', 'aw', 'vi', 'mn', 'ni', 'bm', 'cw', 'om', 'ps',
                                    'sr', 'fo', 'na', 'ke', 'bz', 'ai', 'sy', 'gp', 'gh', 'gi', 'gy', 'im', 'je',
                                    'kg', 'lc', 'gl', 'zw', 'ag', 'um', 'uz', 'ky', 'gg', 'vc', 'mp', 'mq', 'li',
                                    'zg', 'ye', 'ly', 'vg', 'ws', 'fj', 'mc', 'xk', 'uk', 'fm', 'ht', 'sm', 'bw',
                                    'mh', 'mz', 'bq', 'ci', 'tz', 'ic', 'ug', 'aq', 'dm', 'mg', 'cm', 'sx', 'tc',
                                    'sn', 'zm', '15', 'kn', 'bj', 'gf', 'gd', 'cu', 'vu', 'bt', 'cv', 'et', 'sz',
                                    'to', 'zz', 'pg', 'pm', 'sd', 'bf', 'mf', 'io', 'tm', 'ga', 'wf', 'mv', 'pw',
                                    'tl', 'lr', 'rw', 'so', 'cd', 'gm', 'eh', 'gq', 'sc', 'tg', 'va', 'ac', 'bi',
                                    'ml', 'mr', 'tj', 'ea', '223', 'er', 'o2', 'st', 'cc', 'cg', 'cx', 'gn', 'ki',
                                    'km', 'bl', 'cf', 'dg', 'dj', 'ss', 'tv', 'ck', 'fk', 'hm', 'mw', 'nf', 'sb',
                                    'sj', 'sl', 'td', 'yt', 'cp', 'en', 'gw', 'he', 'lg', 'ne'
                                    ],
                'b_user_country': ['us', 'gb', 'ca', 'unknown', 'de', 'br', 'fr', 'au', 'th', 'ph', 'nl', 'it',
                                   'es', 'mx', 'tr', 'ru', 'tw', 'my', 'ar', 'kr', 'be', 'ae', 'jp', 'id', 'pt',
                                   'in', 've', 'pl', 'sg', 'cn', 'ro', 'dk', 'sa', 'se', 'nz', 'co', 'vn', 'hk',
                                   'il', 'cl', 'no', 'ch', 'ie', 'bg', 'eg', 'gr', 'cz', 'hu', 'pe', 'at', 'pr',
                                   'rs', 'za', 'fi', 'hr', 'ec', 'ua', 'sk', 'pk', 'ba', 'lt', 'kw', 'iq', 'uy',
                                   'do', 'tn', 'jo', 'jm', 'al', 'dz', 'lb', 'mk', 'cr', 'si', 'ma', 'mo', 'ee',
                                   'cy', 'kh', 'tt', 'mt', 'qa', 'pa', 'lv', 'lu', 'ge', 'az', 'is', 'gu', 'kz',
                                   'a1', 'as', 'bh', 'mu', 'gt', 'bo', 'ps', 'hn', 'am', 'bs', 'sv', 'sy', 'ng',
                                   'bn', 'nc', 'by', 'me', 'pf', 'la', 'om', 'bd', 'np', 'vi', 'lk', 'mm', 'bb',
                                   'md', 'ni', 'py', 're', 'ye', 'na', 'gp', 'ir', 'cw', 'mn', 'ly', 'aw', 'ao',
                                   'sd', 'gy', 'gh', 'sr', 'ke', '419', 'bz', 'af', 'mp', 'bm', 'zw', 'um', 'vc',
                                   'mq', 'ad', 'ci', 'lc', 'ag', 'fo', 'sn', 'im', 'gi', 'je', 'li', 'gd', 'gg',
                                   'fj', 'ht', 'uz', 'ky', 'tz', 'uk', 'sc', 'sx', 'cs', 'fm', 'mz', 'vg', 'ax',
                                   'gl', 'bw', 'ug', 'dm', 'gf', 'et', 'kg', 'mh', 'fa', 'cm', 'kn', 'bt', 'mg',
                                   'zm', 'bq', 'cd', '150', 'xk', 'mc', 'mv', 'tc', 'cv', 'bj', 'ai', 'gm', 'dj',
                                   '41', 'tm', 'to', 'mf', 'ws', 'ga', 'ml', 'wf', 'pg', 'tg', 'pm', 'vu', 'bf',
                                   'lr', 'sm', 'mr', 'tj', 'sl', 'mw', 'gn', 'an', 'so', 'zg', 'sz', 'rw', 'tl',
                                   'cg', 'yt', 'zz', 'ne', 'en', 'cu', 'er', '001', 'gq', 'ic', 'sb', 'cf', 'ck',
                                   'eh', 'km', 'st', 'bi', 'pw', 'fk', 'td', 'ki', 'bl', 'gw', 'io', 'ls', 'eu',
                                   'ss', 'aq', 'cx', 'ms', 'lg', 'ea', 'nu', 'tk', 'tv', 'iw'
                                   ]
            }
            for column_name, categories in categorical_mapping.items():
                category = CategoricalDtype(categories=categories, ordered=True)
                df[column_name + '_int'] = df[column_name].astype(category)
                df[column_name + '_int'] = df[column_name + '_int'].cat.codes

            # categorify categorical columns and fill NA
            object_cols_bool_index = df.dtypes == object
            object_cols = object_cols_bool_index[object_cols_bool_index].index.values

            category_cols_bool_index = df.dtypes == 'category'
            category_cols = category_cols_bool_index[category_cols_bool_index].index.values

            for col in object_cols:
                df[col] = df[col].astype('category').cat.codes
            for col in category_cols:
                df[col] = df[col].cat.codes

            # fillNA
            df = df.fillna(-999)
            return(df)

            ## Preprocessing specifically for bingobash ltv v2 and v3
        def _preprocess_training_df_bingobash_organic(df, categorical_mapping=categorical_mapping, n_days_horizon=0):
            # Map to int (sorted by historical install volume descending)
            categorical_mapping = {
                'session_country': ['unknown', 'us', 'gb', 'ca', 'au', 'de', 'fr', 'br', 'th', 'nl', 'es', 'ph',
                                    'it', 'my', 'mx', 'in', 'ru', 'tr', 'nz', 'dk', 'u\'', 'be', 'pl', 'ar', 'ro',
                                    'jp', 'pt', 've', 'za', 'id', 'ch', 'tw', 'vn', 'cl', 'eg', 'co', 'sg', 'se',
                                    'no', 'bg', 'il', 'ie', 'hu', 'cz', 'rs', 'lb', 'kr', 'pe', 'hk', 'cn', 'gr',
                                    'ae', 'at', 'ua', 'ec', 'pr', 'sa', 'hr', 'fi', 'uy', 'pk', 'lt', 'sk', 'ba',
                                    'do', 'cr', 'kw', 'jm', 'bd', 'pa', 'kh', 'iq', 'mt', 'al', 'lv', 'si', 'cy',
                                    'ee', 'lu', 'tt', 'is', 'ge', 'kz', 'mk', 'mo', 'az', 'as', 'ma', 'qa', 'jo',
                                    'la', 'gu', 'dz', 'np', 'by', 'bh', 'bo', 'mu', 'bs', 'gt', 'am', 'mm', 'af',
                                    'nc', 'hn', 'pf', 'tn', 'me', 'ad', 'ng', 'md', 'sv', '41', 'ao', 'ir', 'bn',
                                    're', 'bb', 'ax', 'py', 'lk', 'aw', 'vi', 'mn', 'ni', 'bm', 'cw', 'om', 'ps',
                                    'sr', 'fo', 'na', 'ke', 'bz', 'ai', 'sy', 'gp', 'gh', 'gi', 'gy', 'im', 'je',
                                    'kg', 'lc', 'gl', 'zw', 'ag', 'um', 'uz', 'ky', 'gg', 'vc', 'mp', 'mq', 'li',
                                    'zg', 'ye', 'ly', 'vg', 'ws', 'fj', 'mc', 'xk', 'uk', 'fm', 'ht', 'sm', 'bw',
                                    'mh', 'mz', 'bq', 'ci', 'tz', 'ic', 'ug', 'aq', 'dm', 'mg', 'cm', 'sx', 'tc',
                                    'sn', 'zm', '15', 'kn', 'bj', 'gf', 'gd', 'cu', 'vu', 'bt', 'cv', 'et', 'sz',
                                    'to', 'zz', 'pg', 'pm', 'sd', 'bf', 'mf', 'io', 'tm', 'ga', 'wf', 'mv', 'pw',
                                    'tl', 'lr', 'rw', 'so', 'cd', 'gm', 'eh', 'gq', 'sc', 'tg', 'va', 'ac', 'bi',
                                    'ml', 'mr', 'tj', 'ea', '223', 'er', 'o2', 'st', 'cc', 'cg', 'cx', 'gn', 'ki',
                                    'km', 'bl', 'cf', 'dg', 'dj', 'ss', 'tv', 'ck', 'fk', 'hm', 'mw', 'nf', 'sb',
                                    'sj', 'sl', 'td', 'yt', 'cp', 'en', 'gw', 'he', 'lg', 'ne'
                                    ],
                'b_user_country': ['us', 'gb', 'ca', 'unknown', 'de', 'br', 'fr', 'au', 'th', 'ph', 'nl', 'it',
                                   'es', 'mx', 'tr', 'ru', 'tw', 'my', 'ar', 'kr', 'be', 'ae', 'jp', 'id', 'pt',
                                   'in', 've', 'pl', 'sg', 'cn', 'ro', 'dk', 'sa', 'se', 'nz', 'co', 'vn', 'hk',
                                   'il', 'cl', 'no', 'ch', 'ie', 'bg', 'eg', 'gr', 'cz', 'hu', 'pe', 'at', 'pr',
                                   'rs', 'za', 'fi', 'hr', 'ec', 'ua', 'sk', 'pk', 'ba', 'lt', 'kw', 'iq', 'uy',
                                   'do', 'tn', 'jo', 'jm', 'al', 'dz', 'lb', 'mk', 'cr', 'si', 'ma', 'mo', 'ee',
                                   'cy', 'kh', 'tt', 'mt', 'qa', 'pa', 'lv', 'lu', 'ge', 'az', 'is', 'gu', 'kz',
                                   'a1', 'as', 'bh', 'mu', 'gt', 'bo', 'ps', 'hn', 'am', 'bs', 'sv', 'sy', 'ng',
                                   'bn', 'nc', 'by', 'me', 'pf', 'la', 'om', 'bd', 'np', 'vi', 'lk', 'mm', 'bb',
                                   'md', 'ni', 'py', 're', 'ye', 'na', 'gp', 'ir', 'cw', 'mn', 'ly', 'aw', 'ao',
                                   'sd', 'gy', 'gh', 'sr', 'ke', '419', 'bz', 'af', 'mp', 'bm', 'zw', 'um', 'vc',
                                   'mq', 'ad', 'ci', 'lc', 'ag', 'fo', 'sn', 'im', 'gi', 'je', 'li', 'gd', 'gg',
                                   'fj', 'ht', 'uz', 'ky', 'tz', 'uk', 'sc', 'sx', 'cs', 'fm', 'mz', 'vg', 'ax',
                                   'gl', 'bw', 'ug', 'dm', 'gf', 'et', 'kg', 'mh', 'fa', 'cm', 'kn', 'bt', 'mg',
                                   'zm', 'bq', 'cd', '150', 'xk', 'mc', 'mv', 'tc', 'cv', 'bj', 'ai', 'gm', 'dj',
                                   '41', 'tm', 'to', 'mf', 'ws', 'ga', 'ml', 'wf', 'pg', 'tg', 'pm', 'vu', 'bf',
                                   'lr', 'sm', 'mr', 'tj', 'sl', 'mw', 'gn', 'an', 'so', 'zg', 'sz', 'rw', 'tl',
                                   'cg', 'yt', 'zz', 'ne', 'en', 'cu', 'er', '001', 'gq', 'ic', 'sb', 'cf', 'ck',
                                   'eh', 'km', 'st', 'bi', 'pw', 'fk', 'td', 'ki', 'bl', 'gw', 'io', 'ls', 'eu',
                                   'ss', 'aq', 'cx', 'ms', 'lg', 'ea', 'nu', 'tk', 'tv', 'iw'
                                   ]
            }
            for column_name, categories in categorical_mapping.items():
                category = CategoricalDtype(categories=categories, ordered=True)
                df[column_name + '_int'] = df[column_name].astype(category)
                df[column_name + '_int'] = df[column_name + '_int'].cat.codes

            # categorify categorical columns and fill NA
            object_cols_bool_index = df.dtypes == object
            object_cols = object_cols_bool_index[object_cols_bool_index].index.values

            category_cols_bool_index = df.dtypes == 'category'
            category_cols = category_cols_bool_index[category_cols_bool_index].index.values

            for col in object_cols:
                df[col] = df[col].astype('category').cat.codes
            for col in category_cols:
                df[col] = df[col].cat.codes

            # fillNA
            df = df.fillna(-999)
            return (df)

        ## Preprocessing specifically for Tripeaks ltv v6 (refresh)
        def _preprocess_training_df_tripeaks_v6(df, features):
            if 'index' in features:
                df.loc[:, 'index'] = list(range(len(df)))
            return (df)

        ## Run the preprocessing functions based on the app_name and model_version
        if (app in ['ww_mobile', 'worldwinner_mobile']) & (model_version == 2):
            df = _preprocess_training_df_ww_mobile_ltv_v2(df, categorical_mapping=categorical_mapping,
                                                          n_days_horizon=n_days_horizon)
        elif (app in ['bash', 'bingobash']) & ((model_version == 2) | (model_version == 3) | (model_version == 4)):
            df = _preprocess_training_df_bingobash_v2_v3(df, categorical_mapping=categorical_mapping,
                                                         n_days_horizon=n_days_horizon)
        elif (app in ['organic_bingobash']) & ((model_version == 4)):
            df = _preprocess_training_df_bingobash_organic(df, categorical_mapping=categorical_mapping,
                                                         n_days_horizon=n_days_horizon)
        elif (app in ['tripeaks']) & (model_version == 6):
            df = _preprocess_training_df_tripeaks_v6(df, features)

        return (df)