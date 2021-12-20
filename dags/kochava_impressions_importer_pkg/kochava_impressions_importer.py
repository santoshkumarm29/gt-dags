from datetime import timedelta
from kochava_impressions_importer_pkg.kochava_impressions_importer_config import get_config
from kochava_impressions_importer_pkg.kochava_impressions_importer_driver import request_report, truncate_staging_table, wait_on_report, download_report, parse_impressions_data, delete_existing_data_in_range, final_table_insert_with_synthetic,session
#from airflow.operators import BaseOperator
from airflow.models.baseoperator import BaseOperator
#from airflow.hooks import BaseHook
from airflow.hooks.base  import BaseHook
from airflow.models import Variable
from common.db_utils import vertica_copy
from common.hooks.vertica_hook import VerticaHook
from pytz import timezone

class KochavaImpressionsImporter(BaseOperator):
    def __init__(self,
                 range_days=1,
                 vertica_conn_id="vertica_conn",
                 kochava_api_key_gamesnetwork='kochava_api_key_gamesnetwork',
                 kochava_api_key_bitrhymes='kochava_api_key_bitrhymes',
                 kochava_api_key_idlegames='kochava_api_key_idlegames',
                 rejected_pctdiff_expected=0.75,
                 *args,
                 **kwargs):
        super(KochavaImpressionsImporter, self).__init__(*args, **kwargs)
        self.vertica_conn_id = vertica_conn_id
        self.range_days = range_days
        self.kochava_api_key_gamesnetwork = Variable.get(kochava_api_key_gamesnetwork)
        self.kochava_api_key_bitrhymes = Variable.get(kochava_api_key_bitrhymes)
        self.kochava_api_key_idlegames = Variable.get(kochava_api_key_idlegames)
        self.rejected_pctdiff_expected = rejected_pctdiff_expected

        self.config = get_config(self.kochava_api_key_gamesnetwork, self.kochava_api_key_bitrhymes,
                                 self.kochava_api_key_idlegames)

    def execute(self, context):
        self.log.info("start impressions kochava importer")
        vertica_hook = VerticaHook(vertica_conn_id=self.vertica_conn_id,resourcepool='default')
        start_date, end_date, inserted_on = context['prev_execution_date'], context['execution_date'], context['execution_date'].strftime('%Y-%m-%d %H:%M:%S')
        self.log.info('start_time = {start_time} end_time = {end_time}'.format(start_time=start_date , end_time=end_date))
        final_table = self.config['finaltablename']
        staging_table = self.config['stagetablename']
        scheduledreports = []
        erroredreports = []

        # run as batches of one week
        while start_date < end_date:
            end_dt = min(start_date + timedelta(days=7), end_date)

            for account in self.config['Accounts']:
                self.log.info("running for account :{account}".format(account=account))
                app_list = self.config['Accounts'][account]['app_list']
                app_key = self.config['Accounts'][account]['api_key']
                for app_id, app_list in app_list.items():
                    app_name, app_os = app_list
                    report = {
                        'type': 'impression',
                        'account': account,
                        'app_name': app_name,
                        'app_id': app_id,
                        'os': app_os,
                        'api_key': app_key,
                        'start_date': start_date,
                        'end_date': end_dt,
                        'start_date_epoch':str(int(start_date.timestamp())),
                        'end_date_epoch':str(int(end_dt.timestamp()))
                    }

                    token = request_report(report, self.log)
                    if token:
                        scheduledreports.append(report)
                    else:
                        erroredreports.append(report)
            start_date = start_date + timedelta(days=7)

        self.log.info('all report scheduled')

        # truncate staging table seperate session
        connection=vertica_hook.get_conn()
        cursor=connection.cursor()
        truncate_staging_table(cursor, staging_table)

        connection.close()
        error_indicator='N'
        for report in scheduledreports:
            with vertica_hook.get_conn() as conn:
                self.log.info('running report fetch for account  : {account} , app_name : {app_name} . app_id : {app_id}'.format(account=report['account'],app_name=report['app_name'],app_id=report['app_id']))
                link = wait_on_report(report, self.log)
                data = ''
                if link:
                    try:
                        data = download_report(report, self.log)
                    except:
                        self.log.error(
                            'running report fetch for account  : {account} , app_name : {app_name} . app_id : {app_id}'.format(
                                account=report['account'], app_name=report['app_name'], app_id=report['app_id']))
                        error_indicator='Y'

                if data:
                    data_df = parse_impressions_data(inserted_on, data, report['account'], report['os'], report['app_name'],
                                                 report['app_id'], self.log)
                    if data_df.shape[0] > 0:
                        delete_existing_data_in_range(conn, final_table, report['start_date'], report['end_date'],
                                                      report['account'], report['os'], report['app_name'], report['app_id'], self.log)

                        try:
                            vertica_copy(con=conn, data=data_df, target_table=staging_table,
                                         rejected_pctdiff_expected=self.rejected_pctdiff_expected)
                        except Exception as err:
                            self.log.error("error when loading data to staging table error: {error}".format(error=err))
                            conn.rollback()
                            error_indicator = 'Y'
                            raise Exception("error when loading data to staging table error: {error}".format(error=err))

                else:
                    self.log.warning(
                        'running report fetch for account  : {account} , app_name : {app_name} . app_id : {app_id}'.format(
                            account=report['account'], app_name=report['app_name'], app_id=report['app_id']))
                conn.commit()
        # move data to final table  seperate session
        connection = vertica_hook.get_conn()
        cursor = connection.cursor()
        session.close()
        try:
            final_table_insert_with_synthetic(cursor, staging_table, final_table, self.log)
        except Exception as err:
            error_indicator = 'Y'
            self.log.error("error when runnign final table insert error: {error}".format(error=err))

        connection.close()
        if error_indicator=='Y':
            raise Exception("One or more accounts have errored , traceback error in ariflow log above ")
        else :
            self.log.info("Completed impressions  kochava importer")
