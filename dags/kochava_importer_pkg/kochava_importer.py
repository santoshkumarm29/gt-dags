from datetime import timedelta
from kochava_importer_pkg.kochava_importer_config import get_config as master_config

from kochava_importer_pkg.install_importer_parser import default_columns, nested_columns, vendor_specific_columns

from kochava_importer_pkg.kochava_importer_driver import request_report, \
    truncate_staging_table, wait_on_report, download_report, parse_install_data
#from airflow.operators import BaseOperator
from airflow.models import BaseOperator
#from airflow.hooks import BaseHook
from airflow.hooks.base  import BaseHook
from airflow.models import Variable
from common.hooks.vertica_hook import VerticaHook
from common.db_utils import vertica_copy
import pendulum

import io
import csv



class KochavaImporter(BaseOperator):
    def __init__(self,
                 vertica_conn_id="vertica_conn",
                 kochava_gamename="gamesnetwork",
                 kochava_api_key="kochava_api_key_gamesnetwork",
                 ua_config_type = "master",
                 rejected_pctdiff_expected=0,
                 *args,
                 **kwargs):
        super(KochavaImporter, self).__init__(*args, **kwargs)
        self.vertica_conn_id = vertica_conn_id
        self.kochava_api_key = Variable.get(kochava_api_key)
        self.kochava_gamename = kochava_gamename
        self.config = master_config(self.kochava_gamename, self.kochava_api_key)
        self.default_columns = default_columns
        self.nested_columns = nested_columns
        self.vendor_specific_columns = vendor_specific_columns
        self.rejected_pctdiff_expected = rejected_pctdiff_expected

    def get_range(self, next_execution_date):

        # rules for range :
        # 1. during the week at 14 UTC range will be  7 days
        # 2. on sundays at 14 UTC range will be 30 days
        # 3. at all other hours, range will be 1 day

        try:
            next_execution_date_pst = next_execution_date.in_timezone('America/Los_Angeles')
        except :
            # bug in Airflow , When the dag is sceduled once a day context date variables are datatime object
            next_execution_date=pendulum.instance(next_execution_date,'UTC')
            next_execution_date_pst=next_execution_date.in_timezone('America/Los_Angeles')

        self.log.info('day of week {day_of_week}, hour {hour} (UTC)'.format(day_of_week=next_execution_date.day_of_week, hour=next_execution_date.hour))

        if next_execution_date.day_of_week == 6 and next_execution_date.hour == 19:
            range_days = 30
        elif next_execution_date.hour == 19:
            range_days = 7
        else:
            range_days = 1

        self.log.info('range days set as {range_days}'.format(range_days=range_days))

        end_date = next_execution_date_pst
        start_date = next_execution_date_pst - timedelta(range_days)
        inserted_on = next_execution_date_pst.format('%Y-%m-%d %H:%M:%S')

        self.log.info('Start date (PST): {start_date}, end date (pst): {end_date}, inserted on value: {inserted_on}'.
                      format(start_date=start_date, end_date=end_date, inserted_on=inserted_on))

        return start_date, end_date, inserted_on


    def execute(self, context):
        self.log.info("start kochava importer")
        vertica_hook = VerticaHook(vertica_conn_id=self.vertica_conn_id,resourcepool='default')
        start_date, end_date, inserted_on = self.get_range(context['next_execution_date'])
        staging_table = self.config['stagetablename']
        scheduledreports = []
        erroredreports = []

        # run as batches of one week
        while start_date < end_date:
            end_dt = min(start_date + timedelta(days=7), end_date)

            #for account in self.config['Accounts']:
            self.log.info("running for account :{account}".format(account=self.kochava_gamename))
            app_list = self.config['app_list']
            app_key = self.config['api_key']

            for app_id, app_list in app_list.items():
                app_name, app_os = app_list
                report = {
                    'type': 'install',
                    'account': self.kochava_gamename,
                    'app_name': app_name,
                    'app_id': app_id,
                    'os': app_os,
                    'api_key': app_key,
                    'start_date': start_date.to_datetime_string(),
                    'end_date': end_dt.to_datetime_string(),
                    'start_date_epoch':str(int(start_date.timestamp())),
                    'end_date_epoch':str(int(end_dt.timestamp()))
                }
                if 'traffic_including' in self.config:
                    report['traffic_including'] = self.config['traffic_including']

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

        for report in scheduledreports:
                self.log.info('running report fetch for account  : {account} , app_name : {app_name} ,app_id :{app_id}'.format(account=report['account'],app_name=report['app_name'],app_id=report['app_id']))
                link = wait_on_report(report, self.log)
                data = ''
                if link:
                    try:
                        data = download_report(report, self.log)
                    except:
                        self.log.error("Kochava download errored! for account : {account} , app_name : {app_name}".format(account=report['account'],app_name=report['app_name']))

                if data:
                    data_df = parse_install_data(inserted_on, data, report['account'], report['os'], report['app_name'],
                                                 report['app_id'], self.default_columns, self.nested_columns,
                                                 self.vendor_specific_columns, self.log)
                    if data_df.shape[0] > 0:
                        with vertica_hook.get_conn() as conn:
                            try:
                                vertica_copy(conn, data_df, staging_table,
                                             rejected_pctdiff_expected=self.rejected_pctdiff_expected)
                            except Exception as err:
                                self.log.error("error when loading data to staging table error: {error}".format(error=err))
                                conn.rollback()
                                raise Exception("error when loading data to staging table error: {error}".format(error=err))
                            conn.commit()
                else:
                    self.log.warning("Kochava returned empty set! for account : {account} , app_name : {app_name}".format(account=report['account'],app_name=report['app_name']))

        self.log.info("Completed kochava importer")
