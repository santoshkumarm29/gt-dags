"""
 singular importer is moved from existing ETL job
 https://github.com/gsnsocial/verticadw/blob/master/etl/apis/singular/singular_importer.py

 SingularImporter accepts a single parameter called range_days ,meaning what will the date range for which we need to fetch data from singular api
 end_date is execution date of the dag
 start_date is end_Date- range_days

"""

from airflow.models import BaseOperator
from common.hooks.vertica_hook import VerticaHook
#from airflow.hooks import BaseHook
from airflow.hooks.base import BaseHook
from retrying import retry
import requests
from requests.packages.urllib3.util.retry import Retry
from common.date_utils import date_tuple_iter
from common.db_utils import vertica_copy
import pandas as pd
pd.set_option('display.max_columns', 100)
from datetime import timedelta, datetime
import logging
import io
import csv
import json
from time import sleep

class SingularImporterV2(BaseOperator):
    """
    Steps
        * iterate through the days and check for data availability https://api.singular.net/api/v2.0/data_availability_status
            and update into a State per day List
          ** fail fast when there is no data available for any of the days in the date range
        * iterate through the states per day list , when a state in the state list says available,
            create an async report request https://api.singular.net/api/v2.0/create_async_report ,
            update the state list to indicate this current state.
        * iterate through the states per day list,  when the state in the state list says requested,
            check report status with https://api.singular.net/api/v2.0/get_report_status,
            update the state list to indicate when report is ready
        * iterate through the states per day list,  when the state in the state list says readyToDownload,
            use the associated download url for that state and get the json payload,
            process and load into vertica and update the corresponding state to Done.
        * make sure all the states are done and then exit
       Ref: https://support.singular.net/hc/en-us/articles/360045245692-Reporting-API-Reference

       rejected_pctdiff_expected --> float --> range [0,100]
    """


    def __init__(self, dimensions, singular_table, range_days=30, creative_data=False, vertica_conn_id="vertica_conn", singular_api_key='singular_api_key',
                 resourcepool='default', rejected_pctdiff_expected=0, *args, **kwargs):

        super(SingularImporterV2, self).__init__(*args, **kwargs)
        self.vertica_conn_id = vertica_conn_id
        self.range_days = range_days
        self.singular_api_key = singular_api_key
        self.singular_table = singular_table
        self.resourcepool = resourcepool
        self.dimensions = dimensions
        self.creative_data = creative_data
        self.rejected_pctdiff_expected = rejected_pctdiff_expected

        # Our workflow states for Singular
        self.states = []  # {-1,3,2,1,0}|{Unset,Available,Requested,ReadyToDownload,Done}

        # set data format variables

        self.metrics = [
            'adn_impressions',
            'custom_clicks',
            'custom_installs',
            'adn_cost',
            'adn_estimated_total_conversions',
            'adn_original_cost',
            'ctr',
            'cvr',
            'ecpi',
            'ocvr',
            'ecpm',
            'ecpc',
        ]
        self.discrepancy_metrics = [
            'adn_clicks',
            'tracker_clicks',
            'adn_installs',
            'tracker_installs'
        ]

        self.base_url_da = 'https://api.singular.net/api/v2.0/data_availability_status'
        self.base_url_car = 'https://api.singular.net/api/v2.0/create_async_report'
        self.base_url_rs = 'https://api.singular.net/api/v2.0/get_report_status'


    def get_range(self , execution_ts):

        # rules for range :
        # 1. during the week range will be  7 days
        # 2. on saturdays at 10 30 am range will be 14 days
        # 3. on last saturday of the monht at 10 30 am range will be 180 days

        if execution_ts.weekday() == 5 and execution_ts.strftime('%H:%M') == '10:30':
            if execution_ts.month != (
                        execution_ts + timedelta(7)).month:  # saturday and 10 30 AM ans saturday is last saturday
                range_days = 60
            else:
                range_days = 14
        else:
            range_days=self.range_days

        #range_days = 180 # Temporary backfill
        logging.info( 'range days set as {range_days}'.format(range_days=range_days))

        end_date = execution_ts.date().strftime('%Y-%m-%d')
        start_date = (execution_ts.date() - timedelta(range_days)).strftime('%Y-%m-%d')
        updated_at=execution_ts.date().strftime('%Y-%m-%d %H:%M:%S')
        return start_date,end_date,updated_at

    def format_data(self, singleday_df, updated_at):
        # coerce int and float columns
        int_columns = [
            'adn_impressions',
            'custom_clicks',
            'custom_installs',
            'adn_clicks',
            'tracker_clicks',
            'adn_installs',
            'tracker_installs',
            'adn_utc_offset',
        ]

        float_columns = [
            'adn_cost',
            'adn_estimated_total_conversions',
            'adn_original_cost',
            'ctr',
            'cvr',
            'ecpi',
            'ocvr',
            'ecpm',
            'ecpc',
        ]
        # Deal with N/A values
        singleday_df = singleday_df.replace(r'N/A(?:\s+\(.*)?', '', regex=True)
        numeric_columns = int_columns + float_columns
        singleday_df[numeric_columns] = singleday_df[numeric_columns].replace('', 0).fillna(0)
        singleday_df[numeric_columns] = singleday_df[numeric_columns].astype(float)
        singleday_df[int_columns] = singleday_df[int_columns].replace('', 0).fillna(0)
        singleday_df[int_columns] = singleday_df[int_columns].astype(int)
        singleday_df['updated_at'] = updated_at

        # put the columns in the correct order
        columns = ['start_date', 'end_date'] + self.dimensions + self.metrics + self.discrepancy_metrics + [
            'updated_at']
        singleday_df = singleday_df[columns]

        logging.info(' dataFrame re ordered to match vertica columns')

        return singleday_df

    def set_states(self, start_dt, end_dt):
        """
            Initialize self.states[]
        """
        for sdt, edt in date_tuple_iter(start_dt, end_dt, n_days=1, overlap=False):
            self.states.append({
                'start_dt': sdt.strftime('%Y-%m-%d'),
                'end_dt': edt.strftime('%Y-%m-%d'),
                'state': -1,  # {-1,3,2,1,0}|{Unset,Available,Requested,ReadyToDownload,Done}
                'download_url': None
            })

    def get_data_availability_status(self, singular_api_key):
        """
         check first if the data is available before we go down that road
        :param singular_api_key:
        :return:
        """

        querystring = {
            "api_key": singular_api_key,
            "format": "json",
            "display_non_active_sources": "true"
        }

        len_states = len(self.states)
        timeout_sec = 20 * 60
        wait_sec = 1
        started_at = datetime.utcnow()
        atleast_one_available = False

        while (len_states * 3) != sum([s['state'] for s in self.states], 0):

            for i_da, s_da in enumerate(self.states):

                atleast_one_available = False
                querystring['data_date'] = s_da['start_dt']
                response_da = requests.get(url=self.base_url_da, params=querystring)

                da = json.loads(response_da.content.decode('utf-8'))

                if da['status'] == 0:
                    if not da['value']['is_all_data_available']:
                        for d in da['value']['data_sources']:
                            if d['is_available']:
                                atleast_one_available = True
                    else:
                        atleast_one_available = True
                else:
                    logging.info(da)
                    return False

                # will check the latest date and all other days
                if atleast_one_available:
                    self.states[i_da]['state'] = 3  # data is available
                else:
                    sleep(wait_sec)
                    started_at = started_at + timedelta(seconds=wait_sec)

            if (datetime.utcnow() - started_at).total_seconds() > timeout_sec:
                return True


        return True

    def create_async_report(self, singular_api_key, creative_data):
        """
        Request a report for each day to later check its status
        :param singular_api_key:
        :return: bool
        """

        query_params = {"api_key": singular_api_key}
        payload = {
            'dimensions': ','.join(self.dimensions),
            'metrics': ','.join(self.metrics),
            'discrepancy_metrics': ','.join(self.discrepancy_metrics),
            'time_breakdown': 'day',
            'format': 'json',
            'country_code_format': 'iso3'
        }
        if creative_data is True:
            payload.update({'display_alignment': 'true'})

        for i_car, s_car in enumerate(self.states):

            if self.states[i_car]['state'] == 3:

                payload['start_date'] = s_car['start_dt']
                payload['end_date'] = s_car['end_dt']
                logging.info(f"request: {payload}")

                response_car = requests.post(url=self.base_url_car, params=query_params, data=payload)

                car = json.loads(response_car.content.decode('utf-8'))

                if car['status'] == 0:
                    self.states[i_car]['report_id'] = car['value']['report_id']
                    self.states[i_car]['state'] = 2
                else:
                    logging.info(car)
                    return False

        return True


    def get_report_status(self, singular_api_key):
        """
        Keep checking the status of each report, and when its ready get the download url
        :return: bool
        """

        querystring = {
            "api_key": singular_api_key
        }

        len_states = len([ s['state'] for s in self.states if s['state'] == 2])
        timeout_sec = 20 * 60
        wait_sec = 2
        started_at = datetime.utcnow()

        while len_states != sum([s['state'] for s in self.states if s['state'] == 1], 0):

            for i_rs, s_rs in enumerate(self.states):

                if self.states[i_rs]['state'] == 2:

                    querystring['report_id'] = s_rs['report_id']
                    response_rs = requests.get(url=self.base_url_rs, params=querystring)

                    rs = json.loads(response_rs.content.decode('utf-8'))

                    if rs['status'] == 0:
                        if rs['value']['status'] == 'DONE':
                            self.states[i_rs]['download_url'] = rs['value']['download_url']
                            self.states[i_rs]['url_expires_in'] = rs['value']['url_expires_in']
                            self.states[i_rs]['state'] = 1
                        else:
                            sleep(wait_sec)
                            started_at = started_at + timedelta(seconds=wait_sec)

                    else:
                        logging.info(rs)
                        return False

                    if (datetime.utcnow() - started_at).total_seconds() > timeout_sec:
                        logging.info(f"get_report_status() : took longer than {timeout_sec}")
                        return False

        return True


    def load_data(self, conn, updated_at):

        for i_d, s_d in enumerate(self.states):

            if s_d['state'] == 1:

                r = requests.get(url=s_d['download_url'])
                ddata = json.loads(r.content.decode('utf-8'))
                r = None
                dfdata = pd.DataFrame(ddata['results'])

                ddata = None
                self.states[i_d]['state'] = 0

                if dfdata.empty:
                    logging.info(f"load_data(): no data returned for day = {s_d['start_dt']}")
                else:
                    dfdata = self.format_data(dfdata, updated_at)
                    self.copy_data_to_vertica(conn, dfdata, s_d['start_dt'])
                    logging.info(f"load_data(): data loaded for day = {s_d['start_dt']} has the shape {dfdata.shape}")

        return True

    def copy_data_to_vertica(self, connection, singleday_df, day):
        logging.info('loading data into vertica for day = {day}'.format(day=day))
        # logging.info(singleday_df)
        cursor = connection.cursor()
        try:
            sql_delete_range = """ DELETE /*+ direct, LABEL ('airflow-all-ua_master_main_dag-singular_importer_driver')*/ FROM {target_table} WHERE  date(start_date) = '{start_date}' """.format(
                target_table=self.singular_table, start_date=day)
            cursor.execute(sql_delete_range)
        except Exception as err:
            logging.error(
                'Error occured when deleting data in {target_table} '.format(target_table=self.singular_table))
            raise err

        # copy data from the dataframe  table to the target table
        try:
            vertica_copy(con=connection, data=singleday_df, target_table=self.singular_table, rejected_pctdiff_expected=self.rejected_pctdiff_expected, quoting='csv.QUOTE_NONE')

        except Exception as err:
            logging.error(f'Error occurred when copying data for {day} from the dataframe into {self.singular_table} error : {err}')
            raise err

        logging.info(f'Inserted {singleday_df.shape[0]} records into {self.singular_table} for day {day}')
        connection.commit()

    def execute(self, context):
        # set date range
        start_date, end_date, updated_at=self.get_range(context['execution_date'])

        # get api key
        conn = VerticaHook(vertica_conn_id=self.vertica_conn_id,resourcepool=self.resourcepool).get_conn()
        api_key = BaseHook.get_connection(self.singular_api_key)
        singular_api_key = api_key.password

        logging.info(f""" running singular import for date range {start_date} and {end_date} """)

        try:
            self.set_states(start_date,end_date)
            for s in self.states:
                logging.info("%s",s)
            if self.get_data_availability_status(singular_api_key):
                for s in self.states:
                    logging.info("%s",s)
                if self.create_async_report(singular_api_key, self.creative_data):
                    for s in self.states:
                        logging.info("%s", s)
                    if self.get_report_status(singular_api_key):
                        for s in self.states:
                            logging.info("%s", s)
                        if self.load_data(conn,updated_at):
                            for s in self.states:
                                logging.info("%s", s)
                        else:
                            raise Exception("load_data() : Not all days in the range were loaded")
                    else:
                        raise Exception("get_report_status() : Not all reports were produced in time")
                else:
                    for s in self.states:
                        logging.info("%s", s)
                    raise Exception("create_async_report() : All reports did not get requested in singular")
            else:
                for s in self.states:
                    logging.info("%s", s)
                raise Exception("get_data_availability_status() : Not all data for the range is avaliable")

        except Exception as err:
            logging.error(
                """ ERROR OCCURED for range {start_date} and {end_date} : error : {error} """.format(
                    start_date=start_date,
                    end_date=end_date, error=err))
            raise err
        curr=conn.cursor()
        curr.execute("SELECT PURGE_TABLE('{table}' )".format(table= self.singular_table))
        conn.close()

        logging.info(
            """ COMPLETED singular import for date range {start_date} and {end_date} """.format(start_date=start_date,
                                                                                                end_date=end_date))

