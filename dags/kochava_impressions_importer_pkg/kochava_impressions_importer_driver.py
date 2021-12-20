import requests
import json
#from requests.packages.urllib3.util.retry import Retry
from urllib3.util.retry import Retry
from retrying import retry
from urllib.parse import parse_qsl
import pandas as pd

API_VERSION = 'v1.4'
API_DETAIL_URL = 'https://reporting.api.kochava.com/{api_version}/detail'.format(api_version=API_VERSION)
API_PROGRESS_URL = 'https://reporting.api.kochava.com/{api_version}/progress'.format(api_version=API_VERSION)

session = requests.session()
retryy = Retry(total=5, status_forcelist=(500, 502, 503, 504), backoff_factor=0.2)
session.mount('https://', requests.adapters.HTTPAdapter(max_retries=retryy))


def request_report(report, log):
    # prepare request data
    params = {
        'api_key': report['api_key'],
        'app_guid': report['app_id'],
        'time_start': report['start_date_epoch'],
        'time_end': report['end_date_epoch'],
        'traffic': [report['type']],
        'delivery_format': 'json',
        'delivery_method': ['S3link'],
        'notify': ['dummy@gsngames.com'],
        #'time_zone': 'America/Los_Angeles'
    }

    log.info("Requesting %s data on account %s for app %s, %s: %s from %s to %s." % (
        report['type'], report['account'], report['app_name'], report['os'], report['app_id'], report['start_date'],
        report['end_date']))
    r = session.post(API_DETAIL_URL, data=json.dumps(params),timeout=180)
    log.info("post status %s : %s : %s" % (r.status_code, r.url, json.dumps(params)))

    if (r.status_code == 200):
        ret = r.json()
        if ('report_token' in ret):
            log.info("%s Report Queue with token %s" % (report['type'], ret['report_token']))
            report['report_token'] = ret['report_token']
            return ret['report_token']
        else:
            log.error('Error Occured when trying to post data for account : {account} with error {error} check the parameters'.format(
            account=report['account'], error=r.content))
        return None

    else:

        log.error('Error Occured when trying to post data for account : {account} with error {error}'.format(
            account=report['account'], error=r.content))
        return None


def truncate_staging_table(cursor, tablename):
    cursor.execute(""" Truncate table {table_name}""".format(table_name=tablename))


# wait exponentially for 10 seconds between interval to maximum of 60 mins
@retry(wait_exponential_multiplier=1000, wait_exponential_max=10 * 1000, stop_max_delay=60 * 60 * 1000)
def wait_on_report(report, log):
    # prepare request data
    params = {
        'api_key': report['api_key'],
        'app_guid': report['app_id'],
        'token': report['report_token'],
    }
    r = session.post(API_PROGRESS_URL, data=json.dumps(params),timeout=120)

    if r.status_code == 200:
        ret = r.json()
        log.info('wait for report for account : {account} , app_name : {app_name} returned status : {status}'.format(
            account=report['account'], app_name=report['app_name'], status=ret["status"]))
        if 'status' in ret:
            if ret["status"] == 'queued':
                raise Exception(" report not completed yet")
            elif ret["status"] == "error":
                log.error("error waiting for report : {error}".format(error=r.content))
                return None
            elif ret["status"] == "completed":
                report['link'] = ret['report']
                return ret['report']
            else:
                raise Exception(" report not completed yet , waiting status returned as {status} ".format(status=ret["status"]))
        else:
            log.error("error waiting for report : {error}".format(error=r.content))
            return None
    else:
        log.error("error waiting for report : {error}".format(error=r.content))
        log.error("error waiting for report : {error}".format(error=r.text))
        return None


def download_report(report, log):
    r = session.get(report['link'])
    if r.status_code == 200:
        try:
            data = r.json(strict=False)
        except Exception as e:
            log.error("Exception parsing json data. Error : {error}".format(error=str(e)))
            raise Exception("Exception parsing json data.")

        log.info("%s records processed." % (len(data)))
        return data
    else:
        log.error(r.text)
        raise Exception(r.text)


def sanity_filter(impressions, log):
    try:
        for k in impressions.keys():
            # replace empty strings with null
            if (impressions[k] == ''):
                impressions[k] = None
            # remove unicode strings
            if (isinstance(impressions[k], bytes)):
                impressions[k] = impressions[k].encode('utf-8')

            # replace newlines
            if isinstance(impressions[k], str):
                impressions[k] = impressions[k].replace("\n", "")

            # clean up idfa and idfv
            if (k == 'idfa' or k == 'idfv'):
                if (impressions[k] == '00000000-0000-0000-0000-000000000000'):
                    impressions[k] = None
                elif (type(impressions[k]) == str and len(impressions[k]) == 36):
                    impressions[k] = impressions[k].upper()
                else:
                    impressions[k] = None
            # avoid non-string impressions_site
            if (k == 'impression_site' and type(impressions[k]) == dict):
                impressions[k] = None
            # trim site id
            if (k == 'impression_site' and type(impressions[k]) == str):
                impressions[k] = impressions[k][:512]
            # trim too long strings
            if (k in ('impression_campaign', 'impression_campaign_name', 'impression_network', 'impression_network_name',
                      'impression_tracker', 'impression_tracker_name','device_ua', 'impression_device_ver') and type(impressions[k]) == str):
                impressions[k] = impressions[k][:80].replace(',','')
            # parse incorrectly formatted device id strings into dict
            if (k in ('impression_identifiers','impression_original_request')):
                if (type(impressions[k]) == str):
                    impressions[k] = sanity_filter(dict(parse_qsl(impressions[k].replace('\u0026', '&'))), log)
                if (type(impressions[k]) == dict):
                    impressions[k] = sanity_filter(impressions[k], log)
                if (impressions[k] == None):
                    impressions[k] = {}
            # parse and trim creative strings if necessary
            if (k == 'impression_creative') and type(impressions[k]) ==str:
                impressions[k] = impressions[k][:255]
            elif    (k == 'impression_creative') and type(impressions[k]) ==dict:
                impressions[k] = impressions.get('impression_creative',{}).get('creative_id','')[:255]

    except AttributeError as e:
        log.error('Failed to parse impressions  line: %s' % str(impressions))
        raise e

    return impressions


def parse_impressions_data(inserted_on, impressions, account, platform, app_name, app_id, log):
    to_insert = []
    log.info('Parsing data for  account : {account} , app_name : {app_name} ,app_id : {app_id} , platform : {platform}'.format(
        account=account, app_name=app_name, platform=platform,app_id=app_id))
    for impression in impressions:
        impression = sanity_filter(impression, log)
        # convert impression_date to UTC byt adding +00:00 , vertica will automatically convert the value to UTC
        impression_date=impression.get('impression_date_adjusted', None)+'+00:00' if impression.get('impression_date_adjusted', None) !=None else None
        to_insert.append({
            'ad_partner': 'kochava',
            'platform': platform,
            'inserted_on': inserted_on,
            'app_name': app_name,
            'app_id': app_id,
            'account': account,
            'impression_date': impression_date,
            'campaign': impression.get('impression_campaign', None),
            'campaign_name': impression.get('impression_campaign_name', None),
            'creative': impression.get('impression_creative', None),
            'network': impression.get('impression_network', None),
            'network_name': impression.get('impression_network_name', None),
            'site_id': impression.get('impression_site',None),
            'tracker': impression.get('impression_tracker_name', None),
            'tracker_id': impression.get('impression_tracker', None),
            'user_country_code': impression.get('impression_original_request', {}).get('meta_data',{}).get('user_country',None),
            'device_ua': impression.get('impression_original_request', {}).get('device_ua',None),
            'device_version':impression.get('impression_device_ver',None),
            'idfa': impression.get('impression_identifiers', {}).get('idfa', None),
            'idfv': impression.get('impression_identifiers', {}).get('idfv', None),
            'adid': impression.get('impression_identifiers', {}).get('adid',impression.get('impression_identifiers', {}).get('device_id', None)),
            'android_id': impression.get('impression_identifiers', {}).get('android_id', None),
            'origination_ip': impression.get('impression_original_request', {}).get('ip_address',None),
            'campaign_tier1_id': impression.get('impression_original_request', {}).get('meta_data',{}).get('campaign_tier1_id',None),
            'campaign_tier1_name': impression.get('impression_original_request', {}).get('meta_data', {}).get(
                'campaign_tier1_name', None),
            'impression_kochava_imp_id':impression.get('impression_kochava_imp_id', None),
            'imp_event':impression.get('impression_original_request', {}).get('meta_data', {}).get(
                'imp_event', None)

        })

    return pd.DataFrame(to_insert)


def delete_existing_data_in_range(conn, final_table, start_date, end_date, account, platform, app_name, app_id, log):
    cursor=conn.cursor()
    cursor.execute("""DELETE /*+ direct, LABEL ('airflow-ua-ua_master_main_dag.kochava_impressions_importer_subdag-t_import_kochava_impressions') */ FROM {final_table}
                      WHERE ad_partner = 'kochava'
                        AND account = '{account}'
                        AND platform = '{platform}'
                        AND app_name = '{app_name}'
                        AND (app_id = '{app_id}' OR app_id is NULL)
                        AND impression_date >= '{start_date}'
                        AND impression_date <= '{end_date}' 
                    """.format(final_table=final_table, account=account, platform=platform, app_name=app_name,
                               app_id=app_id, start_date=start_date, end_date=end_date))

    deleted_row_count = cursor.fetchall()[0][0]
    log.info("Deleted %i impressions  data records from %s to %s." % (deleted_row_count, start_date, end_date))


def final_table_insert_with_synthetic(cursor, stagingtable, finaltable, log):
    log.info("applying synthetic_ids...")

    # don't mess with this weird looking query, vertica could not figure out how to optimize it properly and IGNORED THE FORCED SYNTAX ORDER unless it was reversed like this!!!
    cursor.execute("""
        insert /* +direct, LABEL ('airflow-ua-ua_master_main_dag.kochava_impressions_importer_subdag-t_import_kochava_impressions') */ into %s

        -- this must be used otherwise vertica is retarded..
        select /*+SYNTACTIC_JOIN, LABEL ('airflow-ua-ua_master_main_dag.kochava_impressions_importer_subdag-t_import_kochava_impressions') */ s.*, TO_CHAR(impression_date, 'YYYYMM'), m.synthetic_id

        -- the order must be reversed and joined backwards otherwise vertica ignores the forced syntax infavor of an insanely slow unoptimization that takes an hour... WOW stupid vertica..        
        from gsnmobile.dim_device_mapping as m
        right outer join %s as s
            on coalesce(s.idfa, s.idfv, s.android_id, s.adid) = m.id
           and id_type in ('idfa','android_id','googleAdId', 'idfv')
    """ % (finaltable, stagingtable))
    cursor.execute(""" SELECT PURGE_TABLE('{finaltable}') """.format(finaltable=finaltable))
