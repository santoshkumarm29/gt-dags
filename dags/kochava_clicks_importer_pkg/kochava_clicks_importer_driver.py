import requests
import json
#from requests.packages.urllib3.util.retry import Retry
from urllib3.util.retry import Retry
from retrying import retry
from urllib.parse import parse_qsl
import pandas as pd
import io
import csv
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
        'time_zone': 'America/Los_Angeles'
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


def sanity_filter(click, log):
    try:
        for k in click.keys():
            # replace empty strings with null
            if (click[k] == ''):
                click[k] = None
            # remove unicode strings
            if (isinstance(click[k], bytes)):
                click[k] = click[k].encode('utf-8')

            # replace newlines
            if isinstance(click[k], str):
                click[k] = click[k].replace("\n", "")

            # clean up idfa and idfv
            if (k == 'idfa' or k == 'idfv'):
                if (click[k] == '00000000-0000-0000-0000-000000000000'):
                    click[k] = None;
                elif (type(click[k]) == str and len(click[k]) == 36):
                    click[k] = click[k].upper()
                else:
                    click[k] = None;
            # avoid non-string click_site
            if (k == 'click_site' and type(click[k]) == dict):
                click[k] = None;
            # trim site_id
            if (k == 'click_site' and type(click[k]) == str):
                click[k] = click[k][:512]
            # trim too long strings
            if (k in ('click_campaign', 'click_campaign_name', 'click_network', 'click_network_name',
                      'click_tracker', 'click_tracker_name', 'click_status','click_device_ver') and type(click[k]) == str):
                click[k] = click[k][:80]
            # parse incorrectly formatted device id strings into dict
            if (k in ('click_identifiers')):
                if (type(click[k]) == str):
                    click[k] = sanity_filter(dict(parse_qsl(click[k].replace('\u0026', '&'))), log)
                if (type(click[k]) == dict):
                    click[k] = sanity_filter(click[k], log)
                if (click[k] == None):
                    click[k] = {}
            # parse and trim creative strings if necessary
            if (k == 'click_creative'):
                if (type(click[k]) == str):
                    try:
                        if (click[k][0] == '{'):
                            creative = json.loads(click[k]);
                            creativeid = creative.get('creative_id', creative.get('creative', click[k]))
                            click[k] = creativeid.encode('utf-8')[0:255]
                        else:
                            click[k] = click[k][:255]
                    except:
                        pass;
                if (type(click[k]) == dict):
                    click[k] = sanity_filter(click[k], log)
                    click[k] = click[k].get('creative_id', click[k].get('creative', None))


    except AttributeError as e:
        log.error('Failed to parse clicks  line: %s' % str(click))
        raise e

    return click


def parse_click_data(inserted_on, clicks, account, platform, app_name, app_id, log):
    to_insert = []
    log.info('Parsing data for  account : {account} , app_name : {app_name} ,app_id : {app_id} , platform : {platform}'.format(
        account=account, app_name=app_name, platform=platform,app_id=app_id))
    for click in clicks:
        click = sanity_filter(click, log)
        to_insert.append({
            'ad_partner': 'kochava',
            'platform': platform,
            'inserted_on': inserted_on,
            'app_name': app_name,
            'app_id': app_id,
            'account': account,
            'click_date': click.get('click_date_adjusted', None),
            'campaign': click.get('click_campaign', None).replace('`','') if click.get('click_campaign', None) != None else None,
            'campaign_name': click.get('click_campaign_name', None).replace('`','') if  click.get('click_campaign_name', None) != None else  None,
            'creative': click.get('click_creative', None).replace('`','') if  click.get('click_creative', None) != None else None ,
            'network': click.get('click_network', None).replace('`','') if  click.get('click_network', None) != None else None,
            'network_name': click.get('click_network_name', None).replace('`','') if  click.get('click_network_name', None) != None else None,
            'site_id': click.get('click_site', None).replace('`','') if  click.get('click_site', None) != None else None,
            'tracker': click.get('click_tracker_name', None).replace('`','') if  click.get('click_tracker_name', None) != None else None,
            'tracker_id': click.get('click_tracker', None).replace('`','') if   click.get('click_tracker', None) != None else  None ,
            'country_code': click.get('clickgeo_country_code', None).replace('`','') if  click.get('clickgeo_country_code', None) != None else None ,
            'click_status': click.get('click_status', None).replace('`','') if  click.get('click_status', None) != None else None,
            'device_version': click.get('click_device_ver', None).replace('`','') if   click.get('click_device_ver', None) != None else  None ,
            'idfa': click.get('click_identifiers', {}).get('idfa', None).replace('`','') if  click.get('click_identifiers', {}).get('idfa', None) != None else None,
            'idfv': click.get('click_identifiers', {}).get('idfv', None).replace('`','') if  click.get('click_identifiers', {}).get('idfv', None) != None else None,
            'adid': click.get('click_identifiers', {}).get('adid',
                                                           click.get('click_identifiers', {}).get('device_id', None)).replace('`','') if click.get('click_identifiers', {}).get('adid',
                                                           click.get('click_identifiers', {}).get('device_id', None)) !=None else None,
            'android_id': click.get('click_identifiers', {}).get('android_id', None).replace('`','') if  click.get('click_identifiers', {}).get('android_id', None) != None else None,
            'origination_ip': click.get('click_ip', None).replace('`','') if  click.get('click_ip', None) != None else None,

            'original_request': click.get('click_original_request', None) if click.get('click_original_request',None) != None else None,
            'clickgeo_continent_code': click.get('clickgeo_continent_code', None).replace('`', '') if click.get('clickgeo_continent_code',
                                                                                        None) != None else None,
            'country_name': click.get('clickgeo_country_name', None).replace('`', '') if click.get('clickgeo_country_name',
                                                                                        None) != None else None,
            'latitude': click.get('clickgeo_latitude', None).replace('`', '') if click.get('clickgeo_latitude',
                                                                                        None) != None else None,
            'longitude': click.get('clickgeo_longitude', None).replace('`', '') if click.get('clickgeo_longitude',
                                                                                        None) != None else None,
            'kochava_click_id': click.get('kochava_click_id', None).replace('`', '') if click.get('kochava_click_id',
                                                                                        None) != None else None,
            'partner_click_id': click.get('partner_click_id', None).replace('`', '') if click.get('partner_click_id',
                                                                                        None) != None else None




        })



    return pd.DataFrame(to_insert)


def delete_existing_data_in_range(conn, final_table, start_date, end_date, account, platform, app_name, app_id, log):
    cursor=conn.cursor()
    cursor.execute("""DELETE /*+ direct, LABEL ('airflow-ua-ua_master_main_dag.kochava_clicks_importer_subdag-t_import_kochava_clicks') */ FROM {final_table}
                      WHERE ad_partner = 'kochava'
                        AND account = '{account}'
                        AND platform = '{platform}'
                        AND app_name = '{app_name}'
                        AND (app_id = '{app_id}' OR app_id is NULL)
                        AND click_date >= TIMESTAMP WITH TIME ZONE '{start_date}' AT TIME ZONE 'GMT' 
                        AND click_date <= TIMESTAMP WITH TIME ZONE '{end_date}' AT TIME ZONE 'GMT' 
                    """.format(final_table=final_table, account=account, platform=platform, app_name=app_name,
                               app_id=app_id, start_date=start_date, end_date=end_date))

    deleted_row_count = cursor.fetchall()[0][0]
    log.info("Deleted %i clicks  data records from %s to %s." % (deleted_row_count, start_date, end_date))


def final_table_insert_with_synthetic(cursor, stagingtable, finaltable, log):
    log.info("applying synthetic_ids...")

    # don't mess with this weird looking query, vertica could not figure out how to optimize it properly and IGNORED THE FORCED SYNTAX ORDER unless it was reversed like this!!!
    cursor.execute("""
        insert /* +direct, LABEL ('airflow-ua-ua_master_main_dag.kochava_clicks_importer_subdag-t_import_kochava_clicks') */ into %s

        -- this must be used otherwise vertica is retarded..
        select /*+SYNTACTIC_JOIN, LABEL ('airflow-ua-ua_master_main_dag.kochava_clicks_importer_subdag-t_import_kochava_clicks') */ s.ad_partner,
        s.platform,
        s.inserted_on,
        s.app_name,
        s.app_id,
        s.account,
        s.click_date,
        s.campaign,
        s.campaign_name,
        s.creative,
        s.network,
        s.network_name,
        s.site_id,
        s.tracker,
        s.tracker_id,
        s.country_code,
        s.click_status,
        s.device_version,
        s.idfa,
        s.idfv,
        s.adid,
        s.android_id,
        s.origination_ip, 
        TO_CHAR(click_date, 'YYYYMM'),
        m.synthetic_id,
        CASE WHEN s.platform ='amazon'  then coalesce(s.android_id, s.adid)
                    WHEN s.platform ='ios'  then coalesce(s.idfa, s.idfv)
                    WHEN s.platform ='android'  then coalesce(s.android_id, s.adid )
                    END id,
        s.original_request,
        s.clickgeo_continent_code,
        s.country_name,
        s.kochava_click_id,
        s.partner_click_id,
        s.latitude,
        s.longitude

        -- the order must be reversed and joined backwards otherwise vertica ignores the forced syntax infavor of an insanely slow unoptimization that takes an hour... WOW stupid vertica..        
        from gsnmobile.dim_device_mapping as m
        right outer join %s as s
            on coalesce(s.idfa, s.idfv, s.android_id, s.adid) = m.id
           and id_type in ('idfa','android_id','googleAdId', 'idfv')
    """ % (finaltable, stagingtable))
    cursor.execute(""" SELECT PURGE_TABLE('{finaltable}') """.format(finaltable=finaltable))

