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
retryy = Retry(total=5, status_forcelist=(500, 502, 503, 504), backoff_factor=10)
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
    if 'traffic_including' in report:
        params['traffic_including'] = [report['traffic_including']]

    log.info("Requesting %s data on account %s for app %s, %s: %s from %s to %s." % (
        report['type'], report['account'], report['app_name'], report['os'], report['app_id'], report['start_date'],
        report['end_date']))
    r = session.post(API_DETAIL_URL, data=json.dumps(params))
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
    print(""" Truncate table {table_name}""".format(table_name=tablename))
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
    r = session.post(API_PROGRESS_URL, data=json.dumps(params))

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
                raise Exception(" report not completed yet , waiting ")
        else:
            log.error("error waiting for report : {error}".format(error=r.content))
            return None
    else:
        log.error("error waiting for report : {error}".format(error=r.content))
        return None


@retry(stop_max_attempt_number=4)
def download_report(report, log):
    print(report['link'])
    r = session.get(report['link'])
    if r.status_code == 200:
        try:
            data = r.json()
        except:
            log.error("Exception parsing json data.")
            raise Exception("Exception parsing json data.")

        log.info("%s records processed." % (len(data)))
        return data
    else:
        log.error(r.text)
        raise Exception(r.text)


def sanity_filter(install, log):
    try:
        for k in install.keys():

            # replace empty strings with null
            if (install[k] == ''):
                install[k] = None

            # remove unicode strings
            if (isinstance(install[k], bytes)):
                install[k] = install[k].encode('utf-8')

            # replace newlines
            if isinstance(install[k], str):
                install[k] = install[k].replace("\n", "")

            # clean up idfa and idfv
            if (k == 'idfa' or k == 'idfv'):
                if (install[k] == '00000000-0000-0000-0000-000000000000'):
                    install[k] = None
                elif (type(install[k]) == str and len(install[k]) == 36):
                    install[k] = install[k].upper()
                else:
                    install[k] = None

            # trim too long strings
            if (k in (
                    'campaignid', 'campaign_id', 'g_id', 'adgroup', 'keyword', 'matchtype', 'network',
                    'request_campaignid',
                    'request_campaign_id', 'request_g_id', 'request_adgroup', 'request_keyword', 'request_matchtype',
                    'request_network', 'install_device_version') and type(install[k]) == str):
                install[k] = install[k][:80]

            if (k in ('installgeo_region') and type(install[k]) == str):
                install[k] = install[k][:10]

            # parse incorrectly formatted request and device id strings and into dict
            if (k in ('install_original_request', 'install_device_ids')):
                if (type(install[k]) == str):
                    install[k] = sanity_filter(dict(parse_qsl(install[k].replace('\u0026', '&'))), log)
                if (type(install[k]) == dict):
                    install[k] = sanity_filter(install[k], log)
                if (install[k] == None):
                    install[k] = {}

            # pull out the site_id if its a dict
            if (k == 'attribution_site' and type(install[k]) == dict):
                install[k] = sanity_filter(install[k], log)
                install[k] = install[k].get('site_id', None)

            # trim site_id when its too long
            if (k in ('attribution_site') and type(install[k]) == str):
                install[k] = install[k][:512]

            # trim install_device_ua when its too long
            if (k in ('install_device_ua') and type(install[k]) == str):
                install[k] = install[k][:255]

            # parse and trim creative strings if necessary
            if (k == 'attribution_creative'):
                if (type(install[k]) == str):
                    try:
                        if (install[k][0] == '{'):
                            creative = json.loads(install[k])
                            creativeid = creative.get('creative_id', creative.get('creative', install[k]))
                            install[k] = creativeid.encode('utf-8')[0:255]
                        else:
                            install[k] = install[k][:255]
                    except:
                        pass
                if (type(install[k]) == dict):
                    install[k] = sanity_filter(install[k], log)
                    install[k] = install[k].get('creative_id', install[k].get('creative', None))

    except AttributeError as e:
        log.error('Failed to parse install line: %s' % str(install))
        raise e

    return install


def parse_install_data(inserted_on, installs, account, platform, app_name, app_id, default_columns,
                           nested_columns, vendor_specific_columns, log):
    to_insert = []
    log.info('Parsing data for  account : {account} , app_name : {app_name} , platform : {platform}'.format(
        account=account, app_name=app_name, platform=platform))
    for install in installs:
        install = sanity_filter(install, log)
        parsed_install = {
            'ad_partner': 'kochava',
            'platform': platform,
            'inserted_on': inserted_on,
            'app_name': app_name,
            'app_id': app_id,
            'account': account,
        }

        parsed_install.update(extract_renamed_keys(install, default_columns))
        parsed_install.update(extract_nested_keys(install, nested_columns))
        parsed_install.update(extract_custom_keys(install, vendor_specific_columns, 'attribution_network_name'))

        to_insert.append(parsed_install)

    return pd.DataFrame(to_insert)


def extract_renamed_keys(event, rename_config, default_value=None):
    # this function takes a dict and returns a new dict with renamed keys based on a config
    out_event = {}
    for new_name, old_name in rename_config.items():

        if isinstance(old_name, list):
            for name in old_name:
                if name in event:
                    out_event[new_name] = event[name]

        elif old_name in event:
            out_event[new_name] = event[old_name]
        else:
            out_event[new_name] = default_value
    return out_event


def extract_nested_keys(event, rename_config, default_value=None):
    # this function takes a dict containing nested values and returns a new dict, unnested
    # with correct key names based on a config
    out_event = {}

    for field_name, config in rename_config.items():
        nested_field = event.get(field_name, {})

        if isinstance(nested_field, dict) is True:
            out_event.update(extract_renamed_keys(nested_field, config))
        else:
            for new_name in config.keys():
                out_event[new_name] = default_value

    return out_event


def extract_custom_keys(event, rename_config, custom_field, default_value=None):
    # this function takes a dict containing nested values and returns a new dict, unnested
    # with correct key names paired to values that are conditional based on a value in the input dict
    # e.g. it parses the original request data differently based on what the vendor network is for an install
    out_event = {}

    for new_name, custom_config in rename_config.items():
        key_list = custom_config.get(event.get(custom_field, 'DEFAULT').upper(), custom_config.get('DEFAULT'))
        if len(key_list) > 0:
            out_event[new_name] = default_value
            for key in key_list:
                if get_nested_value(event, key, default_value) is not None:
                    out_event[new_name] = get_nested_value(event, key, default_value)
                    break
    return out_event


def get_nested_value(d, nest, default_value = None):
    keys = nest.split('.')
    for i in keys:
        if isinstance(d, dict) is True:
            d = d.get(i, default_value)
        else:
            d = default_value
    return d

