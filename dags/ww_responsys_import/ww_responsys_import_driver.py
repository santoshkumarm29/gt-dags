from airflow.models import Variable
import logging
import io
import pandas as pd
import paramiko
import re
import tempfile
import zipfile


from common.hooks.vertica_hook import VerticaHook

logger = logging.getLogger('responsys_importer')


def import_responsys(table_configs, vertica_conn_id, *, ds_nodash=None, **context):
    sftp_client = get_sftp_client()
    sftp_client.chdir('/home/cli/gsncash_scp/export_data_feed')
    for file_name, table_name in get_files_with_date(sftp_client.listdir(), ds_nodash):
        if table_name not in table_configs:
            logger.info(f'No configuration for the table {table_name} skipping')
            continue

        target_table = 'ww.' + table_configs[table_name] + '_ods'
        logger.info(f"Inserting into {target_table} with file {file_name} from Responsys")
        with tempfile.NamedTemporaryFile(mode='w+b') as compressed_file:
            sftp_client.get(file_name, compressed_file.name)
            compressed_file.file.seek(0)
            with zipfile.ZipFile(compressed_file.file, mode='r') as zip_archive:
                vertica_copy(zip_archive.read(zip_archive.namelist()[0]).decode('UTF-8'),
                             'ww', table_configs[table_name] + '_ods', file_name, vertica_conn_id)
                logger.info(f'Finished processing for {table_name}')


def vertica_copy(csv_string, schema_name, table_name, file_name, vertica_conn_id):
    vertica_hook = VerticaHook(vertica_conn_id=vertica_conn_id,resourcepool='default')
    con = vertica_hook.get_conn()
    with io.StringIO() as string_buffer, con.cursor() as cur:
        column_names = get_column_names(dict(schema=schema_name, table_name=table_name))
        column_names.remove('file_name')
        string_buffer.write(csv_string)
        string_buffer.seek(0)
        try:
            logger.info(f"Truncating ods table: {table_name}")
            cur.execute(f"TRUNCATE TABLE {schema_name}.{table_name};")
            cur.copy(f"""
            COPY {schema_name}.{table_name} ({', '.join(column_names)}, file_name AS '{file_name}')
            FROM STDIN PARSER FCSVPARSER() ABORT ON ERROR DIRECT;
            """, string_buffer)
            con.commit()
        except Exception as e:
            logger.exception(f'COPY failed for table {table_name}')
    logger.info(f'Completed COPY into table {table_name}')


def get_column_names(params):
    con = VerticaHook(vertica_conn_id='vertica_conn', driver_type="vertica_python",resourcepool='default').get_conn()
    sql = \
f'''
select column_name
  from columns
 where table_schema = '{params['schema']}'
   and table_name = '{params['table_name']}'
order by ordinal_position asc;
'''
    df = pd.read_sql(sql, con)
    return df.column_name.tolist()


def get_files_with_date(file_names, ds_nodash):
    for file_name in file_names:
        if ds_nodash in file_name:
            # the file name has a form of 55298_<TABLE>_<DATE NODASH>_<SUFFIX>.txt.zip
            # we will parse out the table from the file so we can do keyword matching
            numbers_removed = re.sub('_[0-9]+|[0-9]+_', '', file_name)       # remove numbers
            extension_removed = numbers_removed[:numbers_removed.find('.')]  # remove .txt.zip ext
            yield file_name, extension_removed.lower()


def get_sftp_client():
    host, username, pkey = get_credentials()
    pkey_file = tempfile.NamedTemporaryFile(mode='w+')
    pkey_file.write(pkey)
    pkey_file.file.seek(0)
    pkey_obj = paramiko.RSAKey.from_private_key(pkey_file.file)
    transport = paramiko.Transport((host, 22))
    try:
        transport.connect(pkey=pkey_obj, username=username)
    except Exception as e:
        logger.exception('Could not connect to SFTP server')
        raise e
    logger.info('Successfully connected to SFTP server')
    return paramiko.SFTPClient.from_transport(transport)


def get_credentials():
    try:
        ssh_key = Variable.get('RESPONSYS_SSH_KEY')
    except Exception as e:
        logger.exception('Could not fetch credentials from Variable table')
        raise e
    host = 'files.dc2.responsys.net'
    username = 'gsncash_scp'
    pkey = ssh_key
    return host, username, pkey
