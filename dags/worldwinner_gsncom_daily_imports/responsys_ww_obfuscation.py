import hashlib
import sha3
import logging
import os
import pandas
from common.hooks.vertica_hook import VerticaHook
from common.hooks.ssh_hook import SSHHook
from airflow.models import Variable

logger = logging.getLogger()


def send_obfuscated_unsubscribe_data(vertica_conn_id, remote_host, username, rsa_key_str, templates_dict, ds, yesterday_ds, **kwargs):
    logger.info("Fetching data frame")
    sql_hook=VerticaHook(vertica_conn_id=vertica_conn_id, resourcepool='default')
    ssh_hook=SSHHook(remote_host=remote_host, username=username,
                        rsa_key_str=Variable.get(rsa_key_str))
    df = get_data_frame(sql_hook, ds, yesterday_ds)
    logger.info("Creating obfuscated columns")
    create_obfuscated_columns(df)
    logger.info("Sending via sftp")
    send_df_via_sftp(df, ssh_hook, templates_dict['destination_path'])


def get_data_frame(sql_hook, ds, yesterday_ds):
    sql = """
    SELECT CUSTOMER_ID,
       email
    FROM airflow.snapshot_responsys_ww_pet_unsub
    WHERE ds = '{ds}'
    EXCEPT
    SELECT CUSTOMER_ID,
       email
    FROM airflow.snapshot_responsys_ww_pet_unsub
    WHERE ds = '{yesterday_ds}';
    """.format(ds=ds, yesterday_ds=yesterday_ds)
    return sql_hook.get_pandas_df(sql)


def create_obfuscated_columns(df):
    df["obfuscated_user_id"] = df['CUSTOMER_ID'].apply(lambda x: 'o' + str(368197520 - x))
    df["obfuscated_email"] = df['email'].apply(lambda x: hashlib.sha3_256(x.encode('utf-8')).hexdigest())
    return df


def send_df_via_sftp(df, ssh_hook, destination_path):
    filename = os.path.basename(destination_path)
    temp_filename = '/tmp/' + filename
    df.to_csv(temp_filename, columns=('CUSTOMER_ID', 'obfuscated_user_id', 'obfuscated_email'), index=False, compression='gzip', encoding='utf-8')
    ssh_client = ssh_hook.get_conn()
    sftp_client = ssh_client.open_sftp()
    sftp_client.put(temp_filename, destination_path)
