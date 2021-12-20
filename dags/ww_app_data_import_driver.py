from airflow.hooks.S3_hook import S3Hook
from datetime import datetime, timedelta
import pandas as pd
import pendulum
from airflow.models import BaseOperator, Variable
from airflow.hooks.base_hook import BaseHook
from airflow.utils.decorators import apply_defaults
from common.hooks.vertica_hook import VerticaHook
from common.db_utils import get_dataframe_from_query
from common.slack_notifier import send_sns_slack_notification
import logging
import jinja2
import os
import sys

local_tz = pendulum.timezone('America/New_York')  # timestamps in S3 are in ET time
logger = logging.getLogger('ww_app_data_import')


class S3HomeHook(S3Hook):

    def get_file_size(self, bucket_name, prefix='', delimiter=''):
        s3_conn = self.get_conn()
        response = s3_conn.head_object(
            Bucket=bucket_name,
            Key=prefix
        )
        return response['ContentLength']


def xcom_push(dict, key, **kwargs):
    kwargs['ti'].xcom_push(key=key, value=dict)


def get_xcom(task_id, **kwargs):
    ti = kwargs['ti']
    list_source = ti.xcom_pull(key='ww_source', task_ids='t_get_file_list')[task_id]
    return list_source


def get_xcom_file_list(**kwargs):
    ti = kwargs['ti']
    d_source = ti.xcom_pull(key='ww_file_source', task_ids='t_get_file_list')
    return d_source


def get_column_names(params):
    con = VerticaHook(vertica_conn_id="vertica_conn", driver_type="vertica_python").get_conn()
    sql = \
        f'''
select column_name
  from columns
 where table_schema = '{params['schema_name']}'
   and table_name = '{params['final_table_name']}'
   AND column_name != '{params.get('ignore_column', '')}'
   AND column_name != 's3_key'
order by ordinal_position asc;
'''
    df = pd.read_sql(sql, con)
    return df.column_name.tolist()


def get_template_sql(sql, params):
    template_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'sql/app_data_import')
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(template_path))
    template = env.get_template(sql)
    template.globals['get_column_names'] = get_column_names
    tsql = template.render(params=params)
    return tsql


def evenly_distribute(tab_list, no_of_buckets):
    """Distribute items in tab_list (tuples of (val, weight)) into no_of_buckets buckets"""
    buckets = [[] for i in range(no_of_buckets)]
    weights = [[0, i] for i in range(no_of_buckets)]
    for item in sorted(tab_list, key=lambda x: x[1], reverse=True):
        idx = weights[0][1]
        buckets[idx].append(item)
        weights[0][0] += item[1]
        weights = sorted(weights)
    return buckets


def check_for_file_size(bucket, aws_conn_id, prefix=None, **kwargs):
    s3 = S3HomeHook(aws_conn_id)
    print(prefix)
    print(bucket)
    prefixes = []
    if isinstance(prefix, list):
        for i in prefix:
            try:
                prefixes_temp = s3.list_prefixes(bucket_name=bucket, prefix=i, delimiter='/')
                prefixes = prefixes + prefixes_temp
            except Exception as err:
                logger.info("prefix not found - {pfix} - {err}".format(pfix=i,err=str(err)))
                continue
    else:
        prefixes = s3.list_prefixes(bucket_name=bucket, prefix=prefix, delimiter='/')

    logger.info(prefixes)
    file_list_size = []
    for prefix in prefixes:
        keys = s3.list_keys(bucket_name=bucket, prefix=prefix, delimiter='/')
        logger.info(keys)
        for key in keys:
            d = {}
            d['key'] = key
            d['size'] = s3.get_file_size(bucket_name=bucket, prefix=key, delimiter='/')
            file_list_size.append(d)
    return file_list_size


def get_task_groups(s3_bucket_name, aws_conn_id, vertica_conn_id, resourcepool, look_back_hours, task_prefix,
                    number_of_tasks, env, **kwargs):
    exe_date_str = kwargs.get('ts')
    logger.info(exe_date_str)
    exe_date_dt = datetime.strptime(exe_date_str, '%Y-%m-%dT%H:%M:%S+00:00')
    logger.info(exe_date_dt)
    exe_date_dt_local = exe_date_dt.astimezone(local_tz)
    logger.info(exe_date_dt_local)
    # Add 2 hours to make it to current
    exe_date_dt_local = exe_date_dt_local + timedelta(hours=2)
    start_dt = exe_date_dt_local - timedelta(hours=look_back_hours)
    end_dt = exe_date_dt_local
    logger.info(start_dt)
    logger.info(end_dt)
    prefix_list = []
    for i in range(0, (end_dt - start_dt).seconds, 3600):
        d0 = start_dt + timedelta(seconds=i)
        prefix_list.append(d0.strftime('%Y-%m-%d/%H/'))
    x = check_for_file_size(s3_bucket_name, aws_conn_id, prefix_list)
    if len(x) == 0:
        logger.info("No files to process")
        sys.exit(1)
    df_s3_files = pd.DataFrame(x)
    logger.info(df_s3_files)
    conn = VerticaHook(vertica_conn_id=vertica_conn_id, resourcepool=resourcepool).get_conn()
    processed_files_sql = """Select distinct file_name as key, file_size as size from ww.data_import_logs 
                                 where success_yn = 'Y' 
                                 and final_table_errors is null
                                 and file_date >= to_date('{file_date}','YYYY-MM-DD') 
                                 and file_hour >= {file_hour} 
                                 and environment = '{env}'""".format(file_date=prefix_list[0].split('/')[0],
                                                                    file_hour=int(prefix_list[0].split('/')[1]),env=env)
    df_logs = get_dataframe_from_query(processed_files_sql, conn)
    logger.info(df_logs)
    df_s3_files_to_load = df_s3_files[df_s3_files.key.isin(df_logs.key) == False]
    logger.info("List of files to load...")
    logger.info(df_s3_files_to_load)
    df_s3_files_to_load['folder_name'] = \
        df_s3_files_to_load['key'].str.extract(
            r'[0-9]{4}\-[0-9]{2}\-[0-9]{2}\/[0-9]{2}\/(\w+)\/\d+\.[w]{2}\.(\w+)\.[l][d]',
            expand=True)[1]
    df_s3_files_to_load['fname_with_size'] = df_s3_files_to_load['key'] + '^' + df_s3_files_to_load['size'].astype(str)
    df_group_size = df_s3_files_to_load.groupby(['folder_name'], as_index=False).sum()
    logger.info(df_group_size)
    list_group_size = df_group_size.values.tolist()
    logger.info(list_group_size)
    d_task = {}
    for i, v in enumerate(evenly_distribute(list_group_size, number_of_tasks)):
        d_task[task_prefix + str(i)] = v
    logger.info(d_task)
    d_files_by_group = df_s3_files_to_load.groupby('folder_name')['fname_with_size'].apply(list).to_dict()
    logger.info(d_files_by_group)
    xcom_push(d_task, 'ww_source', **kwargs)
    xcom_push(d_files_by_group, 'ww_file_source', **kwargs)


def get_config_data_by_source(conn, run_freq, env):
    logger.info("run_freq {x}".format(x=run_freq))
    config_sql = f""" SELECT /*+ LABEL ('airflow-skill-worlwinner_import_dag-ww_importer')*/ * 
                            FROM ww.data_import_config 
                           WHERE run_frequency = '{run_freq}' AND active_yn = 'Y' and environment = '{env}';"""
    config_df = get_dataframe_from_query(config_sql, conn)
    return config_df


class WWImporter(BaseOperator):

    @apply_defaults
    def __init__(self, run_freq='DAILY', aws_conn_id='ww_s3_import', vertica_conn_id='vertica_conn', env='PROD',
                 s3_bucket_name='worldwinner-data-imports',slack_secret_key='slack_api_key', op_kwargs={},**kwargs):
        super().__init__(**kwargs)
        self.run_freq = run_freq
        logger.info("run_freq {x}".format(x=run_freq))
        self.aws_conn_id = aws_conn_id
        self.vertica_conn_id = vertica_conn_id
        self.env = env
        self.s3_bucket_name = s3_bucket_name
        self.slack_secret_key = slack_secret_key
        self.send_slack_notification_YN = op_kwargs.get('send_slack_notification_YN','Y')

    def execute(self, context):
        execution_date = context["ds"]
        logger.info("context of execution date {x}".format(x=execution_date))
        vertica_hook = VerticaHook(vertica_conn_id=self.vertica_conn_id, resourcepool='AIRFLOW_ALL_STD')
        con = vertica_hook.get_conn()
        env = self.env
        # slack_api_key = BaseHook.get_connection(self.slack_secret_key)
        config_df = get_config_data_by_source(con, self.run_freq, env)
        list_source = get_xcom(self.task_id, **context)

        slack_channel = Variable.get("data_imports_slack_channel")
        slack_watchers = Variable.get("data_imports_slack_watchers").split(',')
        slack_topic_arn = Variable.get("slack_topic_arn")

        config_df = config_df[config_df.source.isin([i[0] for i in list_source])]
        logger.info(config_df)
        config_list = [row.to_dict() for (_, row) in config_df.iterrows()]
        logger.info(config_list)
        file_list = get_xcom_file_list(**context)
        logger.info(file_list)
        for row in config_list:
            s3_bucket_name = self.s3_bucket_name
            source = row["source"]
            schema_name = row["schema_name"]
            final_table_name = row["schema_name"] + '.' + row["final_table_name"]
            ods_table_name = row["schema_name"] + '.' + row["ods_table_name"]
            ignore_copy_errors_yn = row["ignore_copy_errors_yn"]
            no_pk_tables_yn = row["no_pk_tables_yn"]
            custom_primary_key = row["custom_primary_key"]
            etl_update_time = row["etl_update_time"]
            days_to_dedupe = row["days_to_dedupe"]
            ignore_column = row["ignore_column"]
            history_yn = row["history_yn"]

            if ignore_copy_errors_yn == 'Y':
                copy_params = "DELIMITER '~' REJECTED DATA AS TABLE {schema_name}.s3_copy_rejects_{env} direct".format(schema_name=schema_name,env=env)
            else:
                copy_params = "DELIMITER '~' ABORT ON ERROR REJECTED DATA AS TABLE {schema_name}.s3_copy_rejects_{env} direct".format(schema_name=schema_name,env=env)
            column_list = get_column_names(row)
            key_list = file_list[source]
            logger.info(key_list)
            load_data(con, source, ods_table_name, copy_params, s3_bucket_name, key_list, column_list,env, self.send_slack_notification_YN,self.dag_id, self.task_id,slack_channel,slack_watchers,slack_topic_arn)
            row['dag_id'] = self.dag_id
            row['task_id'] = self.task_id
            row['studio'] = 'skill'
            # loading to data to main table.
            if ignore_column is not None:
                row['ignore_column'] = ignore_column

            if no_pk_tables_yn == 'Y':
                tsql = get_template_sql('t_update_no_pk_prod_table.sql', row)
            elif custom_primary_key is not None:
                row['pk'] = custom_primary_key.split(',')
                tsql = get_template_sql('t_update_custom_pk_table.sql', row)
            elif etl_update_time is not None:
                row['etl_update_time'] = etl_update_time
                row['days_to_dedupe'] = 90 if days_to_dedupe is None else days_to_dedupe
                row['ds'] = execution_date
                tsql = get_template_sql('update_prod_table_with_timestamp.sql', row)
            else:
                tsql = get_template_sql('t_update_prod_table.sql', row)

            logger.info(tsql)
            with con.cursor() as cursor:
                try:
                    cursor.execute(tsql)
                except Exception as err:
                    err = str(err)
                    err = err.replace('"', '^').replace("'", "^")
                    fns = "'" + "','".join([i.split('^')[0] for i in key_list]) + "'"
                    error_update = f"""Update ww.data_import_logs set final_table_errors = '{err}'
                                       Where source = '{source}'
                                          and environment = '{env}'
                                          and (file_name, insert_dttm) 
                                          in (Select file_name, max(insert_dttm) from ww.data_import_logs 
                                               where environment = '{env}'
                                                and file_name in ({fns}) 
                                                group by 1)"""
                    cursor.execute(error_update)
                    message = f"WW Data Import - {source} failed with error {err}"
                    color = '#EA260B'
                    message_title = f"WW import Status for {env}"
                    if self.send_slack_notification_YN == 'Y':
                        print(context["dag"])
                        print(context["task"])
                        print(context["task_instance"])
                        send_sns_slack_notification(slack_channel,
                                                    message_title,
                                                    message,
                                                    color,
                                                    slack_watchers,
                                                    self.dag_id,
                                                    slack_topic_arn,
                                                    self.task_id)
                        #send_slack_notification(slack_api_key.password, 'data-morpheus-vertica', message_title,
                        #                         message, color, ['rganesan'])
            if history_yn == 'Y':
                hsql = get_template_sql('t_insert_history_table.sql', row)
                with con.cursor() as cursor:
                    cursor.execute(hsql)
        con.commit()


def load_data(conn, source, target_table, copy_params, bucket, key_list, columns_list,env,ssn_yn,dag_id,task_id,slack_channel,slack_watchers,slack_topic_arn):
    with conn.cursor() as cursor:
        truncate_stmt = 'TRUNCATE TABLE {target_table};'.format(target_table=target_table)
        logger.info('Executing: {}'.format(truncate_stmt))
        cursor.execute(truncate_stmt)

        
# TODO change the table and columns names if needed once the credentials are in the vertica db
        # set_credentials_stmt = ("SELECT AWS_SET_CONFIG('aws_id', aws_access_key_id), "
        #                         "AWS_SET_CONFIG('aws_secret', aws_secret_access_key) FROM credentials;")
        set_credentials_stmt = ("ALTER SESSION SET UDPARAMETER FOR awslib aws_secret='pU8MloHRMPKOgbyAkPr4ASfuykeoY4mx6/10il2n';"
                                "ALTER SESSION SET UDPARAMETER FOR awslib aws_session_token='IQoJb3JpZ2luX2VjEG0aCXVzLWVhc3QtMSJIMEYCIQDvfO2imgoO4xqR3PRTyMNey82iXVzwVMjXzpFo8DPcSQIhAMxnILlei/EF+2sglfjPxtgSfawq0YwXAZ+fLQjxuzC+KpkDCCYQABoMMjgwNzc1MTE4NjMzIgyxVQOUEdwly4cowDYq9gKk8EkHSOSDTsHnIlEYTj1RPoPKZmIfLjUcZbIfL9bcdQVE++voqMqz2nGu7LlvgHjB0kvgyLHu7uenar9Db0ZUmuAY0nDiyjFG/uOY45lTFvlY/aASnjzU4+vJqPdkbd0E9bAJdO0evcWkkSRz0gNmFRxVQ9aviHQqeIJX1P1ye+9vatZtN656dpmZZrArn2rpoutojb2cGy6kPaya1Ao+XXzgWI81RX3K9vS9kY1e+WEXW170ywMIlpt2ooq93eOQnxt4JIrRfDHA5BQICBlYnJ3fbQPhU225erPbiEmdCWt0Y9AFs4c4pyhr4g7ZHxHjGoF+yqGOvKqSTQ55Bferjpa5ye6bH1saixxgxrOV8FhwDpV7f/faBR7fL1EDjxhKcPs+5Nwz5V8jUCgGCkXGbr1Z2aD1zZlIvaxML88yADxtEZrkyjKKno0qfHBo662s23hUqydnTU/PBivrkKBWzbaAsr1SYvmUlBzPK1L5YxL75LQ4jjC24reMBjqlAda0vCYDAuXp3jyyotB6NxG+FbsrB2FgcdPGV3v55GVdUp+aQCMK5P45+m9lyGcC44NBbWEOYOBdyfB+/YC5xlXW/EQXijhceW8x9C+JYzTC6ctz7vZDFlyvrLgMMUWKat+DACx0YJDvV9aYKqiUBLpqU3xiurnNUSqb1CObr9NRuvmx1WUGqAdEgakFF5CTAaH59oqKfWc4xxC8dqnUYQe9fwSxkw==';"
                                "ALTER SESSION SET UDPARAMETER FOR awslib aws_id='ASIAUCX4AJ4UY43T26XZ';")
        logger.info('Executing: {}'.format(set_credentials_stmt))
        cursor.execute(set_credentials_stmt)
        logger.info(cursor.fetchall())
        columns = ','.join(columns_list)
        for key1 in key_list:
            logger.info(key1)
            key = key1.split('^')[0]
            file_date = key.split('/')[0]
            file_hour = key.split('/')[1]
            file_size = int(key1.split('^')[1])
            try:
                if file_size > 0:
                    copy_stmt = "COPY {target_table} ({columns}) FROM 's3://{bucket}/{key}' {copy_params}".format(
                        target_table=target_table, columns=columns,
                        bucket=bucket, key=key, copy_params=copy_params)
                    logger.info('Executing: {}'.format(copy_stmt))
                    cursor.execute(copy_stmt)
                    copy_affected_rows = cursor.fetchone()[0]
                    logger.info('Copied: {} rows'.format(copy_affected_rows))
                    rec_cnt_reject = 0
                    try:
                        # Attemtp to get failed row number
                        logger.info('Executing: get_num_rejected_rows')
                        cursor.execute("select get_num_rejected_rows();")
                        rec_cnt_reject = cursor.fetchone()[0]
                    except Exception as err:
                        err = str(err)
                        err = err.replace('"', '^').replace("'", "^")
                        logger.error(err)
                    update_key_stmt = f"UPDATE {target_table} SET s3_key = '{key}' WHERE s3_key IS NULL; COMMIT;"
                    logger.info(f'Executing: {update_key_stmt}')
                    cursor.execute(update_key_stmt)
                    log_insert = f"""Insert into ww.data_import_logs(source, file_name, file_date, file_hour, file_size, num_of_records_accepted, num_of_records_rejected, success_yn, insert_dttm, environment)
                                    values ('{source}','{key}',to_date('{file_date}','YYYY-MM-DD'),{file_hour},{file_size},{copy_affected_rows},{rec_cnt_reject},'Y',current_timestamp,'{env}')"""
                    cursor.execute(log_insert)
                else:
                    log_insert = f"""Insert into ww.data_import_logs(source, file_name, file_date, file_hour, file_size, num_of_records_accepted, success_yn, insert_dttm, environment)
                                     values ('{source}','{key}',to_date('{file_date}','YYYY-MM-DD'),{file_hour},{file_size},0,'Y',current_timestamp,'{env}')"""
                    cursor.execute(log_insert)
            except Exception as err:
                err = str(err)
                err = err.replace('"', '^').replace("'", "^")
                logger.error(err)
                error_insert = f"""Insert into ww.data_import_logs(source, file_name, file_date, file_hour, file_size, errors, success_yn, insert_dttm, environment)
                                                        values ('{source}','{key}',to_date('{file_date}','YYYY-MM-DD'),{file_hour},{file_size},'{err}','N',current_timestamp,'{env}')"""
                cursor.execute(error_insert)
                message = f"WW Data Import - {source} failed with error {err}"
                color = '#EA260B'
                message_title = f"WW import Status for {env}"
                if ssn_yn == 'Y':
                    send_sns_slack_notification(slack_channel,
                                                message_title,
                                                message,
                                                color,
                                                slack_watchers,
                                                dag_id,
                                                slack_topic_arn,
                                                task_id)
                    # send_slack_notification(slack_api_key.password, 'data-morpheus-vertica', message_title,
                    #                         message, color, ['rganesan'])
    logger.info('Copy is complete!')
