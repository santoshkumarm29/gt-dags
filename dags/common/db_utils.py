"""Helper functions for connecting to Vertica.

"""
import io
import csv
import logging
import os
import time

import sqlparse
import pandas as pd

from common.deprecated import deprecated


def get_dataframe_from_query(query, connection, **kwargs):
    t_start = time.time()
    logging.info("Executing sql:\n{}".format(query))
    df = pd.read_sql(query, connection, **kwargs)
    logging.info("Completed sql: {} seconds".format(time.time() - t_start))
    return df


@deprecated
def get_db_connection(params=None):
    """Get a connection object to the Vertica database.
    This uses vertica-python as the adapter.

    Note that when running in airflow, it is better to pass `VerticaHook(vertica_conn_id='vertica_conn').get_conn()`.

    FIXME: Make `params` a required argument since '~/.airflow_passwords.cfg' is no longer a supported file.
    Alternatively, we look for vertica credentials in the environment variables.

    """
    import vertica_python
    logging.getLogger().warn("when running in airflow, pass `VerticaHook(vertica_conn_id='vertica_conn').get_conn()`.")
    if not params:
        # FIXME: this password file is no longer supported
        # password_filepath = os.path.expanduser('~/.airflow_passwords.cfg')
        params = {
            'username': '',  # get_password(key='vertica_username', section='gsn_vertica', filepath=password_filepath),
            'password': '',  # get_password(key='vertica_password', section='gsn_vertica', filepath=password_filepath),
            'db_host': 'vertica.gsngames.com',
            'db_port': '5433',
            'db_name': 'db'
        }
    connection = vertica_python.connect(
        host=params['db_host'],
        port=int(params['db_port']),
        database=params['db_name'],
        user=params['username'],
        password=params['password'],
    )
    return connection


def vertica_copy(con, data, target_table, data_type='DataFrame', csv_data_column_list=None, rejected_pctdiff_expected=90, quoting='csv.QUOTE_MINIMAL', parser_stm=None, should_commit=True, should_abort_on_error=False):
    """Performs a Vertica copy statement from a pandas dataframe

    Converts the pandas dataframe into a csv string. Then it uses the copy method of the
    vertica_python connection to COPY the csv file into a table.

    :param data: the staging table to be inserted into the target table
    :type data: DataFrame | CSV
    :param data_type : DataFrame | CSV
    :type data_type: string
    :param csv_data_column_list : columns in order based on data
    :type csv_data_column_list: list
    :param con: database connection
    :type con: vertica_python connection
    :param target_table: the table being inserted into
    :type target_table: str
    """

    # copy data from the dataframe  table to the target table
    try:
        wrote, rejected, passed = vertica_copy_v2(con=con, data=data, data_type=data_type, csv_data_column_list=csv_data_column_list, target_table=target_table,rejected_pctdiff_expected=rejected_pctdiff_expected,
                           quoting=quoting,parser_stm=parser_stm, should_commit=should_commit, should_abort_on_error=should_abort_on_error )

        if not passed:
            raise Exception(
                f'Copy did not pass due to rows rejected: wrote {wrote} | rejected {rejected} | percentage diff threshold {rejected_pctdiff_expected}')
        if rejected > 0:
            logging.warning(
                f'Copy warning due to rows rejected: wrote {wrote} | rejected {rejected} | percentage diff threshold {rejected_pctdiff_expected}')

        return wrote, rejected, passed

    except Exception as err:
        logging.error(f'Error occured when copying data from the dataframe into {target_table} error : {err}')
        raise err



def vertica_copy_v2(con, data, data_type, csv_data_column_list, target_table, rejected_pctdiff_expected=0, quoting='csv.QUOTE_NONE', parser_stm=None, should_commit=True, should_abort_on_error=False):
    """Performs a Vertica copy statement from a pandas dataframe

    Converts the pandas dataframe into a csv string. Then it uses the copy method of the
    vertica_python connection to COPY the csv file into a table.

    :param con: database connection
    :type con: vertica_python connection
    :param data: the staging table to be inserted into the target table
    :type data: DataFrame | CSV
    :param data_type : DataFrame | CSV
    :type data_type: string
    :param csv_data_column_list : columns in order based on data
    :type csv_data_column_list: list
    :param target_table: the table being inserted into
    :type target_table: str
    :param rejected_pctdiff_expected: as a unit of percentage example 50 instead of 0.5 to denote 50%
    :type rejected_pctdiff_expected: float or int
    :param parser_stm: example : fcsvparser(headers='true', reject_on_materialized_type_error='true') etc
    :type parser_stm: bool
    :param should_commit
    :type should_commit: bool
    :param should_abort_on_error
    :type should_abort_on_error: bool
    """

    def pct_diff(t=0, r=0 ):
        """
        :param t: int value total rows
        :param r: int value rows written
        :return: float value as a percentage unit
        """

        if t == 0 or (t - r) == 0:
            return -1

        return abs(t - r) / abs( t + r) * 200


    cur = con.cursor()

    # create a csv string from the dataframe
    parser = """ DELIMITER ',' ENCLOSED BY '"' ESCAPE AS '\\' """ if parser_stm is None else f""" PARSER {parser_stm} """
    copy_params = ('' if not should_abort_on_error else ' ABORT ON ERROR ') + f' DIRECT REJECTED DATA AS TABLE {target_table}_rejected_data '


    csv_quote_map = {
        'csv.QUOTE_NONE' : csv.QUOTE_NONE,
        'csv.QUOTE_MINIMAL': csv.QUOTE_MINIMAL,
        'csv.QUOTE_NONNUMERIC': csv.QUOTE_NONNUMERIC,
        'csv.QUOTE_ALL': csv.QUOTE_ALL,
    }

    string_buffer = io.StringIO()
    if data_type == 'DataFrame':
        dcol_list = [x for x in data.columns]
        data.to_csv(string_buffer, header=False, index=False, encoding='utf-8', columns=dcol_list, quoting=csv_quote_map[quoting], escapechar='\\', doublequote=False)
        columns = ', '.join(data.columns)
    elif data_type == 'CSV':
        csv_writer = csv.writer(string_buffer)
        csv_writer.writerows(data)
        columns = ', '.join(csv_data_column_list)
    else:
        return 0, 0, False
    string_buffer.seek(0)



    cur.copy(f"""
    COPY {target_table} (
            {columns}
    )
    FROM STDIN {parser} {copy_params};""", string_buffer)

    cur.execute("select get_num_accepted_rows();")
    wrote = cur.fetchone()[0]
    cur.execute("select get_num_rejected_rows();")
    rejected = cur.fetchone()[0]

    if should_commit:
        con.commit()


    return wrote, rejected, ( float(abs(rejected_pctdiff_expected)) > pct_diff((wrote + rejected), wrote  ) )
