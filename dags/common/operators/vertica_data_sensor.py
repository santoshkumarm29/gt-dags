"""Use the existence of data in a table as a sensor.
Supports daily or hourly granularity.

TODO:
- When doing hourly aggregation using a timestamp that observes daylight saving time,
  there will be one day a year where there are only 23 distinct hours represented.

"""
from airflow.hooks.base_hook import BaseHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from datetime import date, datetime, timedelta, timezone
import re
import logging
import pytz
from six import string_types
import sqlparse

from common.db_utils import get_dataframe_from_query
from common.hooks.vertica_hook import VerticaHook


class VerticaDataSensor(BaseSensorOperator):
    """Waits for data to be available in the target table at all levels of the specified granularity.

    :param conn_id: The connection to run the sensor against.
    :type conn_id: string
    :param table_name: The name of the table to run the sensor against.
    :type table_name: string
    :param time_dimension: The column name to use for time grouping.
    :type time_dimension: string
    :param metrics: A string or list of strings to use as column values.
    :type metrics: string or list of strings
    :param granularity: The granularity to check for data.  'hour' or 'day'.
                        If 'hour', each metric column must have 24 truthy values.
                        If 'day', each metric column must have 1 truthy value.
    :type granularity: string
    :param conditionals: Additional conditionals to include in the where clause.
    :type conditionals: None, string, or list of strings
    :param check_next_day: If True, also include validation for the next day.
    This is only supported for `granularity='day'`.
    :type check_next_day: Boolean
    :param days_offset: Number of days to offset the check.  If negative, look
    this many days before `ds`.  If positive, look after.
    :type days_offset: int
    :param ignore_dates: Optional list of dates to ignore.  Useful for dates with known issues.
    :type ignore_dates: datetime.date, list of datetime.date, string, list of strings, or None.
    :param tz_name: Name of the timezone used by `time_dimension`. This is used to determine
    DST changeovers when granularity=='hour'.  Set to 'UTC' to ignore DST changes.
    :type tz_name: String, from pytz.all_timezones
    :param resourcepool: Vertica resource pool
    :type reourcepool: str
    :param pool: Airlfow resource pool
    :type pool: str
    """
    ui_color = '#2232a5'

    @apply_defaults
    def __init__(self, vertica_conn_id, table_name, time_dimension, metrics, granularity='hour',
                 conditionals=None, check_next_day=False, days_offset=0, ignore_dates=None,
                 tz_name='America/Los_Angeles', resourcepool=None, pool='sensor_tasks', *args, **kwargs):
        self.vertica_conn_id = vertica_conn_id
        self.table_name = table_name
        self.time_dimension = time_dimension
        self.metrics = metrics
        self.granularity = granularity
        self.conditionals = conditionals
        self.check_next_day = check_next_day
        self.days_offset = days_offset
        self.ignore_dates = self.convert_ignore_dates_to_list(ignore_dates)
        self.tz_name = tz_name
        self.resourcepool = resourcepool
        if kwargs.get('timeout') is None:
            kwargs['timeout'] = 60*60*2  # 2 hour default timeout rather than the 7 day default
        if kwargs.get('poke_interval') is None:
            kwargs['poke_interval'] = 60*5  # 5 minute default interval rather than the default of 1 minute
        # TODO: validate that granularity is in valid
        # TODO: validate that check_next_day is valid given granularity
        super(VerticaDataSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        time_value = datetime.fromtimestamp(context['execution_date'].timestamp()).astimezone(timezone.utc)
        time_value = time_value.astimezone(pytz.timezone(self.tz_name))
        date_value = time_value.date()
        granularity = self.granularity
        hours_granularity_regex = 'hours\((-?\d+),(-?\d+)\)'
        if re.match(hours_granularity_regex, granularity):
            start_hour = int(re.search(hours_granularity_regex, granularity).group(1))
            end_hour = int(re.search(hours_granularity_regex, granularity).group(2))
            run_hour = time_value.hour
            if start_hour < 0:
                start_hour = max(0, run_hour + start_hour)
            if end_hour < 0:
                end_hour = max(0, run_hour + end_hour)
            granularity = f'hours({start_hour},{end_hour})'

        if self.days_offset != 0:
            date_value = date_value + timedelta(days=self.days_offset)
        if date_value in self.ignore_dates:
            logging.info('Passing on validation check because date value "{}" is in the ignore_dates list.'
                         .format(date_value.strftime('%Y-%m-%d')))
            return True
        sql = self.get_sql(
            table_name=self.table_name,
            time_dimension=self.time_dimension,
            date_value=date_value,
            metrics=self.metrics,
            granularity=granularity,
            conditionals=self.conditionals,
            check_next_day=self.check_next_day,
            label=self.label,
        )
        logging.info('Poking: ' + sql)
        connection = VerticaHook(vertica_conn_id=self.vertica_conn_id, resourcepool=self.resourcepool).get_conn()
        df = self.get_dataframe(sql, connection=connection)
        return self.validate_dataframe(df, granularity, date_value,
                                       check_next_day=self.check_next_day, tz_name=self.tz_name)

    @staticmethod
    def convert_ignore_dates_to_list(ignore_dates):
        """Convert the argument `ignore_dates` to a standardized list of `datetime.date` objects.

        :param ignore_dates: Can be None, a string, at `datetime.date`, or list of `datetime.date` or string objects.
        Strings are assumed to be in the format 'YYYY-MM-DD'.
        :return: List of `datetime.date` objects.  Empty if passed None.
        """
        def _convert_single_date(d):
            if isinstance(d, string_types):
                return datetime.strptime(d, '%Y-%m-%d').date()
            else:
                return d
        if ignore_dates is None:
            _ignore_dates = []
        elif isinstance(ignore_dates, string_types):
            # convert the single string 'YYYY-MM-DD' to a date object and put in a list
            _ignore_dates = [_convert_single_date(ignore_dates), ]
        elif isinstance(ignore_dates, date):
            _ignore_dates = [ignore_dates, ]
        else:
            # iterate through each ignore_date in the list and convert if necessary
            _ignore_dates = [_convert_single_date(ignore_date) for ignore_date in ignore_dates]
        return _ignore_dates

    @staticmethod
    def get_sql(table_name, time_dimension, date_value, metrics, granularity, conditionals, check_next_day, label):
        """Return the sql query used to validate the data.

        Useful for testing and debugging.
        """
        # put the metrics in the form of 'metric as metric_0,'
        if isinstance(metrics, string_types):
            columns = '{v} as metric_0'.format(v=metrics)
        else:
            columns = ',\n'.join('{v} as metric_{i}'.format(i=i, v=v) for i, v in enumerate(metrics))
        # handle granularity
        if granularity.startswith('hour'):
            grouper = "date_trunc('hour', {time_dimension})".format(time_dimension=time_dimension)
        elif granularity == 'day':
            grouper = "date_trunc('day', {time_dimension})".format(time_dimension=time_dimension)
        else:
            raise ValueError('granularity must be "day" or "hour(s)". Unsupported value: "{}"'.format(granularity))
        # handle conditionals
        additional_conditionals = ''
        if conditionals is not None:
            if isinstance(conditionals, string_types):
                additional_conditionals = conditionals
            else:
                additional_conditionals = ' and '.join(conditionals)
            additional_conditionals = ' and ' + additional_conditionals
        hours_granularity_regex = 'hours\((\d+),(\d+)\)'
        if re.match(hours_granularity_regex, granularity):
            start_hour = int(re.search(hours_granularity_regex, granularity).group(1))
            end_hour = int(re.search(hours_granularity_regex, granularity).group(2))
            additional_conditionals += f" and hour({time_dimension}) between {start_hour} and {end_hour}"
        if check_next_day:
            # ensure that data is also available for the next calendar day 
            date_values_check = "(date('{date_value}'), date('{date_value}') + interval '1 day')"
            date_values_check = date_values_check.format(date_value=date_value)
        else:
            date_values_check = "(date('{date_value}'))".format(date_value=date_value)
        query = """
        select /*+ LABEL ('{label}') */ 
          {grouper} as dim_column,
          {columns}
        from {table_name}
        where date({time_dimension}) in {date_values_check}
        {additional_conditionals}
        group by 1
        order by 1 asc
        """
        query = query.format(
            grouper=grouper,
            table_name=table_name,
            time_dimension=time_dimension,
            date_values_check=date_values_check,
            columns=columns,
            additional_conditionals=additional_conditionals,
            label=label,
        )
        query = sqlparse.format(query, reindent=True)
        return query

    @staticmethod
    def get_dataframe(sql, connection):
        logging.debug(sql)
        df = get_dataframe_from_query(sql, connection=connection)
        df = df.set_index(['dim_column'])
        return df

    @staticmethod
    def validate_dataframe(df, granularity, date_value=None, check_next_day=False, tz_name='America/Los_Angeles'):
        """Check the validity of the data from the table as represented by `df`.
        :param df: Dataframe returned by `get_dataframe`.
        :type df: pandas DataFrame
        :param granularity: The granularity to check for data.  'hour' or 'day'.
        If 'hour', it ensures there is data for each of the 24 hour buckets for each metric.
        If 'day' and `check_next_day` is False, it ensures there is one entry for each metric.
        If 'day' and `check_next_day` is True, it ensures there are two entries for each metric.
        :type granularity: string
        :param date_value: Date on which the validation is being done.  Used for DST calculations.
        :type date_value: `datetime.date`
        :param check_next_day: If True, also check the validity of the data for the next calendar day.
        This is only supported for `granularity`='day'.
        :type check_next_day: Boolean
        :param tz_name: Name of the timezone for the timestamp.  Used for DST calculations when
        granularity=='hour'.
        :type tz_name: String, see pytz.all_timezones
        """
        if granularity.startswith('hour'):
            if check_next_day:
                raise ValueError("check_next_day is not supported for granularity='hour'")
            if date_value is None:
                raise ValueError('date_value is required when granularity=="hour"')
            required_rows = VerticaDataSensor.get_distinct_hours_in_date(date_value, tz_name=tz_name)
            hours_granularity_regex = 'hours\((\d+),(\d+)\)'
            if re.match(hours_granularity_regex, granularity):
                start_hour = int(re.search(hours_granularity_regex, granularity).group(1))
                end_hour = int(re.search(hours_granularity_regex, granularity).group(2))
                if end_hour == 0:
                    return True
                required_rows = end_hour - start_hour + 1
        elif granularity == 'day':
            if check_next_day:
                required_rows = 2
            else:
                required_rows = 1
        else:
            raise ValueError('granularity must be "day" or "hour". Unsupported value: "{}"'.format(granularity))
        if len(df) == 0:
            # no results returned, silently return False
            return False
        elif (len(df) < required_rows and granularity.startswith('hour')) or (len(df) != required_rows and granularity == 'day'):
            # partial results, log the details then return False
            logging.warning('Got {} rows but expected {} rows.'.format(len(df), required_rows))
            logging.debug(df)
            return False
        else:
            # validate that each metric column has the required number of non-zero rows
            for metric_column in df.columns:
                # Note: `pandas.Series.count` returns the number of non-null rows.
                if df[metric_column].count() < required_rows:
                    logging.warning('Got {} non-null rows for metric "{}" but expected {} rows.'.format(
                        df[metric_column].count(), metric_column, required_rows))
                    logging.debug(df)
                    return False
                if not df[metric_column].all():
                    logging.warning('Got missing values for metric "{}".'.format(metric_column))
                    logging.debug(df)
                    return False
        return True

    @staticmethod
    def get_distinct_hours_in_date(date_value, tz_name='America/Los_Angeles'):
        """Return the number of distinct hours expected on `date_value`.
        This will return 23 on the switch to Daylight Saving Time in spring and 24 otherwise.

        :param date_value:  The `date` or `datetime` object to test.
        :param tz_name:     A timezone name supported by `pytz.all_timezones`
                            Change to 'UTC' for a timezone that does not observe DST.
        :return:            The number of distinct hours on this date considering DST.
        """
        tz = pytz.timezone(tz_name)
        if not isinstance(date_value, datetime):
            date_value = datetime(date_value.year, date_value.month, date_value.day)
        start_datetime = tz.localize(date_value)
        hours = {tz.normalize(start_datetime + timedelta(hours=i)) for i in range(24)}
        distinct_hours = {d for d in hours if d.date() == date_value.date()}
        return len(distinct_hours)
    
    @property
    def label(self):
        """Creates a Vertica Label based on the task ID and Dag ID"""
        studios = {'bingo': ['bingo', 'bash'],
                   'casino': ['grancasino', 'casino', 'casino'],
                   'solitaire': ['tripeaks', 'solitaire', 'smash'],
                   'skill': ['ww', 'phoenix', 'worldwinner'],
                   'vegas': ['wofs', 'idle', 'poker'],
                   }

        task_id = getattr(self, 'task_id')

        dag = getattr(self, 'dag', None)
        if dag is None:
            dag_id = 'dag_id'
        else:
            dag_id = dag.dag_id

        studio_id = None
        for studio in studios:
            for keyword in studios[studio]:
                if dag_id is not None and keyword in dag_id:
                    studio_id = studio
        if studio_id is None:
            studio_id = 'studio'

        return f'airflow-{studio_id}-{dag_id}-{task_id}'
    
    # added setter to resolve for cannot set attribute error
    @label.setter
    def label(self, value):
        self._label = value



