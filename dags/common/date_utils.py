"""Date utility methods.

"""

from datetime import date, datetime, timedelta
import pytz
import six


def get_datetime_from_datestr(datestr, format='%Y-%m-%d'):
    return datetime.strptime(datestr, format)


"""Common utilities for dealing with dates, datetimes, timestamps, strings, etc.

"""
from datetime import date, datetime, timedelta
import pytz
import six


def get_previous_date_as_string(event_date, n_days=7):
    """Return a string representation in the format 'YYYY-MM-DD' for the date `n_days` before `event_date`.

    :param event_date:  String or datetime or date object.
    :param n_days:      Integer number of days.
    :return:            String date in the format 'YYYY-MM-DD'.
    """
    event_date = convert_to_datetime(event_date)
    return (event_date - timedelta(days=n_days)).strftime('%Y-%m-%d')


def date_iter(start=None, end=None, n_days=1, n_days_stride=1):
    """Generator the iterates over a range of dates.

    :param start:           If not None, use this as the lower end of the range and iterate forward.
    :param end:             If not None, use this as the upper end of the range and iterate backward.
    :param n_days:          Number of dates to iterate over.
    :param n_days_stride:   Number of days between each iterated date.
    """
    if start is None and end is None:
        raise ValueError('Must provide either a start or end date')
    if start is not None and end is not None:
        raise ValueError('Must provide either a start or end date, but not both')
    if n_days < 0:
        raise ValueError('n_days must be non-negative')
    if start is not None:
        start = get_normalized_date(start)
    elif end is not None:
        delta = (n_days - 1) * n_days_stride
        start = get_normalized_date(end) - timedelta(days=delta)
    for the_date in [start + timedelta(days=n * n_days_stride) for n in range(n_days)]:
        yield the_date


def date_tuple_iter(start, end, n_days=1, overlap=False):
    """Generator that iterates from start_date to end_date in increment of days.
    Each iteration yields a (start, end) tuple of datetime.date values.

    :param start:       datetime.date object, or string in 'YYYY-MM-DD' format
    :param end:    datetime.date object, or string in 'YYYY-MM-DD' format
    :param n_days:      integer number of days for each range tuple
    :param overlap:     If True, the start date will equal the end date of the previous tuple.
                        If False (default), the end date of a tuple will be n_days - 1 days from the start date.
                        This is the case when dates are inclusive.

    """
    start_date = convert_to_date(start)
    end_date = convert_to_date(end)
    if not overlap:
        end_date = end_date + timedelta(days=1)
    for d in range(0, (end_date - start_date).days, n_days):
        d0 = start_date + timedelta(days=d)
        d1 = min(d0 + timedelta(days=n_days), end_date)
        if not overlap:
            d1 = d1 - timedelta(days=1)
        yield (d0, d1)


def datetime_tuple_iter(start, end, delta=timedelta(days=1)):
    """Generator that iterates from start to end in increments of days.
    Each iteration yields a (start, end) tuple of datetime.datetime values.
    For subsequent tuples, the start datetime will be identical to the end datetime of the previous tuple.

    start           Start of the range.  Any object that can be coerced to a datetime object.
                    E.g., datetime, date, or string in 'YYYY-MM-DD' format.
    end             End of the range.  Any object that can be coerced to a datetime object.
    n_days          Integer number of days for each range tuple.

    """
    start = convert_to_datetime(start)
    end = convert_to_datetime(end)
    while True:
        d0 = start
        d1 = min(start + delta, end)
        yield d0, d1
        start = d0 + delta
        if start >= end:
            break


def datetime_to_timestamp(dt):
    """Return the number of seconds since the epoch as a float.

    :param dt:  A datetime object.
    :return:    (float) Number of seconds since the epoch.
    """
    epoch = datetime.utcfromtimestamp(0)
    delta = dt - epoch
    return delta.total_seconds()


def get_normalized_date(a_date=None):
    """Return a datetime.date object from `the_date` object.

    :param  a_date:     String (in YYYY-MM-DD format), datetime.date, or datetime.datetime object.
                        Defaults to today at timezone 'US/Pacific'.
    :return:            `datetime.date` object.
    """
    if isinstance(a_date, six.string_types):
        a_date = datetime.strptime(a_date, '%Y-%m-%d').date()
    elif isinstance(a_date, datetime):
        a_date = a_date.date()
    elif a_date is None:
        a_date = date_today_tz()
    if not a_date <= datetime.today().date():
        raise ValueError('a_date "{}" is later than today "{}"'.format(a_date, datetime.today().date()))
    return a_date


def get_normalized_date_string(input_date=None):
    """Return a string representation of the date `input_date` in format 'YYYY-MM-DD'.

    :param input_date:  If a string, ensure it is in the format 'YYYY-MM-DD' and return it.
                        If a datetime, return the date string as 'YYYY-MM-DD'.
                        If None, return the date string for yesterday UTC as 'YYYY-MM-DD'.
    :return:            A string representation of the date in format 'YYYY-MM-DD'.
    """
    if not input_date:
        # yesterday using UTC
        date_string = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
    elif isinstance(input_date, six.string_types):
        # normalize the date string
        date_string = datetime.strptime(input_date, '%Y-%m-%d').strftime('%Y-%m-%d')
    elif isinstance(input_date, (datetime, date)):
        date_string = input_date.strftime('%Y-%m-%d')
    else:
        raise ValueError('unhandled input type: {}'.format(input_date))
    return date_string


def convert_to_date(date_object, default=None):
    """Convert `date_object` to a datetime.date object.

    :param date_object: Thing to convert to a datetime.date value.
                        Either a string in the format 'YYYY-MM-DD', or a datetime.datetime object.
    :return:            datetime.date object
    """
    if not date_object:
        if default is None:
            return default
        else:
            date_object = default
    if isinstance(date_object, six.string_types):
        return datetime.strptime(date_object, "%Y-%m-%d").date()
    elif isinstance(date_object, datetime):
        return date_object.date()
    elif isinstance(date_object, date):
        return date_object
    else:
        raise ValueError('convert_to_date: Unhandled argument: {}'.format(date_object))


def convert_to_datetime(date_object, default=None):
    """Convert `date_object` to a datetime.datetime object.

    :param date_object: Thing to convert to a datetime.date value.
                        Either a string in the format 'YYYY-MM-DD', or a datetime.date object.
    :return:            datetime.datetime object
    """
    if not date_object:
        if default is None:
            return default
        else:
            date_object = default
    if isinstance(date_object, six.string_types):
        return datetime.strptime(date_object, "%Y-%m-%d")
    elif isinstance(date_object, date):
        return datetime.combine(date_object, datetime.min.time())
    elif isinstance(date_object, datetime):
        return date_object
    else:
        raise ValueError('string_to_date: Unhandled argument: {}'.format(date_object))


def date_today_utc():
    """Return a datetime.date object for today at UTC"""
    return date.today()


def date_today_tz(timezone='US/Pacific'):
    """Return a datetime.date object for today at `timezone`

    See `pytz.common_timezones` for a list of supported timezones.
    """
    return datetime.now(pytz.timezone(timezone)).date()
