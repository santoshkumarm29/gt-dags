"""Simple methods for string formatting.

"""
import numpy as np
import random
import string


def to_currency(value, precision=2):
    if np.isnan(value):
        return ''
    elif np.isinf(value):
        return '+inf'
    elif np.isneginf(value):
        return '-inf'
    try:
        return '${0:,.{1}f}'.format(float(value), precision)
    except ValueError as e:
        return '$0'
    except OverflowError as e:
        return ''


def to_currency_round(value):
    try:
        return '${0:,}'.format(int(round(value)))
    except ValueError as e:
        return '$0'
    except OverflowError as e:
        return ''


def to_percent(value, precision=2):
    if np.isnan(value):
        return ''
    elif np.isinf(value):
        return '+inf'
    elif np.isneginf(value):
        return '-inf'
    try:
        return '{0:,.{1}f}%'.format(100 * value, precision)
    except ValueError as e:
        return ''
    except OverflowError as e:
        return ''


def to_percent_round(value):
    try:
        return '{0:,}%'.format(int(round(100 * value)))
    except ValueError as e:
        return ''
    except OverflowError as e:
        return ''


def to_integer(value):
    if np.isnan(value):
        return ''
    elif np.isinf(value):
        return '+inf'
    elif np.isneginf(value):
        return '-inf'
    try:
        return '{0:,}'.format(int(value))
    except ValueError as e:
        return '0'
    except OverflowError as e:
        return ''


def to_float(value, precision=2):
    try:
        return '{0:,.{1}f}'.format(value, precision)
    except ValueError as e:
        return '{0:,.{1}f}'.format(0, precision)
    except OverflowError as e:
        return ''


def get_random_id(length=16, characters=string.ascii_lowercase + string.digits):
    return ''.join(random.SystemRandom().choice(characters) for _ in range(length))
