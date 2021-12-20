"""Convenience functions for matplotlib plotting.
"""
import base64
from io import BytesIO
from matplotlib.ticker import FuncFormatter
import numpy as np


def rotate_xaxis_labels(ax, degrees=30):
    """Rotate the xaxis labels for the sub-plot `ax`.

    :param ax:          A matplotlib axis object.
    :param degrees:     Number of degrees to rotate, counter-clockwise is positive.
    """
    labels = ax.get_xticklabels()
    for label in labels:
        label.set_rotation(degrees)


def format_axis(ax, format_fn):
    """Format the matplotlib axis `ax` using the formatter function `format_fn`.

    :param ax:          Matplotlib xaxis or yaxis.
    :param format_fn:   Format function used by FuncFormatter.
    """
    ax.set_major_formatter(FuncFormatter(format_fn))


def format_axis_as_int(ax):
    """Format the matplotlib axis as integers.

    """
    def format_fn(value, pos=None):
        if np.isnan(value):
            return ''
        elif np.isinf(value):
            return '+inf'
        elif np.isneginf(value):
            return '-inf'
        return '{:,}'.format(int(value))
    format_axis(ax, format_fn)


def format_axis_as_int_word(ax):
    """Format the matplotlib axis using `humanize.intword`.
    
    Names of large numbers from https://en.wikipedia.org/wiki/Names_of_large_numbers
    """
    import humanize

    def format_fn(value, pos=None):
        if np.isnan(value):
            return ''
        s = humanize.intword(value)
        d = {
            'thousand': 'k',
            'million': 'M',
            'billion': 'B',
            'trillion': 'T',
            'quadrillion': 'P',
            'quintillion': 'E',
            'sextillion': 'Z',
            'septillion': 'Y',
            'octillion': 'O',
            'nonillion': 'N',
        }
        for word, abbr in d.items():
            s = s.replace(word, abbr)
        return s

    format_axis(ax, format_fn)


def format_axis_as_currency(ax, precision=2):
    """Format the matplotlib axis as currency.

    :param ax:          Matplotlib xaxis or yaxis.
    """
    def format_fn(value, pos=None):
        try:
            return "${{:,.{precision}f}}".format(precision=precision).format(float(value))
        except (ValueError, AttributeError) as _e:
            return str(value)
    format_axis(ax, format_fn)


def format_axis_as_currency_big(ax):
    """Format the matplotlib axis as large currency, no cents.

    :param ax:          Matplotlib xaxis or yaxis.
    """
    format_axis_as_currency(ax, precision=0)


def format_axis_as_currency_small(ax):
    """Format the matplotlib axis as small currency, three digits of precision.

    :param ax:          Matplotlib xaxis or yaxis.
    """
    format_axis_as_currency(ax, precision=3)


def format_axis_as_percent(ax, precision=2):
    """Format the matplotlib axis as a percent with `precision` digits.

    :param ax:          Matplotlib xaxis or yaxis.
    :param precision:   Number of digits to include in the formatted value.
    """
    def format_fn(value, position=None):
        if np.isnan(value):
            return ''
        elif np.isinf(value):
            return '+inf'
        elif np.isneginf(value):
            return '-inf'
        return '{{:.{precision}f}}%'.format(precision=precision).format(100*value)
    format_axis(ax, format_fn)


def get_bins_freedman(s):
    """Get the number of bins as determined by the Freedman-Diaconis rule.

    :param s:   A Pandas Series with the values to be binned.
    :return:    Integer number of bins.
    """
    n = len(s)
    bin_size = 2 * (s.quantile(0.75) - s.quantile(0.25)) * np.power(n, (-1./3))
    bins = np.ceil(1.0 * (s.max() - s.min()) / (bin_size + 0.001))
    return int(bins)


def get_bins_scotts(s):
    """Get the number of bins as determined by Scott's rule.

    :param s:   A Pandas Series with the values to be binned.
    :return:    Integer number of bins.
    """
    n = len(s)
    if n <= 1:
        return n
    bin_size = 3.5 * s.std() * np.power(n, (-1./3))
    bins = np.ceil(1.0 * (s.max() - s.min()) / (bin_size + 0.001))
    return int(bins)


def mpl_figure_to_base64(fig):
    """Return a base64 encode string of `fig` as a png.
    This is useful for including images inline in html.
    
    :param fig:    Matplotlib figure.
    :return:       base64 encode string.
    """
    img_data = BytesIO()
    fig.savefig(img_data, format='png')
    img_data.seek(0)  # rewind the data
    figdata_png = 'data:image/png;base64,' + base64.b64encode(img_data.getvalue()).decode('utf8')
    return figdata_png
