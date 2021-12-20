import numpy as np
from datetime import datetime, timedelta


def get_revenue_tuple(df, vertica_name):
    return tuple(df.loc[df.app_name == vertica_name, :].iloc[:, 1:].values[0])


# Returns string with abbreviated revenue value
def get_short_rev_num(decimal_num):
    # Round Number to nearest 1000
    rounded_num = round(decimal_num, -3)
    # Truncate into String, add k
    if rounded_num>999999:
        rounded_num = round(decimal_num, -5)
        num_millions = rounded_num / 1000000
        return "$" + str(num_millions) + "M"
    elif rounded_num<1000:
        return "<$1k"
    else:
        numThousands = int( rounded_num / 1000 )
        return "$" + str(numThousands) + "K"


# Gets the bottom of the previous bar
def get_bottom(idx, rev_list):
    if idx == 0:
        return 0
    else:
        return np.array(rev_list[idx - 1]['revenue']) + get_bottom(idx - 1, rev_list)


# Returns list of app names from dict list
def get_app_names(app_list):
    app_names_list = []
    for app in app_list:
        app_names_list.append(app['name'])
    return app_names_list


# Returns rectangles for legend
def get_bar_bits(bar_list):
    legend_bar_list = []
    for bar in bar_list:
        legend_bar_list.append(bar[0])
    return legend_bar_list


def get_total_rev_for_bar(bar_num, app_dict_list):
    running_total = 0
    for app in app_dict_list:
        running_total += app['revenue'][bar_num]
    return running_total


def get_revenue_tuple(df, vertica_name):
    return tuple(df.loc[df.app_name == vertica_name, :].iloc[:, 1:].values[0])


def get_y_tick_labels(locArr):
    label_arr = []
    for loc in locArr:
        label_num = int(loc / 1000)
        label_text = str(label_num) + "K"
        label_arr.append(label_text)
    return label_arr


def get_5_prev_dates_to_4_weeks(report_date_str):

    report_datetime = datetime.strptime(report_date_str, '%Y-%m-%d')
    yesterday = datetime.strftime(report_datetime, '%m/%d')
    date_list = [yesterday]

    for i in range(1, 5):
        date_list.append(datetime.strftime(report_datetime - timedelta(weeks=i), "%m/%d"))

    return date_list
