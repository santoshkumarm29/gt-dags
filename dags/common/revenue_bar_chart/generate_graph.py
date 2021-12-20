import numpy as np
import matplotlib.pyplot as plt
from common.revenue_bar_chart.bar_chart_utils import get_revenue_tuple, get_app_names, get_bar_bits, get_bottom, get_short_rev_num,\
get_total_rev_for_bar, get_y_tick_labels, get_5_prev_dates_to_4_weeks

app_list = [
    {
        "name": 'Tripeaks',
        "vertica_name": 'TriPeaks Solitaire',
        "color": '#fbc9ec',
        "text_color": '#666666',
        "revenue": (217000, 199000, 222000, 205000, 218000)
    },
    {
        "name": 'Bash Mobile',
        "vertica_name": 'Bingo Bash Mobile',
        "color": '#fecd7f',
        "text_color": '#666666',
        "revenue": (126000, 145000, 138000, 160000, 125000)
    },
    {
        "name": 'Bash Canvas',
        "vertica_name": 'Bingo Bash Canvas',
        "color": '#fffcc5',
        "text_color": '#666666',
        "revenue": (55000, 61000, 58000, 62000, 52000)
    },
    {
        "name": 'GSN Casino',
        "vertica_name": 'GSN Casino',
        "color": '#aad4fc',
        "text_color": '#666666',
        "revenue": (54000, 60000, 67000, 70000, 77000)
    },
    {
        "name": 'GSN Canvas',
        "vertica_name": 'GSN Canvas',
        "color": '#d5feff',
        "text_color": '#666666',
        "revenue": (30000, 30000, 30000, 30000, 30000)
    },
    {
        "name": 'WW Web',
        "vertica_name": 'WorldWinner Web',
        "color": '#b6f8af',
        "text_color": '#666666',
        "revenue": (30000, 30000, 30000, 30000, 30000)
    },
    # {
    #     "name": 'GSN.com',
    #     "vertica_name": 'GSN.com',
    #     "color": '#e7fedb',
    #     "text_color": '#666666',
    #     "revenue": (30000, 30000, 30000, 30000, 30000)
    # },
    {
        "name": 'WW App',
        "vertica_name": 'WorldWinner App',
        "color": '#fed1d0',
        "text_color": '#666666',
        "revenue": (30000, 30000, 30000, 30000, 30000)
    },
    {
        "name": 'Grand Casino',
        "vertica_name": 'Grand Casino',
        "color": '#f8e2d7',
        "text_color": '#666666',
        "revenue": (30000, 30000, 30000, 30000, 30000)
    },
    {
        "name": 'WoF App',
        "vertica_name": 'Wheel of Fortune Slots 2.0',
        "color": '#d8c9a2',
        "text_color": '#666666',
        "revenue": (30000, 30000, 30000, 30000, 30000)
    },
    {
        "name": 'FDP',
        "vertica_name": 'Fresh Deck Poker',
        "color": '#fecbcf',
        "text_color": '#666666',
        "revenue": (30000, 30000, 30000, 30000, 30000)
    },
    {
        "name": 'G2 - Casino',
        "vertica_name": 'G2 - Casino',
        "color": '#e7cbfe',
        "text_color": '#666666',
        "revenue": (30000, 30000, 30000, 30000, 30000)
    },
    {
        "name": 'G2 - Bash',
        "vertica_name": 'G2 - Bash',
        "color": '#fad2c5',
        "text_color": '#666666',
        "revenue": (30000, 30000, 30000, 30000, 30000)
    },
]


def get_bar_chart(df, report_date_str, ww_flag=None):

    global app_list

    # this is meant to preserve current behavior by default
    if ww_flag is None:
        pass
    elif ww_flag is True:
        app_list = [app for app in app_list if app['name'].startswith("WW")]
    elif ww_flag is False:
        app_list = [app for app in app_list if not app['name'].startswith("WW")]

    # Replace dummy rev values with actual values
    for app in app_list:
        actualRev = get_revenue_tuple(df, app['vertica_name'])
        app['revenue'] = actualRev

    dpi = 96
    w = 960 / dpi
    fig, ax = plt.subplots(figsize=(w, w), dpi=dpi)

    # Get rid of the spines on the top and right
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.spines['left'].set_color('#eeeeee')
    ax.spines['bottom'].set_color('#eeeeee')
    ax.tick_params(axis=u'both', which=u'both', length=0)

    # Start building the graph
    ind = np.arange(0, 10, 2)  # 5 stacked bars
    width = 1.7  # the width of the bars: can also be len(x) sequence

    # Add bars
    # barList will be a two dimensional array:
    # barList[rowNum][colNum] represents a bar (rectangle matplotlib object) with
    # each rowNum being a distinct studio and each colNum being a distinct time period (today, 7 days ago, etc)
    bar_list = []
    for idx, app in enumerate(app_list):
        app_bar = plt.bar(ind, app['revenue'], width, color=app['color'], bottom=get_bottom(idx, app_list))
        bar_list.append(app_bar)

    # Find the highest Y tick to make
    max_y = 0
    for i in range(0, 5):
        max_rev = get_total_rev_for_bar(i, app_list)
        if max_rev > max_y:
            max_y = max_rev
    max_y = round(max_y, -5) + 200000

    # Get list of previous dates
    prev_dates_list = get_5_prev_dates_to_4_weeks(report_date_str)

    # Set up ticks and axes
    plt.ylabel('IAP Revenue', weight="bold", fontsize="16", color="#333333")
    plt.xticks(ind, prev_dates_list, weight="600", fontsize="12",
               color="#666666")
    plt.yticks(np.arange(0, max_y, 100000), get_y_tick_labels(np.arange(0, max_y, 100000)), color="#666666",
               weight="600")
    plt.legend((get_bar_bits(bar_list)), (get_app_names(app_list)), bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

    # Add labels for first 4 graphs
    for idx in range(0, 4):

        if idx >= len(app_list):
            continue

        app = app_list[idx]

        # Create labels
        name_labels = [app['name'] for i in ind]
        name_rev_labels = ["%s" % get_short_rev_num(i) for i in app['revenue']]

        # Add labels to graph
        for idx2, rect in enumerate(bar_list[idx]):
            height = rect.get_height()
            name_label = name_labels[idx2]
            name_rev_label = name_rev_labels[idx2]
            ax.text(rect.get_x() + rect.get_width() / 2, height / 2 + rect.get_y(), name_label,
                    ha='center', va='bottom', color=app['text_color'], weight="bold", fontsize="10")
            ax.text(rect.get_x() + rect.get_width() / 2, height / 2 - 20000 + rect.get_y(), name_rev_label,
                    ha='center', va='bottom', color=app['text_color'], weight="bold", fontsize="10")

    # Print totals
    for i in range(0, 5):
        total_rev = get_total_rev_for_bar(i, app_list)
        bar_rev_str = get_short_rev_num(total_rev)
        y_loc = total_rev + 15000  # 15000 for spacing
        x_loc = bar_list[0][i].get_x() + bar_list[0][i].get_width() / 2  # could replace 0 with 1-11 -- doesn't matter
        ax.text(x_loc, y_loc, bar_rev_str, ha='center', va='bottom', color="black", weight="500", fontsize="20")

    return fig
