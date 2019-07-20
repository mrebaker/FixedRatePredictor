"""
FixedRatePredictor

Gets files from the Bank of England website and charts the recent history of swap rates
Should predict movements in fixed rates but doesn't do that yet.
"""


from calendar import monthrange
from datetime import date
from math import ceil, floor
import os
from time import localtime, strftime
import zipfile

import certifi
import urllib3
import yaml
import pandas as pd

from pandas.tseries.offsets import BDay
from retry import retry
import slack
import tweepy
from openpyxl import load_workbook


# have to import matplotlib separately first
# then change away from x-using backend
# then import pyplot
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


# set the swap rate tenors we are interested in
TERMS = [2, 3, 4, 5, 10, 1]

BANK_PATH = os.path.join('temp', 'yields', 'BLC Nominal daily data current month.xlsx')
GOV_PATH = os.path.join('temp', 'yields', 'GLC Nominal daily data current month.xlsx')
CHART_SAVE = 'chart.png'
DB_PATH = 'db.csv'
FILE_URL = 'https://www.bankofengland.co.uk/-/media/boe/files/statistics/yield-curves/latest-yield-curve-data.zip'
LOG_PATH = 'log.txt'
SHEET_NAME = '2. fwd curve'
ZIP_PATH = os.path.join('temp', 'yield.zip')

# chart appearance
PLOT_COLOUR = ['#00E87E', '#0C87F2', '#5000DB', '#F20F7B', '#E85200']
BG_COLOUR = '#FAFAFD'
FG_COLOUR = '#EEEEEE'
CHART_FONT = {'fontname': 'Cabin'}


# custom error in case BoE website not updated
class OutdatedFileError(Exception):
    pass


@retry(OutdatedFileError, delay=60, backoff=5, max_delay=7500)
def get_file():
    write_log('Downloading file')
    projdir = os.path.dirname(os.path.realpath(__file__))
    http = urllib3.PoolManager(
        cert_reqs='CERT_REQUIRED',
        ca_certs=certifi.where())
    r = http.request('GET', FILE_URL)
    f = open(os.path.join(projdir, ZIP_PATH), 'w+b')
    f.write(r.data)
    zip_ref = zipfile.ZipFile(os.path.join(projdir, ZIP_PATH), 'r')
    zip_ref.extractall(os.path.join(projdir, 'temp', 'yields'))
    zip_ref.close()
    f.close()
    workbook = load_workbook(filename=os.path.join(projdir, BANK_PATH))
    worksheet = workbook[SHEET_NAME]
    lastrow = str(workbook[SHEET_NAME].max_row)
    filedate = worksheet['A'+lastrow].value.date()
    prevday = date.today() - BDay(1)

    config = yaml.safe_load(open("config.yml"))
    check_date = config['check_date']

    if check_date and filedate != prevday.date():
        write_log('Date not OK, retrying...')
        raise OutdatedFileError


def extract_data(wbpath):
    projdir = os.path.dirname(os.path.realpath(__file__))
    worksheet = load_workbook(os.path.join(projdir, wbpath))[SHEET_NAME]
    worksheet['A4'].value = "Dates"

    # openpyxl max_row is unreliable with blank rows
    # so work out the max row manually
    lastrow = worksheet.max_row
    for r in range(lastrow, 1, -1):
        if worksheet.cell(row=r, column=1).value is not None:
            lastrow = r
            break

    # same for columns
    lastcol = worksheet.max_column
    for c in range(lastcol, 1, -1):
        if worksheet.cell(row=4, column=c).value is not None:
            lastcol = c
            break

    cols = [c.value for c in worksheet[4] if c.value is not None]
    data = list()
    for r in range(6, lastrow+1):
        rowdata = list()
        for c in range(1, lastcol+1):
            rowdata.append(worksheet.cell(row=r, column=c).value)
        data.append(rowdata)

    df = pd.DataFrame(data, columns=cols)
    df['Dates'] = df['Dates'].dt.day

    df.set_index('Dates', inplace=True)
    TERMS.sort()
    df = df[TERMS].dropna()
    df /= 100

    return df


def make_chart(dfs):
    """
    Creates a chart from two input dataframes, and saves it to a PNG file.
    TODO: return filename
    :param dfs:
    :return: none
    '''
    """
    write_log('Making chart')
    projdir = os.path.dirname(os.path.realpath(__file__))

    for i in range(len(TERMS) - len(PLOT_COLOUR)):
        PLOT_COLOUR.append(PLOT_COLOUR[i])

    fig = plt.figure(figsize=(4 * len(dfs), 4))
    layouts = {1: (1, 1),
               2: (1, 2)}

    axs = list()

    for i, data in enumerate(dfs, 1):
        dfname = data[0]
        df = data[1]
        x, y = layouts[len(dfs)]
        if i % 2 == 0:
            ax1 = axs[-1]
            axs.append(fig.add_subplot(x, y, i, sharey=ax1))
            plt.setp(axs[-1].get_yticklabels(), visible=False)
        else:
            axs.append(fig.add_subplot(x, y, i))

        axs[-1].set_facecolor(BG_COLOUR)
        axs[-1].set_prop_cycle('color', PLOT_COLOUR)
        axs[-1].grid(color=FG_COLOUR, linestyle='-', linewidth=0.5)

        dmin = df.index.values[0]
        dmax = monthrange(date.today().year, date.today().month)[1]
        axs[-1].set_xlim(1, dmax)
        axs[-1].plot(df)

        for j, col in enumerate(df):
            axs[-1].plot((dmin, dmax), (df[col][dmin], df[col][dmin]),
                         linestyle=":", linewidth=1)

        # format axis labels
        plt.title(dfname, **CHART_FONT)
        axs[-1].set_ybound(floor(axs[-1].get_ybound()[0] * 1000) / 1000,
                           ceil(axs[-1].get_ybound()[1] * 1000) / 1000)
        axs[-1].set_yticklabels((f'{x*100 : 1.2f}%' for x in axs[-1].get_yticks()),
                                **CHART_FONT)
        axs[-1].set_xticklabels((f'{x : 1.0f}' for x in axs[-1].get_xticks()),
                                **CHART_FONT)
        axs[-1].set_xlabel("Day", **CHART_FONT)

    ymin, ymax = axs[-1].get_ylim()
    yrange = ymax - ymin

    for i, data in enumerate(dfs):
        df = data[1]
        ax = axs[i]
        dmin = df.index.values[0]
        drpt = df.index.values[-1]
        today = date.today()
        dmax = monthrange(today.year, today.month)[1]
        for j, col in enumerate(df):
            # label near end of dashed line with relevant term (2yr, 10yr etc)
            ax.annotate(str(col) + 'yr',
                        xy=(dmax - 0.5, df[col][dmin] + 0.0150 * yrange),
                        xycoords='data',
                        ha='right',
                        color=PLOT_COLOUR[j],
                        fontsize=12,
                        **CHART_FONT)
            # label end of dashed line with rate from day one
            ax.annotate('  {:1.2f}%'.format(100 * df[col][dmin]),
                        xy=(dmax, df[col][dmin] - 0.015 * yrange),
                        xycoords='data',
                        color=PLOT_COLOUR[j],
                        fontsize=10,
                        **CHART_FONT)
            # label end of plotted line with current rate
            # first, work out if displacement needed to avoid clash
            labeloffset = 0
            ratediff = df[col][drpt] - df[col][dmin]

            if abs(ratediff) < 0.0002:
                labeloffset = -0.0002
            ax.annotate('  {:1.2f}%'.format(100 * df[col][drpt]),
                        xy=(drpt, df[col][drpt] + labeloffset),
                        xycoords='data',
                        color=PLOT_COLOUR[j],
                        fontsize=10,
                        **CHART_FONT)

    plt.tight_layout(rect=[-0.010, 0, 0.85 + 0.05 * len(dfs), 0.94])
    plt.subplots_adjust(wspace=0.20)
    plt.suptitle(strftime('Swap rates, %b %Y', localtime()), y=0.98, **CHART_FONT)
    plt.savefig(os.path.join(projdir, CHART_SAVE), facecolor=BG_COLOUR, edgecolor='none')


def send_to_slack(imgpath):
    write_log('Sending plot to Slack')
    proj_dir = os.path.dirname(os.path.realpath(__file__))
    file_path = os.path.join(proj_dir, imgpath)

    config = yaml.safe_load(open("config.yml"))
    slack_token = config['slack_login']['bot_token']
    client = slack.WebClient(token=slack_token)

    response = client.files_upload(
        channels='CLN9YJ6H4',
        file=file_path)
    assert response["ok"]


def send_to_twitter(imgpath):
    write_log('Tweeting plot')
    projdir = os.path.dirname(os.path.realpath(__file__))

    config = yaml.safe_load(open("config.yml"))
    twit_auth = config['twitter_login']

    auth = tweepy.OAuthHandler(twit_auth['consumer_key'], twit_auth['consumer_secret'])
    auth.set_access_token(twit_auth['access_key'], twit_auth['access_secret'])
    api = tweepy.API(auth)
    api.update_with_media(os.path.join(projdir, imgpath))


def write_log(log_text):
    projdir = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(projdir, LOG_PATH), 'a') as f:
        log_time = strftime('%Y-%m-%d %H:%M:%S', localtime())
        f.write(f'{log_time} {log_text}\n')


if __name__ == '__main__':
    # todo: predictions
    # todo: fix first day of the month issue
    write_log('Starting up')

    config = yaml.safe_load(open("config.yml", "r"))

    if config['download_file']:
        get_file()

    bank_data = extract_data(BANK_PATH)
    gov_data = extract_data(GOV_PATH)
    chart_data = [('Bank', bank_data), ('Sovereign', gov_data)]
    make_chart(chart_data)

    if config['send_tweet']:
        send_to_twitter(CHART_SAVE)
    if config['send_slack']:
        send_to_slack(CHART_SAVE)

    write_log('Done\n----------------')
