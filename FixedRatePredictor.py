from calendar import monthrange
from datetime import date
from math import ceil, floor
import os
from time import localtime, strftime

import certifi
from openpyxl import load_workbook
import pandas as pd
from pandas.tseries.offsets import BDay
from retry import retry
import tweepy
import urllib3
import yaml
import zipfile

# have to import matplotlib separately first
# then change away from x-using backend
# then import pyplot
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


# set the swap rate tenors we are interested in
terms = [2, 3, 4, 5, 10, 1]

bankpath = os.path.join('temp', 'yields', 'BLC Nominal daily data current month.xlsx')
govpath = os.path.join('temp', 'yields', 'GLC Nominal daily data current month.xlsx')
chartsave = 'chart.png'
dbpath = 'db.csv'
fileurl = 'https://www.bankofengland.co.uk/-/media/boe/files/statistics/yield-curves/latest-yield-curve-data.zip'
logpath = 'log.txt'
sheetname = '2. fwd curve'
zippath = os.path.join('temp', 'yield.zip')

# chart appearance
plotcolour = ['#00E87E', '#0C87F2', '#5000DB', '#F20F7B', '#E85200']
bgcolour = '#FAFAFD'
fgcolour = '#EEEEEE'
chartfont = {'fontname': 'Cabin'}


# custom error in case BoE website not updated
class OutdatedFileError(Exception):
    pass


@retry(OutdatedFileError, delay=60, backoff=5, max_delay=7500)
def getfile():
    writelog('Downloading file')
    projdir = os.path.dirname(os.path.realpath(__file__))
    http = urllib3.PoolManager(
        cert_reqs='CERT_REQUIRED',
        ca_certs=certifi.where())
    r = http.request('GET', fileurl)
    f = open(os.path.join(projdir, zippath), 'w+b')
    f.write(r.data)
    zip_ref = zipfile.ZipFile(os.path.join(projdir, zippath), 'r')
    zip_ref.extractall(os.path.join(projdir, 'temp', 'yields'))
    zip_ref.close()
    f.close()
    wb = load_workbook(filename=os.path.join(projdir, bankpath))
    ws = wb[sheetname]
    lastrow = str(wb[sheetname].max_row)
    filedate = ws['A'+lastrow].value.date()
    prevday = date.today() - BDay(1)

    config = yaml.safe_load(open("config.yml"))
    check_date = config['check_date']

    if check_date and filedate != prevday.date():
        writelog('Date not OK, retrying...')
        raise OutdatedFileError


def extractdata(wbpath):
    projdir = os.path.dirname(os.path.realpath(__file__))
    ws = load_workbook(os.path.join(projdir, wbpath))[sheetname]
    ws['A4'].value = "Dates"

    # openpyxl max_row is unreliable with blank rows
    # so work out the max row manually
    lastrow = ws.max_row
    for r in range(lastrow, 1, -1):
        if ws.cell(row=r, column=1).value is not None:
            lastrow = r
            break

    # same for columns
    lastcol = ws.max_column
    for c in range(lastcol, 1, -1):
        if ws.cell(row=4, column=c).value is not None:
            lastcol = c
            break

    cols = [c.value for c in ws[4] if c.value is not None]
    data = list()
    for r in range(6, lastrow):
        rowdata = list()
        for c in range(1, lastcol+1):
            rowdata.append(ws.cell(row=r, column=c).value)
        data.append(rowdata)

    df = pd.DataFrame(data, columns=cols)
    df['Dates'] = df['Dates'].dt.day
    df.set_index('Dates', inplace=True)
    terms.sort()
    df = df[terms].dropna()
    df /= 100

    return df


def makechart(dfs):
    writelog('Making chart')
    projdir = os.path.dirname(os.path.realpath(__file__))
    
    for i in range(len(terms)-len(plotcolour)):
        plotcolour.append(plotcolour[i])

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

        axs[-1].set_facecolor(bgcolour)
        axs[-1].set_prop_cycle('color', plotcolour)
        axs[-1].grid(color=fgcolour, linestyle='-', linewidth=0.5)

        dmin = df.index.values[0]
        # drpt = df.index.values[-1]
        today = date.today()
        dmax = monthrange(today.year, today.month)[1]

        axs[-1].set_xlim(1, dmax)
        axs[-1].plot(df)

        for j, col in enumerate(df):
            axs[-1].plot((dmin, dmax), (df[col][dmin], df[col][dmin]),
                         linestyle=":",
                         linewidth=1)

        # format axis labels
        plt.title(dfname, **chartfont)
        axs[-1].set_ybound(floor(axs[-1].get_ybound()[0] * 1000) / 1000,
                           ceil(axs[-1].get_ybound()[1] * 1000) / 1000)
        axs[-1].set_yticklabels(('{:1.2f}%'.format(x * 100) for x in axs[-1].get_yticks()),
                                **chartfont)
        axs[-1].set_xticklabels(('{:1.0f}'.format(x) for x in axs[-1].get_xticks()),
                                **chartfont)

        axs[-1].set_xlabel("Day", **chartfont)

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
                        color=plotcolour[j],
                        fontsize=12,
                        **chartfont)
            # label end of dashed line with rate from day one
            ax.annotate('  {:1.2f}%'.format(100 * df[col][dmin]),
                        xy=(dmax, df[col][dmin] - 0.015 * yrange),
                        xycoords='data',
                        color=plotcolour[j],
                        fontsize=10,
                        **chartfont)
            # label end of plotted line with current rate
            # first, work out if displacement needed to avoid clash
            labeloffset = 0
            ratediff = df[col][drpt] - df[col][dmin]

            if abs(ratediff) < 0.0002:
                labeloffset = -0.0002
            ax.annotate('  {:1.2f}%'.format(100 * df[col][drpt]),
                        xy=(drpt, df[col][drpt] + labeloffset),
                        xycoords='data',
                        color=plotcolour[j],
                        fontsize=10,
                        **chartfont)
    plt.tight_layout(rect=[-0.010, 0, 0.85 + 0.05 * len(dfs), 0.94])
    plt.subplots_adjust(wspace=0.20)
    plt.suptitle(strftime('Swap rates, %b %Y', localtime()), y=0.98, **chartfont)
    plt.savefig(os.path.join(projdir, chartsave), facecolor=bgcolour, edgecolor='none')


def tweetplot(imgpath):
    writelog('Tweeting plot')
    projdir = os.path.dirname(os.path.realpath(__file__))

    config = yaml.safe_load(open("config.yml"))
    twit_auth = config['twitter_login']

    auth = tweepy.OAuthHandler(twit_auth['consumer_key'], twit_auth['consumer_secret'])
    auth.set_access_token(twit_auth['access_key'], twit_auth['access_secret'])
    api = tweepy.API(auth)
    api.update_with_media(os.path.join(projdir, imgpath))


def writelog(logtext):
    projdir = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(projdir, logpath), 'a') as f:
        f.write('{} {}{}'.format(strftime('%Y-%m-%d %H:%M:%S', localtime()),
                                 logtext,
                                 '\n'))


if __name__ == '__main__':
    writelog('Starting up')

    config = yaml.safe_load(open("config.yml", "r"))

    if config['download_file']:
        getfile()

    bankdata = extractdata(bankpath)
    govdata = extractdata(govpath)
    chartdata = [('Bank', bankdata), ('Sovereign', govdata)]
    makechart(chartdata)

    if config['download_file']:
        tweetplot(chartsave)

    writelog('Done\n----------------')
