from datetime import datetime, timedelta
import numpy as np
import os
import pandas as pd
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import ops.lib.lib as lib
import ops.lib.querydb as qdb
import ops.lib.sms as sms
import ops.ipr.sms as ipr_sms


def ewi_stats(df, period='month'):
    sent = np.round(100 * (1 - len(df.loc[(df.ts_sent.isnull()) | (df.ts_sent> df.ts_due), :]) / len(df)), 2)
    queued = np.round(100 * (1 - len(df.loc[(df.ts_written.isnull()) | (df.ts_written > df.ts_due), :]) / len(df)), 2)
    ts = pd.to_datetime(df.data_ts.values[0])
    if period == 'quarter':
        ts = '{year} Q{quarter}'.format(quarter=int(np.ceil(ts.month/3)), year=ts.year)
    elif period == 'month':
        ts = ts.strftime('%b %Y')
    else:
        ts = ts.strftime('%Y')
    temp = pd.DataFrame({'ts': [ts], 'sent': [sent], 'queued': [queued]})
    return temp

def sent_per_org(df):
    data_ts = df['data_ts'].values[0]
    ts_due = df['ts_due'].values[0]
    site_id = df['site_id'].values[0]
    org_name = df['org_name'].values[0]
    ts_written = min(df.ts_written)
    ts_sent = min(df.ts_sent)
    release = pd.DataFrame({'data_ts': [data_ts], 'site_id': [site_id], 'org_name': [org_name], 'ts_due': [ts_due], 'ts_written': [ts_written], 'ts_sent': [ts_sent]})
    return release


def main(start, end, mysql=True, to_csv=False, per_org=False, remove_org=False):
    sent_start = start - timedelta(hours=0.25)
    sent_end = end + timedelta(hours=4)
    site_names = lib.get_site_names()

    sched = lib.release_sched(start, end, mysql=mysql, to_csv=to_csv)
    per_release = sched.groupby('data_ts', as_index=False)
    sched = per_release.apply(ipr_sms.sending_sched)
    recipients = qdb.get_sms_recipients(mysql=mysql, to_csv=to_csv)
    sent = qdb.get_sms_sent(sent_start, sent_end, site_names, mysql=mysql, to_csv=to_csv)
    sent = sent.loc[~sent.ts_sent.isnull(), :]
    sched = sms.ewi_sched(sched, recipients, sent, site_names)
    if per_org:
        sched = sched.groupby(['data_ts', 'site_id', 'org_name'], as_index=False).apply(sent_per_org).reset_index(drop=True)
    if remove_org:
        sched = sched.loc[sched.org_name.isin(['lewc', 'blgu', 'mlgu', 'plgu']), :]
    sched.loc[:, 'month'] = sched.data_ts.dt.month
    sched.loc[:, 'year'] = sched.data_ts.dt.year
    stat = sched.groupby(['month', 'year'], as_index=False).apply(ewi_stats).reset_index(drop=True)
    sched.loc[:, 'quarter'] = np.ceil(sched.month/3)
    stat = stat.append(sched.groupby(['quarter', 'year'], as_index=False).apply(ewi_stats, period='quarter').reset_index(drop=True), ignore_index=True)
    stat = stat.append(sched.groupby('year', as_index=False).apply(ewi_stats, period='year').reset_index(drop=True), ignore_index=True)
    
    return sched, stat

###############################################################################
if __name__ == "__main__":
    run_start = datetime.now()
    
    start = pd.to_datetime('2021-11-01')
    end = pd.to_datetime('2022-02-01')
    sched, stat = main(start, end, per_org=True, remove_org=True)
    print(stat)
    print(np.round(100 * (1 - len(sched.loc[(sched.ts_written.isnull()) | (sched.ts_written > sched.ts_due), :]) / len(sched)), 2))

#    all_stat = stat.groupby('ts').sum().reset_index()
#    all_stat.loc[:, 'dt'] = pd.to_datetime(all_stat.ts)
##    #monthly comparison between diff yrs
##    all_stat.loc[:, 'dt'] = all_stat.dt.apply(lambda x: str(x)[5:7] + ' ' + str(x)[:4])
#    all_stat = all_stat.sort_values('dt')
#    #percent received / expected
#    all_stat.loc[:, 'percentage'] = np.round(100 * all_stat.received / all_stat.expected, 2).apply(lambda x: str(x) + '%')
#    
#    site_stat = stat.groupby('site_code').sum()
#    site_stat.loc[:, 'percentage'] = 100 * site_stat.received / site_stat.expected
#
#    import matplotlib.pyplot as plt
#    fig = plt.figure()
#    ax = fig.add_subplot()
#    expected = ax.bar(all_stat.ts, all_stat.expected)
#    received = ax.bar(all_stat.ts, all_stat.received)
#    ax.bar_label(expected, labels=all_stat.percentage, padding=8)
#    plt.xticks(rotation = 45)

#    import matplotlib.pyplot as plt
#    fig = plt.figure()
#    ax = fig.add_subplot()
#    # Dec 2020- 96.72%
#    month_list = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov']
#    percent2021 = [94.42, 97.15, 99.35, 83.82, 99.92, 98.43, 98.85, 98.85, 89.57, 98.95, 97.12]
#    ewi2021 = ax.bar(month_list, percent2021, color='#7CB5EC', label='2021', width = 0.85)
#    ax.bar_label(ewi2021, labels=list(map(lambda x: str(x)+'%', percent2021)))
#    percent2020 = [64.81, 75.74, 97.19, 64.71, 93.32, 96.36, 78.05, 86.29, 83.26, 94.72, 91.69]
#    ewi2020 = ax.bar(month_list, percent2020, color='#434348', label='2020', width=0.75)
#    ax.bar_label(ewi2020, labels=list(map(lambda x: str(x)+'%', percent2020)))
#    plt.axhline(y=88, color='k', linestyle='--')
#    ax.legend(loc='lower right')
#    fig.suptitle('Percent EWI SMS sent according to protocol')


    runtime = datetime.now() - run_start
    print("runtime = {}".format(runtime))