# -*- coding: utf-8 -*-
"""
Created on Wed Dec 15 10:19:42 2021

@author: Data Scientist 1
"""

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import dynadb.db as db
import ops.lib.lib as lib
import ops.lib.querydb as qdb
import ops.lib.raininfo as raininfo
import volatile.memory as mem

output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
conn = mem.get('DICT_DB_CONNECTIONS')

year = datetime.now().year
end = pd.to_datetime('{}-12-01'.format(year))
start = end - relativedelta(years=2)
mysql=True
to_csv=False


def invalidated_alert_stat():
    
    query = "select * from operational_triggers "
    query += "inner join operational_trigger_symbols using (trigger_sym_id) "
    query += "inner join trigger_hierarchies using (source_id) "
    query += "right join "
    query += "    (select * from alert_status "
    query += "	where ts_last_retrigger >= '{}-12-01' ".format(year-2)
    query += "	and ts_last_retrigger <= '{}-12-01' ".format(year)
    query += "    ) alert_status "
    query += "using (trigger_id) "
    query += "where trigger_source in ('rainfall', 'subsurface', 'surficial') "
    query += "order by ts_last_retrigger"
    df = db.df_read(query, connection='analysis')
    df = df.drop_duplicates('trigger_id')
    df.loc[:, 'year'] = df.ts_last_retrigger.apply(lambda x: (x+relativedelta(months=1)).year)
    total = df.groupby(['year', 'trigger_source'], as_index=False).apply(len).rename('num').reset_index()
    valid = df.loc[df.alert_status != -1, :].groupby(['year', 'trigger_source'], as_index=False).apply(len).rename('num').reset_index()

    fig = plt.figure()
    ax = fig.add_subplot()
    width = 0.35
    prev_total = ax.bar(np.arange(3)-width/2, total.loc[total.year == year-1, 'num'], color='#434348', alpha=0.5, width=width-0.02, label='total {}'.format(year-1))
    prev_valid = ax.bar(np.arange(3)-width/2, valid.loc[valid.year == year-1, 'num'], color='#434348', width=width-0.1, label='valid {}'.format(year-1))
    curr_total = ax.bar(np.arange(3)+width/2, total.loc[total.year == year, 'num'], color='#7CB5EC', alpha=0.5, width=width-0.02, label='total {}'.format(year))
    curr_valid = ax.bar(np.arange(3)+width/2, valid.loc[valid.year == year, 'num'], color='#7CB5EC', width=width-0.1, label='valid {}'.format(year))
    plt.xticks(np.arange(3), valid.loc[valid.year == year-1, 'trigger_source'])
    ax.bar_label(prev_valid, labels=valid.loc[valid.year == year-1, 'num'], padding=8)
    ax.bar_label(curr_valid, labels=valid.loc[valid.year == year, 'num'], padding=8)
    ax.bar_label(curr_total, labels=total.loc[total.year == year, 'num'], padding=8)
    ax.bar_label(prev_total, labels=total.loc[total.year == year-1, 'num'], padding=8)
    ax.legend(loc='upper right')
    fig.suptitle('Operational trigger count')


def stat_percentage(df, period='month'):
    sent_ontime = np.round(100*len(df.loc[(df.ts_due >= df.ts_written) & (~df.ts_written.isnull()), :])/len(df), 2)
    sent = np.round(100*len(df.loc[(~df.ts_written.isnull()), :])/len(df), 2)
    ts = pd.to_datetime(df.data_ts.values[0])
    year = ts.year
    month = ts.month
    if period == 'quarter':
        ts = '{year} Q{quarter}'.format(quarter=int(np.ceil(month/3)), year=year)
    elif period == 'month':
        ts = ts.strftime('%b %Y')
    else:
        ts = year
    temp = pd.DataFrame({'ts': [ts], 'sent': [sent], 'sent_ontime': [sent_ontime], 'year': [year], 'month': [month]})
    return temp

def rain_stat():
    site_names = lib.get_site_names()
    sched = lib.release_sched(start, end, mysql=mysql, to_csv=to_csv)
    
    sent_start = start - timedelta(hours=0.25)
    sent_end = end + timedelta(hours=4)
    sched.loc[:, 'ts_due'] = sched.data_ts + timedelta(minutes=30+15)
    
    rain_recipients = qdb.get_rain_recipients(mysql=mysql, to_csv=to_csv)
    rain_sent = qdb.get_rain_sent(sent_start, sent_end, mysql=mysql, to_csv=to_csv)
    rain_sent = rain_sent.loc[~rain_sent.ts_sent.isnull(), :]
    rain_sched = raininfo.ewi_sched(sched, rain_recipients, rain_sent, site_names)
    rain_sched = rain_sched.loc[rain_sched.data_ts >= '2020-11-01', :]
    rain_sched.loc[:, 'month'] = rain_sched.data_ts.dt.month
    rain_sched.loc[:, 'year'] = rain_sched.data_ts.dt.year
    stat = rain_sched.groupby(['month', 'year'], as_index=False).apply(stat_percentage).reset_index(drop=True)
    stat = stat.sort_values(['year', 'month'])
    fig = plt.figure()
    ax = fig.add_subplot()
    sent = ax.bar(stat.ts, stat.sent, color='#7CB5EC', label='sent', width = 0.85)
    ax.bar_label(sent, labels=list(map(lambda x: str(x)+'%', stat.sent)))
    ontime = ax.bar(stat.ts, stat.sent_ontime, color='#434348', label='sent on time', width = 0.75)
    ax.bar_label(ontime, labels=list(map(lambda x: str(x)+'%', stat.sent_ontime)))
    ax.legend(loc='lower right')
    fig.suptitle('Percent rainfall information sent according to protocol')
    
    
    
site_names = lib.get_site_names()
sched = lib.release_sched(start, end, mysql=mysql, to_csv=to_csv)

sent_start = start - timedelta(hours=0.25)
sent_end = end + timedelta(hours=4)
sched.loc[:, 'ts_due'] = sched.data_ts + timedelta(minutes=30+15)

rain_recipients = qdb.get_rain_recipients(mysql=mysql, to_csv=to_csv)
rain_sent = qdb.get_rain_sent(sent_start, sent_end, mysql=mysql, to_csv=to_csv)
rain_sent = rain_sent.loc[~rain_sent.ts_sent.isnull(), :]
rain_sched = raininfo.ewi_sched(sched, rain_recipients, rain_sent, site_names)
