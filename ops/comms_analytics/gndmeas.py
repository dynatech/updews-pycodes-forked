# -*- coding: utf-8 -*-
"""
Created on Wed Jun 24 11:08:27 2020

@author: Meryll
"""

from datetime import datetime, timedelta, time
import matplotlib.pyplot as plt
import os
import pandas as pd
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import dynadb.db as db
import ewisms
import volatile.memory as mem


def get_gndmeas(start, end):
    conn = mem.get('DICT_DB_CONNECTIONS')
    query  = "select site_code, ts from "
    query += "  (select * from {analysis}.marker_observations "
    query += "  where ts between '{start}' and '{end}' "
    query += "  ) obs "
    query += "inner join {common}.sites using (site_id) "
    query = query.format(start=start, end=end, common=conn['common']['schema'], analysis=conn['analysis']['schema'])
    df = db.df_read(query, resource='sensor_analysis')
    return df


def gndmeas_received(releases, gndmeas):
    ts_start = releases.ts_start.values[0]
    ts_end = releases.ts_end.values[0]
    site_code = releases.site_code.values[0]
    df = gndmeas.loc[(gndmeas.ts >= ts_start) & (gndmeas.ts < ts_end) & (gndmeas.site_code == site_code), :]
    releases.loc[:, 'gndmeas'] = abs(int(df.empty)-1)
    return releases


def gndmeas_stat(df):
    ts = pd.to_datetime(df.ts_start.values[0]).strftime('%B %Y')
    site_code = df.site_code.values[0]
    expected = len(df)
    received = sum(df.gndmeas)
    stat = pd.DataFrame({'site_code': [site_code], 'ts': [ts], 'expected': [expected], 'received': [received]})
    return stat


def main(start, end):
    ewi_sched = ewisms.releases(start, end)
    ewi_sched = ewi_sched.loc[(ewi_sched.ts_start.dt.time >= time(8,0)) & (ewi_sched.ts_start.dt.time <= time(16,0)) & (ewi_sched.raising != 1), :]
    ewi_sched.loc[:, ['ts_start', 'ts_end']] = ewi_sched.loc[:, ['ts_start', 'ts_end']].apply(pd.to_datetime)
    ewi_sched.loc[:, 'ts_end'] = ewi_sched.ts_start
    ewi_sched.loc[ewi_sched.event == 1, 'ts_start'] = ewi_sched.loc[ewi_sched.event == 1, 'ts_end'] - timedelta(hours=4)
    ewi_sched.loc[ewi_sched.event != 1, 'ts_start'] = ewi_sched.loc[ewi_sched.event != 1, 'ts_end'] - timedelta(hours=8)
    
    gndmeas = get_gndmeas(start, end)
    
    ewi_sched_grp = ewi_sched.reset_index().groupby('index', as_index=False)
    gndmeas_sched = ewi_sched_grp.apply(gndmeas_received, gndmeas=gndmeas).reset_index(drop=True)
    gndmeas_sched.loc[:, 'month'] = gndmeas_sched.ts_start.dt.month
    
    stat = gndmeas_sched.groupby(['month', 'site_code'], as_index=False).apply(gndmeas_stat).reset_index(drop=True)
    
    return stat


###############################################################################
if __name__ == "__main__":
    run_start = datetime.now()
    
    start = pd.to_datetime('2020-07-01')
    end = pd.to_datetime('2020-10-01')
    stat = main(start, end)
    all_stat = stat.groupby('site_code').sum().reset_index()
    
    fig = plt.figure()
    ax = fig.add_subplot()
    ax.bar(all_stat.site_code, all_stat.expected)
    ax.bar(all_stat.site_code, all_stat.received)
    
    runtime = datetime.now() - run_start
    print("runtime = {}".format(runtime))