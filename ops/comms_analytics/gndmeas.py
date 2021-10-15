# -*- coding: utf-8 -*-
"""
Created on Wed Jun 24 11:08:27 2020

@author: Meryll
"""

from datetime import datetime
import matplotlib.pyplot as plt
import os
import pandas as pd
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import dynadb.db as db
import ops.ipr.ewisms_meal as ewisms_meal
import volatile.memory as mem


output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../ipr'))


def get_gndmeas(start, end, mysql=True):
    if mysql:
        conn = mem.get('DICT_DB_CONNECTIONS')
        query  = "select site_code, ts from "
        query += "  (select * from {analysis}.marker_observations "
        query += "  where ts between '{start}' and '{end}' "
        query += "  ) obs "
        query += "inner join {common}.sites using (site_id) "
        query = query.format(start=start, end=end, common=conn['common']['schema'], analysis=conn['analysis']['schema'])
        df = db.df_read(query, resource='sensor_analysis')
        df.to_csv(output_path+'/input/gndmeas.csv', index=False)
    else:
        df = pd.read_csv(output_path+'/input/gndmeas.csv')

    return df


def gndmeas_received(releases, gndmeas):
    ts_start = releases.ts_start.values[0]
    ts_end = releases.ts_end.values[0]
    site_code = releases.site_code.values[0]
    df = gndmeas.loc[(gndmeas.ts >= ts_start) & (gndmeas.ts < ts_end) & (gndmeas.site_code == site_code), :]
    releases.loc[:, 'gndmeas_received'] = abs(int(df.empty)-1)
    return releases


def gndmeas_stat(df):
    ts = pd.to_datetime(df.ts_start.values[0]).strftime('%B %Y')
    site_code = df.site_code.values[0]
    expected = len(df)
    received = sum(df.gndmeas_received)
    stat = pd.DataFrame({'site_code': [site_code], 'ts': [ts], 'expected': [expected], 'received': [received]})
    return stat


def main(start, end, mysql=True):
    ewi_sched = ewisms_meal.releases(start, end, mysql)
    ewi_sched = ewi_sched.loc[ewi_sched.gndmeas == 1, :]

    gndmeas = get_gndmeas(start, end, mysql=mysql)
    
    ewi_sched_grp = ewi_sched.reset_index().groupby('index', as_index=False)
    gndmeas_sched = ewi_sched_grp.apply(gndmeas_received, gndmeas=gndmeas).reset_index(drop=True)
    gndmeas_sched.loc[:, 'month'] = gndmeas_sched.ts_start.dt.month
    gndmeas_sched.loc[:, 'year'] = gndmeas_sched.ts_start.dt.year
    
    stat = gndmeas_sched.groupby(['month', 'year', 'site_code'], as_index=False).apply(gndmeas_stat).reset_index(drop=True)
    
    return stat


###############################################################################
if __name__ == "__main__":
    run_start = datetime.now()
    
    start = pd.to_datetime('2018-11-20')
    end = pd.to_datetime('2021-07-09')
    stat = main(start, end, mysql=False)
    all_stat = stat.groupby('ts').sum().reset_index()
    
    fig = plt.figure()
    ax = fig.add_subplot()
    ax.bar(all_stat.ts, all_stat.expected)
    ax.bar(all_stat.ts, all_stat.received)
    
    runtime = datetime.now() - run_start
    print("runtime = {}".format(runtime))