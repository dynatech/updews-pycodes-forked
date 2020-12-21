# -*- coding: utf-8 -*-
"""
Created on Wed Jun 24 11:08:27 2020

@author: Meryll
"""

from datetime import timedelta
import numpy as np
import pandas as pd
import re

import dynadb.db as db


def check_sending(df, releases):
    start = pd.to_datetime(df.ts_start.values[0])
    df = pd.merge(df, releases, how='left', on=['ts_start', 'site_code'])
    for i in df.loc[df.raising == 1, :].index:
        site_code = df.loc[df.index == i, 'site_code'].values[0]
        ts_start = df.loc[df.index == i, 'ts_start'].values[0]
        ts = releases.loc[(releases.site_code == site_code) & (releases.timestamp > pd.to_datetime(ts_start) - timedelta(hours=1)) & (releases.timestamp < pd.to_datetime(ts_start) + timedelta(minutes=100)), 'timestamp']
        if len(ts) != 0:
            df.loc[df.index == i, 'timestamp'] = ts.values[0]
    if len(df) > 5:
        add_time = 2 * (len(df) - 5)
    else:
        add_time = 0
    df.loc[:, 'ts_end'] = start + timedelta(minutes=15+add_time)
    df.loc[df.raising == 1, 'ts_end'] = start+timedelta(hours=1)
    df.loc[df.timestamp.isnull(), 'deduction'] = 1
    df.loc[df.timestamp <= df.ts_end, 'deduction'] = 0
    if len(df.loc[df.timestamp > df.ts_end, :]) != 0:
        df.loc[df.timestamp > df.ts_end, 'deduction'] = 0.1 * np.ceil(df.loc[df.timestamp > df.ts_end, ['ts_end', 'timestamp']].diff(axis=1) / timedelta(minutes=10)).timestamp.values[0]
    return df


def system_downtime(mysql=False):
    if mysql:
        query = 'SELECT * FROM system_down'
        df = db.df_read(query=query, resource="sensor_data")
    else:
        df = pd.read_csv('input/downtime.csv')
    df.loc[:, ['start_ts', 'end_ts']] = df.loc[:, ['start_ts', 'end_ts']].apply(pd.to_datetime)
    return df


def remove_downtime(df, downtime):
    for start, end in downtime[['start_ts', 'end_ts']].values:
        df = df.loc[(df.ts_start < start+np.timedelta64(150, 'm')) | (df.ts_start > end+np.timedelta64(150, 'm')), :]
    return df


def release_start(releases):
    ts_date = pd.to_datetime(pd.to_datetime(releases.timestamp.values[0]).date())
    text = releases.narrative.values[0]
    text = text.replace('NN', 'PM')
    match = re.search("\d{0,2}[:]{0,1}\d{1,2}[ ]{0,1}[AP]M", text)[0]
    hour = int(match[0:2])
    ts_start = ts_date 
    if hour != 12:
        ts_start += timedelta(hours=int(match[0:2]))
    if match[-2] == "P":
        ts_start += timedelta(0.5)
    if ':' in match:
        ts_start += timedelta(minutes=int(match[3:5]))
    releases.loc[:, 'ts_start'] = ts_start
    return releases


def get_releases(start, end, mysql=False):
    if mysql:
        query  = "SELECT site_code, timestamp, narrative FROM "
        query += "  (SELECT * FROM commons_db.narratives "
        query += "  WHERE narrative REGEXP 'EWI BULLETIN' "
        query += "  AND timestamp > '{start}' "
        query += "  AND timestamp <= '{end}' "
        query += "  ) bulletin "
        query += "INNER JOIN commons_db.sites USING (site_id) "
        query += "ORDER BY timestamp"
        query = query.format(start=start+timedelta(hours=8), end=end+timedelta(hours=8))
        df = db.df_read(query=query, connection="common")
    else:
        df = pd.read_csv('input/webbulletin.csv')
    df.loc[:, 'timestamp'] = pd.to_datetime(df.timestamp)
    df_grp = df.reset_index().groupby('index', as_index=False)
    df = df_grp.apply(release_start).reset_index(drop=True)
    return df

def main(start, end, mysql=False):
    downtime = system_downtime(mysql=mysql)
    ewi_sched = pd.read_csv('output/sending_status.csv', parse_dates=['ts_start', 'ts_end'])
    ewi_sched = remove_downtime(ewi_sched, downtime)
    ewi_sched = ewi_sched.loc[ewi_sched.event == 1, :]
    
    releases = get_releases(start, end, mysql=mysql)
    
    monitoring_ipr = pd.read_excel('output/monitoring_ipr.xlsx', sheet_name=None)
    
    for name in monitoring_ipr.keys():
        indiv_ipr = monitoring_ipr[name]
    
        for ts in indiv_ipr.columns[5:]:
            ts = pd.to_datetime(ts)
            ts_end = ts + timedelta(0.5)
            shift_release = ewi_sched.loc[(ewi_sched.ts_start > ts) & (ewi_sched.ts_start <= ts_end), :]
            if len(shift_release) != 0:
                indiv_release = shift_release.groupby('ts_start', as_index=False)
                shift_release = indiv_release.apply(check_sending, releases=releases).reset_index(drop=True)
                grade = np.round((len(shift_release) - sum(shift_release.deduction)) / len(shift_release), 2)
                indiv_ipr.loc[indiv_ipr.Output2 == 'EWI bulletin', str(ts)] = grade
        monitoring_ipr[name] = indiv_ipr
        
    writer = pd.ExcelWriter('output/monitoring_ipr.xlsx')
    for sheet_name, xlsxdf in monitoring_ipr.items():
        xlsxdf.to_excel(writer, sheet_name, index=False)
    writer.save()
