# -*- coding: utf-8 -*-
"""
Created on Wed Jun 24 11:08:27 2020

@author: Meryll
"""

from datetime import timedelta
import numpy as np
import pandas as pd
import re

import lib

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


def release_start(releases):
    ts_date = pd.to_datetime(pd.to_datetime(releases.timestamp.values[0]).date())
    text = releases.narrative.values[0]
    text = text.replace('NN', 'PM')
    match = re.search("\d{0,2}[:]{0,1}\d{1,2}[ ]{0,1}[AP]M", text)[0]
    hour = int(match[:-2].split(':')[0])
    ts_start = ts_date 
    if hour != 12:
        ts_start += timedelta(hours=int(hour))
    if match[-2] == "P":
        ts_start += timedelta(0.5)
    if ':' in match:
        ts_start += timedelta(minutes=int(match[:-2].split(':')[1]))
    releases.loc[:, 'ts_start'] = ts_start
    return releases


def main(start, end, eval_df, mysql=False):
    downtime = lib.system_downtime(mysql=mysql)
    ewi_sched = pd.read_csv('output/sending_status.csv', parse_dates=['ts_start', 'ts_end'])
    ewi_sched = lib.remove_downtime(ewi_sched, downtime)
    ewi_sched = ewi_sched.loc[ewi_sched.event == 1, :]
    
    releases = lib.get_narratives(start, end, mysql=mysql)
    releases_grp = releases.reset_index().groupby('index', as_index=False)
    releases = releases_grp.apply(release_start).reset_index(drop=True)

    monitoring_ipr = pd.read_excel('output/monitoring_ipr.xlsx', sheet_name=None)
    
    for name in monitoring_ipr.keys():
        indiv_ipr = monitoring_ipr[name]
        indiv_ipr.columns = indiv_ipr.columns.astype(str)
        for ts in indiv_ipr.columns[5:]:
            ts = pd.to_datetime(ts)
            ts_end = ts + timedelta(0.5)
            shift_release = ewi_sched.loc[(ewi_sched.ts_start > ts) & (ewi_sched.ts_start <= ts_end), :]
            if len(shift_release) != 0:
                indiv_release = shift_release.groupby('ts_start', as_index=False)
                shift_release = indiv_release.apply(check_sending, releases=releases).reset_index(drop=True)
                grade = np.round((len(shift_release) - sum(shift_release.deduction)) / len(shift_release), 2)
                indiv_ipr.loc[indiv_ipr.Output2 == 'EWI bulletin', str(ts)] = grade
            if ts >= pd.to_datetime('2021-04-01') and len(shift_release) != 0:
                shift_eval = eval_df.loc[(eval_df.shift_ts >= ts) & (eval_df.shift_ts <= ts+timedelta(1)) & ((eval_df['evaluated_MT'] == name) | (eval_df['evaluated_CT'] == name) | (eval_df['evaluated_backup'] == name)), :].drop_duplicates('shift_ts', keep='last')
                deduction = np.nansum((4/15)*shift_eval['bul_ts'] + shift_eval['bul_alert'] + (1/15)*shift_eval['bul_typo'])
                indiv_ipr.loc[indiv_ipr.Output1 == 'EWI bulletin', str(ts)] = np.round((len(shift_release) - deduction)/len(shift_release), 2)
        monitoring_ipr[name] = indiv_ipr
        
    writer = pd.ExcelWriter('output/monitoring_ipr.xlsx')
    for sheet_name, xlsxdf in monitoring_ipr.items():
        xlsxdf.to_excel(writer, sheet_name, index=False)
    writer.save()
