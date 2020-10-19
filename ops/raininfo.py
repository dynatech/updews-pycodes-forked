# -*- coding: utf-8 -*-
"""
Created on Wed Jun 24 11:08:27 2020

@author: Meryll
"""

from datetime import timedelta
import numpy as np
import pandas as pd

import dynadb.db as db
import smstags


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


def site_recipient():
    try:
        query = "SELECT site_code FROM sites "
        query += "WHERE province  IN ('Benguet', 'Samar') "
        query += "OR site_code = 'NAG'"
        df = db.df_read(query=query, connection="common")
        site_list = df.site_code.values
    except:
        site_list = ['bak', 'bar', 'hin', 'ime', 'jor', 'lab', 'lay', 'lpa', 'lte',
       'mam', 'nag', 'par', 'pug', 'sin']
    return site_list


def check_sending(shift_release, outbox_tag):
    site_code = shift_release.site_code.values[0]
    start = shift_release.ts_start.values[0]
    end = shift_release.ts_end.values[0]
    sent = outbox_tag.loc[(outbox_tag.ts_written >= start) & (outbox_tag.ts_written <= end + np.timedelta64(100, 'm')) & (outbox_tag.site_code == site_code), :]
    if len(sent) == 0:
        shift_release.loc[:, 'deduction'] = 1
    else:
        shift_release.loc[:, 'deduction'] = 0.1 * np.ceil((min(sent.ts_written) - shift_release.ts_end).dt.total_seconds()/600)
    return shift_release


def main(start, end):
    site_list = site_recipient()
    downtime = system_downtime()
    ewi_sched = pd.read_csv('output/sending_status.csv', parse_dates=['ts_start'])
    ewi_sched = ewi_sched.loc[(ewi_sched.raising != 1) & (ewi_sched.event == 1) & (ewi_sched.site_code.isin(site_list)), :]
    ewi_sched.loc[:, 'ts_end'] = ewi_sched.ts_start + timedelta(minutes=15)
    ewi_sched.loc[:, ['ts_start', 'ts_end']] = ewi_sched.loc[:, ['ts_start', 'ts_end']].apply(pd.to_datetime)
    ewi_sched = remove_downtime(ewi_sched, downtime)
    
    outbox_tag = smstags.outbox_tag(start, end)
    outbox_tag = outbox_tag.loc[outbox_tag.tag.isin(['#RainInfo']), :]
    
    monitoring_ipr = pd.read_excel('output/monitoring_ipr.xlsx', sheet_name=None)
    
    
    for name in monitoring_ipr.keys():
        indiv_ipr = monitoring_ipr[name]
    
        for ts in indiv_ipr.columns[5:]:
            ts = pd.to_datetime(ts)
            ts_end = ts + timedelta(0.5)
            shift_release = ewi_sched.loc[(ewi_sched.ts_start > ts) & (ewi_sched.ts_start <= ts_end), :]
            if len(shift_release) != 0:
                indiv_release = shift_release.groupby('index', as_index=False)
                shift_release = indiv_release.apply(check_sending, outbox_tag=outbox_tag).reset_index(drop=True)
                grade = np.round((len(shift_release) - sum(shift_release.deduction)) / len(shift_release), 2)
                indiv_ipr.loc[indiv_ipr.Output2 == 'Rainfall info', str(ts)] = grade
        monitoring_ipr[name] = indiv_ipr
        
    writer = pd.ExcelWriter('output/monitoring_ipr.xlsx')
    for sheet_name, xlsxdf in monitoring_ipr.items():
        xlsxdf.to_excel(writer, sheet_name, index=False)
    writer.save()
