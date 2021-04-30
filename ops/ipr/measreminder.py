# -*- coding: utf-8 -*-
"""
Created on Wed Jun 24 11:08:27 2020

@author: Meryll
"""

from datetime import timedelta, time
import numpy as np
import pandas as pd

import dynadb.db as db
import smstags


def check_sending(shift_release, inbox_tag, outbox_tag):
    event = shift_release.event.values[0]
    start = shift_release.ts_start.values[0] - np.timedelta64(4, 'h')
    if event != 1:
        start -= np.timedelta64(4, 'h')
    end = shift_release.ts_reminder.values[0]
    site_code = shift_release.site_code.values[0]
    meas = inbox_tag.loc[(inbox_tag.ts_sms >= start) & (inbox_tag.ts_sms < end) & (inbox_tag.site_code == site_code), :]
    meas_reminder = outbox_tag.loc[(outbox_tag.ts_written >= end-np.timedelta64(5, 'm')) & (outbox_tag.ts_written < end+np.timedelta64(30, 'm')) & (outbox_tag.site_code == site_code), :]
    shift_release.loc[:, 'reminder'] = int((len(meas) + len(meas_reminder) != 0) & (len(meas) * len(meas_reminder) == 0))
    return shift_release


def system_downtime(mysql=False):
    if mysql:
        query = 'SELECT * FROM system_down WHERE reported = 1'
        df = db.df_read(query=query, resource="sensor_data")
    else:
        df = pd.read_csv('input/downtime.csv')
    df.loc[:, ['start_ts', 'end_ts']] = df.loc[:, ['start_ts', 'end_ts']].apply(pd.to_datetime)
    return df


def remove_downtime(shift_release, downtime):
    for start, end in downtime[['start_ts', 'end_ts']].values:
        shift_release = shift_release.loc[(shift_release.ts_start < start+np.timedelta64(150, 'm')) | (shift_release.ts_start > end+np.timedelta64(150, 'm')), :]
    return shift_release


def main(start, end, mysql=False):
    ewi_sched = pd.read_csv('output/sending_status.csv', parse_dates=['ts_start'])
    ewi_sched = ewi_sched.loc[(ewi_sched.ts_start.dt.time >= time(8,0)) & (ewi_sched.ts_start.dt.time <= time(16,0)) & (ewi_sched.raising != 1), :]
    ewi_sched.loc[:, ['ts_start', 'ts_end']] = ewi_sched.loc[:, ['ts_start', 'ts_end']].apply(pd.to_datetime)

    inbox_tag = smstags.inbox_tag(start, end, mysql=mysql)
    inbox_tag = inbox_tag.loc[inbox_tag.tag.isin(['#GroundMeas', '#CantSendGroundMeas']), :]
    outbox_tag = smstags.outbox_tag(start, end, mysql=mysql)
    outbox_tag = outbox_tag.loc[outbox_tag.tag == '#GroundMeasReminder', :]
    
    monitoring_ipr = pd.read_excel('output/monitoring_ipr.xlsx', sheet_name=None)
    downtime = system_downtime(mysql=mysql)
    ewi_sched = remove_downtime(ewi_sched, downtime)
    
    for name in monitoring_ipr.keys():
        indiv_ipr = monitoring_ipr[name]
    
        for ts in indiv_ipr.columns[5:]:
            ts = pd.to_datetime(ts)
            ts_end = ts + timedelta(0.5)
            shift_release = ewi_sched.loc[(ewi_sched.ts_start > ts) & (ewi_sched.ts_start <= ts_end), :]
            if len(shift_release) != 0:
                shift_release.loc[:, 'ts_reminder'] = shift_release.ts_start-timedelta(hours=2.5)
                indiv_release = shift_release.groupby('index', as_index=False)
                shift_release = indiv_release.apply(check_sending, inbox_tag=inbox_tag, outbox_tag=outbox_tag).reset_index(drop=True)
                indiv_ipr.loc[indiv_ipr.Output2 == 'Ground meas reminder', str(ts)] = np.mean(shift_release.reminder)
        monitoring_ipr[name] = indiv_ipr
    
    writer = pd.ExcelWriter('output/monitoring_ipr.xlsx')
    for sheet_name, xlsxdf in monitoring_ipr.items():
        xlsxdf.to_excel(writer, sheet_name, index=False)
    writer.save()
