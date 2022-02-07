# -*- coding: utf-8 -*-
"""
Created on Wed Jun 24 11:08:27 2020

@author: Meryll
"""

from datetime import timedelta
import numpy as np
import os
import pandas as pd

import lib as ipr_lib
import smstags

output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..//input_output//'))


def check_sending(shift_release, inbox_tag, outbox_tag):
    event = shift_release.event.values[0]
    start = shift_release.data_ts.values[0] - np.timedelta64(int(3.5*60), 'm')
    if event != 1:
        start -= np.timedelta64(4, 'h')
    end = shift_release.ts_reminder.values[0]
    site_code = shift_release.site_code.values[0]
    meas = inbox_tag.loc[(inbox_tag.ts_sms >= start) & (inbox_tag.ts_sms < end) & (inbox_tag.site_code == site_code), :]
    meas_reminder = outbox_tag.loc[(outbox_tag.ts_written >= end-np.timedelta64(5, 'm')) & (outbox_tag.ts_written < end+np.timedelta64(30, 'm')) & (outbox_tag.site_code == site_code), :]
    shift_release.loc[:, 'reminder'] = int((len(meas) + len(meas_reminder) != 0) & (len(meas) * len(meas_reminder) == 0))
    return shift_release


def main(start, end, sched, mysql=False):
    sched = sched.loc[(sched.gndmeas == 1) | (sched.moms == 1), :]

    inbox_tag = smstags.inbox_tag(start, end, mysql=mysql)
    inbox_tag = inbox_tag.loc[inbox_tag.tag.isin(['#GroundMeas', '#CantSendGroundMeas', '#GroundObs']), :]
    outbox_tag = smstags.outbox_tag(start, end, mysql=mysql)
    outbox_tag = outbox_tag.loc[outbox_tag.tag.isin(['#GroundObsReminder', '#GroundMeasReminder']), :]
    
    monitoring_ipr = pd.read_excel(output_path + 'monitoring_ipr.xlsx', sheet_name=None)
    downtime = ipr_lib.system_downtime(mysql=mysql)
    sched = ipr_lib.remove_downtime(sched, downtime, meas_reminder=True)
    
    for name in monitoring_ipr.keys():
        indiv_ipr = monitoring_ipr[name]
        indiv_ipr.columns = indiv_ipr.columns.astype(str)
        for ts in indiv_ipr.columns[5:]:
            ts = pd.to_datetime(ts)
            ts_end = ts + timedelta(0.5)
            shift_release = sched.loc[(sched.data_ts >= ts) & (sched.data_ts < ts_end), :]
            if len(shift_release) != 0:
                shift_release.loc[:, 'ts_reminder'] = shift_release.data_ts-timedelta(hours=2)
                indiv_release = shift_release.reset_index().groupby('index', as_index=False)
                shift_release = indiv_release.apply(check_sending, inbox_tag=inbox_tag, outbox_tag=outbox_tag).reset_index(drop=True)
                indiv_ipr.loc[indiv_ipr.Output2 == 'Ground meas reminder', str(ts)] = np.mean(shift_release.reminder)
            else:
                indiv_ipr.loc[indiv_ipr.Output2 == 'Ground meas reminder', str(ts)] = np.nan
        monitoring_ipr[name] = indiv_ipr
    
    writer = pd.ExcelWriter(output_path + 'monitoring_ipr.xlsx')
    for sheet_name, xlsxdf in monitoring_ipr.items():
        xlsxdf.to_excel(writer, sheet_name, index=False)
    writer.save()
