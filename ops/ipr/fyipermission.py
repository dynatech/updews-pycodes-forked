# -*- coding: utf-8 -*-
"""
Created on Wed Jun 24 11:08:27 2020

@author: Meryll
"""

from datetime import timedelta
import numpy as np
import os
import pandas as pd
import sys

import lib as ipr_lib
import smstags

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import ops.lib.lib as lib


output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..//input_output//'))


def fyi_sending(shift_release, FYI_tag, inbox_tag, outbox_tag, site_names, start_tag, shift_end):

    site_code = shift_release.site_code.values[0]
    site_name = site_names.loc[site_names.site_code == site_code, 'name'].values[0]
    start = pd.to_datetime(shift_release.data_ts.values[0]) - timedelta(hours=1)
    end = lib.release_time(start+timedelta(hours=1))
    sent = FYI_tag.loc[(FYI_tag.site_code == site_code) & (FYI_tag.timestamp >= start) & (FYI_tag.timestamp <= shift_end), :]
    tag = outbox_tag.loc[(outbox_tag.ts_sms >= start) & (outbox_tag.ts_sms <= shift_end) & (outbox_tag.sms_msg.str.contains(site_name)) & (outbox_tag.tag == '#AlertFYI'), :]
    if len(tag) == 0:
        tag = inbox_tag.loc[(inbox_tag.ts_sms >= start) & (inbox_tag.ts_sms <= shift_end) & (inbox_tag.sms_msg.str.contains(site_name)) & (inbox_tag.tag == '#AlertFYI'), :]
    
    # no FYI sent
    if len(sent) == 0:
        shift_release.loc[:, 'deduction_t'] = 1
        shift_release.loc[:, 'deduction_q'] = 1
    else:
        # timeliness
        if pd.to_datetime(max(sent.timestamp)) <= end:
            shift_release.loc[:, 'deduction_t'] = 0
        else:
            shift_release.loc[:, 'deduction_t'] = 0.1 * np.ceil((max(sent.timestamp) - end).total_seconds()/600)
        # quality erratum
        if start >= start_tag:
            if len(tag) == 1:
                shift_release.loc[:, 'deduction_q'] = 0
            elif len(tag) == 0:
                shift_release.loc[:, 'deduction_q'] = 1
            elif pd.to_datetime(tag.ts_sms.values[0]) <= start+timedelta(hours=1):
                shift_release.loc[:, 'deduction_q'] = 0.2
            elif pd.to_datetime(tag.ts_sms.values[0]) <= end:
                shift_release.loc[:, 'deduction_q'] = 0.6
            else:
                shift_release.loc[:, 'deduction_q'] = 1        

    return shift_release


def permission_sending(shift_release, permission_tag, inbox_tag, outbox_tag, site_names, start_tag, shift_end):

    site_code = shift_release.site_code.values[0]
    site_name = site_names.loc[site_names.site_code == site_code, 'name'].values[0]
    start = pd.to_datetime(shift_release.data_ts.values[0])
    end = lib.release_time(start+timedelta(hours=1))
    sent = permission_tag.loc[(permission_tag.site_code == site_code) & (permission_tag.timestamp >= start) & (permission_tag.timestamp <= shift_end), :]
    tag = outbox_tag.loc[(outbox_tag.ts_sms >= start) & (outbox_tag.ts_sms <= shift_end) & (outbox_tag.sms_msg.str.contains(site_name)) & (outbox_tag.tag == '#AlertFYI'), :]
    if len(tag) == 0:
        tag = inbox_tag.loc[(inbox_tag.ts_sms >= start) & (inbox_tag.ts_sms <= shift_end) & (inbox_tag.sms_msg.str.contains(site_name)) & (inbox_tag.tag == '#AlertFYI'), :]
    
    # no permission sent
    if len(sent) == 0:
        shift_release.loc[:, 'deduction_t'] = 1
        shift_release.loc[:, 'deduction_q'] = 1
    else:
        # timeliness
        if pd.to_datetime(max(sent.timestamp)) <= end:
            shift_release.loc[:, 'deduction_t'] = 0
        else:
            shift_release.loc[:, 'deduction_t'] = 0.1 * np.ceil((max(sent.timestamp) - end).total_seconds()/600)
        # quality erratum
        if start >= start_tag:
            if len(tag) == 1:
                shift_release.loc[:, 'deduction_q'] = 0
            elif len(tag) == 0:
                shift_release.loc[:, 'deduction_q'] = 1
            elif pd.to_datetime(tag.ts_sms.values[0]) <= start+timedelta(hours=1):
                shift_release.loc[:, 'deduction_q'] = 0.2
            elif pd.to_datetime(tag.ts_sms.values[0]) <= end:
                shift_release.loc[:, 'deduction_q'] = 0.6
            else:
                shift_release.loc[:, 'deduction_q'] = 1
                
    return shift_release


def main(start, end, sched, mysql=False):
    sched = sched.loc[(sched.raising == 1) | (sched.lowering == 1), :]
    sched.loc[:, 'ts_end'] = sched.data_ts + timedelta(hours=4)
    sched.loc[:, ['data_ts', 'ts_end']] = sched.loc[:, ['data_ts', 'ts_end']].apply(pd.to_datetime)
    
    start_tag = pd.to_datetime('2021-04-01')
    inbox_tag = smstags.inbox_tag(start, end, mysql=mysql)
    inbox_tag = inbox_tag.loc[inbox_tag.tag.isin(['#AlertFYI', '#Permission']), :]
    inbox_tag.loc[:, 'sms_msg'] = inbox_tag.loc[:, 'sms_msg'].apply(lambda x: x.lower().replace('city', '').replace('.', ''))
    outbox_tag = smstags.outbox_tag(start, end, mysql=mysql)
    outbox_tag = outbox_tag.loc[outbox_tag.tag.isin(['#AlertFYI', '#Permission']), :]
    outbox_tag.loc[:, 'sms_msg'] = outbox_tag.loc[:, 'sms_msg'].apply(lambda x: x.lower().replace('city', '').replace('.', ''))
    FYI_tag = ipr_lib.get_narratives(start, end, mysql=mysql, category='FYI')
    permission_tag = ipr_lib.get_narratives(start, end, mysql=mysql, category='permission')
    site_names = lib.get_site_names()
    
    monitoring_ipr = pd.read_excel(output_path + 'monitoring_ipr.xlsx', sheet_name=None)
    
    for name in monitoring_ipr.keys():
        indiv_ipr = monitoring_ipr[name]
        indiv_ipr.columns = indiv_ipr.columns.astype(str)
        for ts in indiv_ipr.columns[5:]:
            ts = pd.to_datetime(ts)
            ts_end = ts + timedelta(0.5)
            shift_FYI = sched.loc[(sched.data_ts > ts) & (sched.data_ts <= ts_end), :]
            shift_permission = sched.loc[(sched.data_ts > ts) & (sched.data_ts <= ts_end) & (sched.permission == 1), :]
            if len(shift_FYI) != 0:
                indiv_FYI = shift_FYI.reset_index().groupby('index', as_index=False)
                shift_FYI = indiv_FYI.apply(fyi_sending, FYI_tag=FYI_tag, inbox_tag=inbox_tag, outbox_tag=outbox_tag, site_names=site_names, start_tag=start_tag, shift_end=ts_end+timedelta(hours=1.5)).reset_index(drop=True)
                grade_t = np.round((len(shift_FYI) - sum(shift_FYI.deduction_t)) / len(shift_FYI), 2)
                indiv_ipr.loc[indiv_ipr.Output2 == 'FYI', str(ts)] = grade_t
            if len(shift_permission) != 0:
                indiv_permission = shift_permission.reset_index().groupby('index', as_index=False)
                shift_permission = indiv_permission.apply(permission_sending, permission_tag=permission_tag, inbox_tag=inbox_tag, outbox_tag=outbox_tag, site_names=site_names, start_tag=start_tag, shift_end=ts_end+timedelta(hours=1.5)).reset_index(drop=True)
                grade_t = np.round((len(shift_permission) - sum(shift_permission.deduction_t)) / len(shift_permission), 2)
                indiv_ipr.loc[indiv_ipr.Output2 == 'Permission', str(ts)] = grade_t
            shift_release = shift_FYI.append(shift_permission, ignore_index=True)
            shift_release = shift_release.loc[shift_release.data_ts >= start_tag, :]
            if len(shift_release) != 0:
                grade_q = np.round((len(shift_release) - sum(shift_release.deduction_q)) / len(shift_release), 2)
                indiv_ipr.loc[indiv_ipr.Output1 == 'FYI/Permission', str(ts)] = grade_q
        monitoring_ipr[name] = indiv_ipr
        
    writer = pd.ExcelWriter(output_path + 'monitoring_ipr.xlsx')
    for sheet_name, xlsxdf in monitoring_ipr.items():
        xlsxdf.to_excel(writer, sheet_name, index=False)
    writer.save()
