# -*- coding: utf-8 -*-
"""
Created on Wed Jun 24 11:08:27 2020

@author: Meryll
"""

from datetime import timedelta
import numpy as np
import pandas as pd

import lib
import smstags


def fyi_sending(shift_release, FYI_tag, inbox_tag, site_names, start_tag):

    site_code = shift_release.site_code.values[0]
    site_name = site_names.loc[site_names.site_code == site_code, 'name'].values[0]
    start = pd.to_datetime(shift_release.ts_start.values[0])
    end = lib.release_time(start+timedelta(hours=1))
    sent = FYI_tag.loc[(FYI_tag.site_code == site_code) & (FYI_tag.timestamp >= start) & (FYI_tag.timestamp <= end), :]
    tag = inbox_tag.loc[(inbox_tag.ts_sms >= start) & (inbox_tag.ts_sms <= end) & (inbox_tag.sms_msg.str.contains(site_name)) & (inbox_tag.tag == '#AlertFYI'), :]
    
    # no FYI sent
    if len(sent) == 0:
        shift_release.loc[:, 'deduction_t'] = 1
        shift_release.loc[:, 'deduction_q'] = 1
    else:
        # timeliness
        if pd.to_datetime(max(sent.timestamp)) <= end:
            shift_release.loc[:, 'deduction_t'] = 0
        else:
            shift_release.loc[:, 'deduction_t'] = 0.1 * np.ceil((max(sent.timestamp) - end).dt.total_seconds()/600)
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


def permission_sending(shift_release, permission_tag, inbox_tag, site_names, start_tag):

    site_code = shift_release.site_code.values[0]
    site_name = site_names.loc[site_names.site_code == site_code, 'name'].values[0]
    start = pd.to_datetime(shift_release.ts_start.values[0])
    end = lib.release_time(start+timedelta(hours=1))
    sent = permission_tag.loc[(permission_tag.site_code == site_code) & (permission_tag.timestamp >= start) & (permission_tag.timestamp <= end), :]
    tag = inbox_tag.loc[(inbox_tag.ts_sms >= start) & (inbox_tag.ts_sms <= end) & (inbox_tag.sms_msg.str.contains(site_name)) & (inbox_tag.tag == '#Permission'), :]
    
    # no permission sent
    if len(sent) == 0:
        shift_release.loc[:, 'deduction_t'] = 1
        shift_release.loc[:, 'deduction_q'] = 1
    else:
        # timeliness
        if pd.to_datetime(max(sent.timestamp)) <= end:
            shift_release.loc[:, 'deduction_t'] = 0
        else:
            shift_release.loc[:, 'deduction_t'] = 0.1 * np.ceil((max(sent.timestamp) - end).dt.total_seconds()/600)
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


def main(start, end, mysql=False):
    ewi_sched = pd.read_csv('output/sending_status.csv', parse_dates=['ts_start'])
    ewi_sched = ewi_sched.loc[(ewi_sched.raising == 1) | (ewi_sched.lowering == 1), :]
    ewi_sched.loc[:, 'ts_end'] = ewi_sched.ts_start + timedelta(hours=4)
    ewi_sched.loc[:, ['ts_start', 'ts_end']] = ewi_sched.loc[:, ['ts_start', 'ts_end']].apply(pd.to_datetime)
    
    start_tag = pd.to_datetime('2021-04-01')
    inbox_tag = smstags.inbox_tag(start, end, mysql=mysql)
    inbox_tag = inbox_tag.loc[inbox_tag.tag.isin(['#AlertFYI', '#Permission']), :]
    inbox_tag.loc[:, 'sms_msg'] = inbox_tag.loc[:, 'sms_msg'].apply(lambda x: x.lower().replace('city', '').replace('.', ''))
    FYI_tag = lib.get_narratives(start, end, mysql=mysql, category='FYI')
    permission_tag = lib.get_narratives(start, end, mysql=mysql, category='permission')
    site_names = lib.get_site_names()
    
    monitoring_ipr = pd.read_excel('output/monitoring_ipr.xlsx', sheet_name=None)
    
    for name in monitoring_ipr.keys():
        indiv_ipr = monitoring_ipr[name]
        indiv_ipr.columns = indiv_ipr.columns.astype(str)
        for ts in indiv_ipr.columns[5:]:
            ts = pd.to_datetime(ts)
            ts_end = ts + timedelta(0.5)
            shift_FYI = ewi_sched.loc[(ewi_sched.ts_start > ts) & (ewi_sched.ts_start <= ts_end), :]
            shift_permission = ewi_sched.loc[(ewi_sched.ts_start > ts) & (ewi_sched.ts_start <= ts_end) & (ewi_sched.permission == 1), :]
            if len(shift_FYI) != 0:
                indiv_FYI = shift_FYI.groupby('index', as_index=False)
                shift_FYI = indiv_FYI.apply(fyi_sending, FYI_tag=FYI_tag, inbox_tag=inbox_tag, site_names=site_names, start_tag=start_tag).reset_index(drop=True)
                grade_t = np.round((len(shift_FYI) - sum(shift_FYI.deduction_t)) / len(shift_FYI), 2)
                indiv_ipr.loc[indiv_ipr.Output2 == 'FYI', str(ts)] = grade_t
            if len(shift_permission) != 0:
                indiv_permission = shift_permission.groupby('index', as_index=False)
                shift_permission = indiv_permission.apply(permission_sending, permission_tag=permission_tag, inbox_tag=inbox_tag, site_names=site_names, start_tag=start_tag).reset_index(drop=True)
                grade_t = np.round((len(shift_permission) - sum(shift_permission.deduction_t)) / len(shift_permission), 2)
                indiv_ipr.loc[indiv_ipr.Output2 == 'Permission', str(ts)] = grade_t
            shift_release = shift_FYI.append(shift_permission, ignore_index=True)
            shift_release = shift_release.loc[shift_release.ts_start >= start_tag, :]
            if len(shift_release) != 0:
                grade_q = np.round((len(shift_release) - sum(shift_release.deduction_q)) / len(shift_release), 2)
                indiv_ipr.loc[indiv_ipr.Output1 == 'FYI/Permission', str(ts)] = grade_q
        monitoring_ipr[name] = indiv_ipr
        
    writer = pd.ExcelWriter('output/monitoring_ipr.xlsx')
    for sheet_name, xlsxdf in monitoring_ipr.items():
        xlsxdf.to_excel(writer, sheet_name, index=False)
    writer.save()
