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

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import ops.lib.lib as lib
import ops.lib.querydb as qdb
import ops.lib.bulletin as bulletin


add_start = 30 #number of minutes from data ts to start sending of ewi bulletin
due = 10 #target number of minutes from start to send all ewi bulletin
add_5 = 2 #additional minute(s) [per site] from target for releases above 5 sites
add_l = 15 #additional minute(s) [per site] from target for lowering releases
due_r = 60 #target number of minutes from data ts to send raising ewi bulletin
delay_deduction = 0.1 #deduction per delay_min in minutes
delay_min = 10

output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..//input_output//'))


def sending_sched(curr_release):
    num_routine = int(len(curr_release.loc[(curr_release.event != 1) & (curr_release.extended != 1), :]) > 0)
    num_sites = num_routine + len(curr_release.loc[(curr_release.event == 1) | (curr_release.extended == 1), :])
    curr_release.loc[:, 'ts_due'] = curr_release.data_ts + timedelta(minutes=add_start+due+add_5*max(0, num_sites - 5))
    curr_release.loc[curr_release.lowering == 1, 'ts_due'] = curr_release.ts_due + timedelta(minutes=add_l)
    curr_release.loc[curr_release.raising == 1, 'ts_due'] = curr_release.data_ts + timedelta(minutes=due_r)
    return curr_release


def main(start, end, sched, site_names, eval_df, mysql=True, to_csv=False):

    sent_start = start - timedelta(hours=0.25)
    sent_end = end + timedelta(hours=4)
    per_release = sched.groupby('data_ts', as_index=False)
    sched = per_release.apply(sending_sched)
    
    recipients = qdb.get_bulletin_recipients(mysql=mysql, to_csv=to_csv)
    sent = qdb.get_bulletin_sent(sent_start, sent_end, mysql=mysql, to_csv=to_csv)
    sched = bulletin.ewi_sched(sched, recipients, sent)
    
    
    monitoring_ipr = pd.read_excel(output_path + 'monitoring_ipr.xlsx', sheet_name=None)
    
    downtime = ipr_lib.system_downtime(mysql=mysql)
    sched = ipr_lib.remove_downtime(sched, downtime)
    
    
    for name in monitoring_ipr.keys():
        indiv_ipr = monitoring_ipr[name]
        indiv_ipr.columns = indiv_ipr.columns.astype(str)
        for ts in indiv_ipr.columns[5:]:
            ts = pd.to_datetime(ts)
            sending_status = sched.loc[(sched.data_ts >= ts) & (sched.data_ts < ts+timedelta(0.5)), :]
            # ewi bulletin timeliness
            if len(sending_status) == 0:
                # no scheduled
                grade_t = np.nan
            elif len(sending_status.loc[sending_status.ts_sent.isnull(), :]) == 0 and all(sending_status.ts_sent <= sending_status.ts_due):
                # all sent on time
                grade_t = 1
            else:
                grade_t = np.round(np.average(1 - sending_status.apply(lambda row: np.where(row.ts_sent is pd.NaT, 1, delay_deduction * max(0, (row.ts_sent - row.ts_due).total_seconds())/(60*delay_min)), axis=1)), 2)
            indiv_ipr.loc[indiv_ipr.Output2 == 'EWI bulletin', str(ts)] = grade_t
            if ts >= pd.to_datetime('2021-04-01') and len(sending_status) != 0:
                shift_eval = eval_df.loc[(eval_df.shift_ts >= ts) & (eval_df.shift_ts <= ts+timedelta(1)) & ((eval_df['evaluated_MT'] == name) | (eval_df['evaluated_CT'] == name) | (eval_df['evaluated_backup'] == name)), :].drop_duplicates('shift_ts', keep='last')[0:1]
                shift_eval = shift_eval.drop_duplicates('shift_ts', keep='last')[0:1]
                deduction = np.nansum((4/15)*shift_eval['bul_ts'] + shift_eval['bul_alert'] + (1/15)*shift_eval['bul_typo'])
                indiv_ipr.loc[indiv_ipr.Output1 == 'EWI Bulletin', str(ts)] = np.round((len(sending_status) - deduction)/len(sending_status), 2)
        monitoring_ipr[name] = indiv_ipr
        
    writer = pd.ExcelWriter(output_path + 'monitoring_ipr.xlsx')
    for sheet_name, xlsxdf in monitoring_ipr.items():
        xlsxdf.to_excel(writer, sheet_name, index=False)
    writer.save()