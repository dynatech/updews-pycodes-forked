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
import ops.lib.querydb as qdb

add_start = 20 #number of minutes from data ts to start web alert releases
due = 10 #target number of minutes from start to release all web alert
due_r = 50 #target number of minutes from data ts to release raising web alert
delay_deduction = 0.1 #deduction per delay_min in minutes
delay_min = 3

output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..//input_output//'))


def main(start, end, sched, eval_df, mysql=True):
    downtime = ipr_lib.system_downtime(mysql=mysql)
    sched = ipr_lib.remove_downtime(sched, downtime)
    sched.loc[sched.raising != 1, 'ts_start'] = sched.loc[sched.raising != 1, 'data_ts'] + timedelta(minutes=add_start)
    sched.loc[sched.raising != 1, 'ts_due'] = sched.loc[sched.raising != 1, 'ts_start'] + timedelta(minutes=due)
    sched.loc[sched.raising == 1, 'ts_start'] = sched.loc[sched.raising == 1, 'data_ts']
    sched.loc[sched.raising == 1, 'ts_due'] = sched.loc[sched.raising == 1, 'data_ts'] + timedelta(minutes=due_r)
    
    releases = qdb.get_web_releases(start, end, mysql=mysql)
    sched = pd.merge(sched, releases, how='left', on=['site_code', 'data_ts'])
    sched.loc[:, 'grade_t'] = sched.apply(lambda row: 1 - min(1, delay_deduction * np.ceil(max(max(0, (row.ts_release-row.ts_due).total_seconds()), max(0, (row.ts_start-row.ts_release).total_seconds()))/(60*delay_min))), axis=1)
    sched.loc[sched.ts_release.isnull(), 'grade_t'] = 0

    monitoring_ipr = pd.read_excel(output_path + 'monitoring_ipr.xlsx', sheet_name=None)
    
    for name in monitoring_ipr.keys():
        indiv_ipr = monitoring_ipr[name]
        indiv_ipr.columns = indiv_ipr.columns.astype(str)
        for ts in indiv_ipr.columns[5:]:
            ts = pd.to_datetime(ts)
            ts_end = ts + timedelta(0.5)
            shift_release = sched.loc[(sched.data_ts >= ts) & (sched.data_ts < ts_end), :]
            if len(shift_release) != 0:
                grade = np.round(np.average(shift_release.grade_t), 2)
                indiv_ipr.loc[indiv_ipr.Output2 == 'EWI web release', str(ts)] = grade
            else:
                indiv_ipr.loc[indiv_ipr.Output2 == 'EWI web release', str(ts)] = np.nan
            if ts >= pd.to_datetime('2021-04-01') and len(shift_release) != 0:
                shift_eval = eval_df.loc[(eval_df.shift_ts >= ts) & (eval_df.shift_ts <= ts+timedelta(1)) & ((eval_df['evaluated_MT'] == name) | (eval_df['evaluated_CT'] == name) | (eval_df['evaluated_backup'] == name)), :].drop_duplicates('shift_ts', keep='last')[0:1]
                shift_eval = shift_eval.drop_duplicates('shift_ts', keep='last')[0:1]
                deduction = np.nansum(shift_eval[['routine_web_alert_ts', 'web_alert_ts']].values)/3 + np.nansum(shift_eval[['routine_web_alert_level', 'web_alert_level']].values)
                indiv_ipr.loc[indiv_ipr.Output1 == 'web release', str(ts)] = np.round((len(shift_release) - deduction)/len(shift_release), 2)
            else:
                indiv_ipr.loc[indiv_ipr.Output1 == 'web release', str(ts)] = np.nan
        monitoring_ipr[name] = indiv_ipr
        
    writer = pd.ExcelWriter(output_path + 'monitoring_ipr.xlsx')
    for sheet_name, xlsxdf in monitoring_ipr.items():
        xlsxdf.to_excel(writer, sheet_name, index=False)
    writer.save()
