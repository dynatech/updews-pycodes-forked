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

output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..//input_output//'))


def main(start, end, sched, eval_df, mysql=False):
    monitoring_ipr = pd.read_excel(output_path + 'monitoring_ipr.xlsx', sheet_name=None)

    downtime = ipr_lib.system_downtime(mysql=mysql)
    
    for name in monitoring_ipr.keys():
        indiv_ipr = monitoring_ipr[name]
        indiv_ipr.columns = indiv_ipr.columns.astype(str)
        for ts in indiv_ipr.columns[5:]:
            ts = pd.to_datetime(ts)
            ts_end = ts + timedelta(0.5)
            shift_release = sched.loc[(sched.data_ts > ts) & (sched.data_ts <= ts_end), :]
            if ts >= pd.to_datetime('2021-04-01'):
                shift_eval = eval_df.loc[(eval_df.shift_ts >= ts) & (eval_df.shift_ts <= ts+timedelta(1)) & ((eval_df['evaluated_MT'] == name) | (eval_df['evaluated_CT'] == name) | (eval_df['evaluated_backup'] == name)), :].drop_duplicates('shift_ts', keep='last')
                shift_eval = shift_eval.drop_duplicates('shift_ts', keep='last')[0:1]
                # bug reports | ensure system is working
                shift_downtime = downtime.loc[(((downtime.start_ts<=ts_end)&(downtime.end_ts>=ts_end)) | ((downtime.start_ts<=ts)&(downtime.end_ts>=ts_end)) | ((downtime.start_ts<=ts)&(downtime.end_ts>=ts)) | ((downtime.start_ts>=ts)&(downtime.end_ts<=ts_end))) & (downtime.reported == 0), :]
                if len(shift_downtime) == 0:
                    grade_sys = 1
                else:
                    grade_sys = 1 - np.sum(shift_downtime.loc[:, ['start_ts', 'end_ts']].apply(lambda row: (min(ts_end, row.end_ts) - max(ts, row.start_ts)).total_seconds()/3600, axis=1))/12.5
                if all(shift_eval[['bug_log', 'relayed']].isnull().values.flatten()):
                    grade_bug = np.nan
                elif set(shift_eval[['bug_log', 'relayed']].values.flatten())-(set(['Yes', np.nan])) == set():
                    grade_bug = 1
                else:
                    grade_bug = 0
                if len(shift_release) != 0:
                    indiv_ipr.loc[indiv_ipr.Output1 == 'ensure system is working', str(ts)] = grade_sys
                    indiv_ipr.loc[indiv_ipr.Output1 == 'bug reports', str(ts)] = grade_bug
                else:
                    if not np.isnan(grade_bug):
                        grade = np.mean([grade_sys, grade_bug])
                    else:
                        grade = grade_sys
                    indiv_ipr.loc[indiv_ipr.Output2 == 'ensure system is working', str(ts)] = grade
                # contacts updating
                if len(shift_release) == 0 and len(shift_eval) != 0:
                    if shift_eval.contacts.values[0] == 'NAN EEEE':
                        grade_contacts = 0
                    else:
                        grade_contacts = min(1, len(shift_eval.contacts.values[0].split(', ')) / 5)
                    indiv_ipr.loc[indiv_ipr.Output2 == 'updating of contacts', str(ts)] = grade_contacts
                # sc concerns
                if all(shift_eval[['sc_log', 'responded']].isnull().values.flatten()):
                    grade_SC = np.nan
                elif set(shift_eval[['sc_log', 'responded']].values.flatten())-(set(['Yes', np.nan])) == set():
                    grade_SC = 1
                else:
                    grade_SC = 0
                if len(shift_release) != 0:
                    indiv_ipr.loc[indiv_ipr.Output1.str.contains('stakeholders concerns', na=False), str(ts)] = grade_SC
                else:
                    indiv_ipr.loc[indiv_ipr.Output2 == 'stakeholders concerns', str(ts)] = grade_SC
        monitoring_ipr[name] = indiv_ipr
        
    writer = pd.ExcelWriter(output_path + 'monitoring_ipr.xlsx')
    for sheet_name, xlsxdf in monitoring_ipr.items():
        xlsxdf.to_excel(writer, sheet_name, index=False)
    writer.save()
