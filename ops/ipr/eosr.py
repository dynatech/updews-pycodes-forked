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
    sched = ipr_lib.remove_downtime(sched, downtime)
    sched = sched.loc[sched.event == 1, :]

    
    for name in monitoring_ipr.keys():
        indiv_ipr = monitoring_ipr[name]
        indiv_ipr.columns = indiv_ipr.columns.astype(str)
        for ts in indiv_ipr.columns[5:]:
            ts = pd.to_datetime(ts)
            ts_end = ts + timedelta(0.5)
            shift_release = sched.loc[(sched.data_ts > ts) & (sched.data_ts <= ts_end), :]
            if ts >= pd.to_datetime('2021-04-01') and len(shift_release) != 0:
                shift_eval = eval_df.loc[(eval_df.shift_ts >= ts) & (eval_df.shift_ts <= ts+timedelta(1)) & ((eval_df['evaluated_MT'] == name) | (eval_df['evaluated_CT'] == name) | (eval_df['evaluated_backup'] == name)), :].drop_duplicates('shift_ts', keep='last')
                shift_eval = shift_eval.drop_duplicates('shift_ts', keep='last')[0:1]
                indiv_ipr.loc[indiv_ipr.Output2 == 'EoSR', str(ts)] = np.round((len(shift_release) - np.nansum(shift_eval['eosr']))/len(shift_release), 2)
                deduction = np.nansum(shift_eval[['routine_tag', 'surficial_tag', 'response_tag', 'rain_tag', 'call_log']].values)/15
                indiv_ipr.loc[indiv_ipr.Output1.str.contains('narrative', na=False), str(ts)] = np.round((len(shift_release) - deduction)/len(shift_release), 2)
                indiv_ipr.loc[indiv_ipr.Output2 == 'plot attachment', str(ts)] = np.round((len(shift_release) - 0.2*np.nansum(shift_eval['plot']))/len(shift_release), 2)
                indiv_ipr.loc[indiv_ipr.Output2 == 'subsurface analysis', str(ts)] = np.round((len(shift_release) - np.nansum(shift_eval['subsurface'])/3)/len(shift_release), 2)
                indiv_ipr.loc[indiv_ipr.Output2 == 'surficial analysis', str(ts)] = np.round((len(shift_release) - np.nansum(shift_eval['surficial'])/3)/len(shift_release), 2)
                indiv_ipr.loc[indiv_ipr.Output2 == 'rainfall analysis', str(ts)] = np.round((len(shift_release) - np.nansum(shift_eval['rain'])/3)/len(shift_release), 2)
                moms_release = shift_release.loc[shift_release.moms == 1, :]
                if len(moms_release) != 0:
                    indiv_ipr.loc[indiv_ipr.Output2 == 'moms analysis', str(ts)] = np.round((len(moms_release) - np.nansum(shift_eval['moms'])/3)/len(moms_release), 2)
                eq_release = shift_release.loc[shift_release.eq == 1, :]
                if len(eq_release) != 0:
                    indiv_ipr.loc[indiv_ipr.Output2 == 'eq analysis', str(ts)] = np.round((len(eq_release) - np.nansum(shift_eval['eq'])/3)/len(eq_release), 2)
            if ts >= pd.to_datetime('2021-10-01') and len(shift_release) != 0:
                indiv_ipr.loc[indiv_ipr.Output2 == 'narratives', str(ts)] = 1
        monitoring_ipr[name] = indiv_ipr
        
    writer = pd.ExcelWriter(output_path + 'monitoring_ipr.xlsx')
    for sheet_name, xlsxdf in monitoring_ipr.items():
        xlsxdf.to_excel(writer, sheet_name, index=False)
    writer.save()
