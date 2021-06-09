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


def main(start, end, eval_df, mysql=False):
    downtime = lib.system_downtime(mysql=mysql)
    ewi_sched = pd.read_csv('output/sending_status.csv', parse_dates=['ts_start', 'ts_end'])
    ewi_sched = lib.remove_downtime(ewi_sched, downtime)
    ewi_sched = ewi_sched.loc[ewi_sched.event == 1, :]
    
    monitoring_ipr = pd.read_excel('output/monitoring_ipr.xlsx', sheet_name=None)
    
    for name in monitoring_ipr.keys():
        indiv_ipr = monitoring_ipr[name]
        indiv_ipr.columns = indiv_ipr.columns.astype(str)
        for ts in indiv_ipr.columns[5:]:
            ts = pd.to_datetime(ts)
            ts_end = ts + timedelta(0.5)
            shift_release = ewi_sched.loc[(ewi_sched.ts_start > ts) & (ewi_sched.ts_start <= ts_end), :]
            if ts >= pd.to_datetime('2021-04-01') and len(shift_release) != 0:
                shift_eval = eval_df.loc[(eval_df.shift_ts >= ts) & (eval_df.shift_ts <= ts+timedelta(1)) & ((eval_df['evaluated_MT'] == name) | (eval_df['evaluated_CT'] == name) | (eval_df['evaluated_backup'] == name)), :].drop_duplicates('shift_ts', keep='last')
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
                eq_release = shift_release.loc[shift_release.moms == 1, :]
                if len(eq_release) != 0:
                    indiv_ipr.loc[indiv_ipr.Output2 == 'eq analysis', str(ts)] = np.round((len(eq_release) - np.nansum(shift_eval['moms'])/3)/len(eq_release), 2)

        monitoring_ipr[name] = indiv_ipr
        
    writer = pd.ExcelWriter('output/monitoring_ipr.xlsx')
    for sheet_name, xlsxdf in monitoring_ipr.items():
        xlsxdf.to_excel(writer, sheet_name, index=False)
    writer.save()
