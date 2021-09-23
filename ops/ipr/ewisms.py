from datetime import timedelta
import numpy as np
import pandas as pd

import dynadb.db as db
import ewisms_meal
import lib


def main(start, end, eval_df, recompute=False, mysql=True):
    if recompute:
        ewisms_meal.main(start=start, end=end, mysql=mysql)

    monitoring_ipr = pd.read_excel('output/monitoring_ipr.xlsx', sheet_name=None)
    ewi_sms = pd.read_csv('output/sending_status.csv', parse_dates=['data_ts', 'ts_start', 'ts_end', 'queued'])
    downtime = lib.system_downtime(mysql=mysql)
    ewi_sms = lib.remove_downtime(ewi_sms, downtime)
    
    for name in monitoring_ipr.keys():
        indiv_ipr = monitoring_ipr[name]
        indiv_ipr.columns = indiv_ipr.columns.astype(str)
        for ts in indiv_ipr.columns[5:]:
            ts = pd.to_datetime(ts)
            sending_status = ewi_sms.loc[(ewi_sms.data_ts >= ts) & (ewi_sms.data_ts < ts+timedelta(0.5)), :]
            if len(sending_status) == 0:
                indiv_ipr.loc[indiv_ipr.Output2.str.contains('EWI SMS', na=False), str(ts)] = np.nan
            elif sum(sending_status.tot_unsent) == 0 and all(sending_status.queued <= sending_status.ts_end):
                # all sent on time
                indiv_ipr.loc[indiv_ipr.Output2.str.contains('EWI SMS', na=False), str(ts)] = 1
            else:
                sending_status.loc[:, 'deduction'] = 0.1 * np.ceil((sending_status.queued - sending_status.ts_end).dt.total_seconds()/(60*3))
                sending_status.loc[sending_status.deduction > 1, 'deduction'] = 1
                sending_status.loc[sending_status.deduction < 0, 'deduction'] = 0
                sending_status.loc[:, 'deduction'].fillna(1, inplace = True)
                indiv_ipr.loc[indiv_ipr.Output2.str.contains('EWI SMS', na=False), str(ts)] = np.round((sum(sending_status.min_recipient) - sum(sending_status.tot_unsent * sending_status.deduction)) / sum(sending_status.min_recipient), 2)
            if ts >= pd.to_datetime('2021-04-01') and len(sending_status) != 0:
                shift_eval = eval_df.loc[(eval_df.shift_ts >= ts) & (eval_df.shift_ts <= ts+timedelta(1)) & ((eval_df['evaluated_MT'] == name) | (eval_df['evaluated_CT'] == name) | (eval_df['evaluated_backup'] == name)), :].drop_duplicates('shift_ts', keep='last')[0:1]
                deduction = np.nansum(shift_eval[['routine_sms_alert', 'sms_alert']].values) + np.nansum(shift_eval[['routine_sms_ts', 'sms_ts']].values)/3 + np.nansum(shift_eval[['routine_sms_typo', 'sms_typo']].values)/30
                indiv_ipr.loc[indiv_ipr.Output1 == 'EWI SMS', str(ts)] = np.round((len(sending_status) - deduction)/len(sending_status), 2)
        monitoring_ipr[name] = indiv_ipr
    
    writer = pd.ExcelWriter('output/monitoring_ipr.xlsx')
    for sheet_name, xlsxdf in monitoring_ipr.items():
        xlsxdf.to_excel(writer, sheet_name, index=False)
    writer.save()