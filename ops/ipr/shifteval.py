from datetime import timedelta
import numpy as np
import os
import pandas as pd

import lib as ipr_lib


output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..//input_output//'))


def get_eval_df():
    personnel = ipr_lib.get_personnel()
    
    # 2021 eval_df
    key = "1ESijuvovL-yTFY4J_0P5lLQBZjOHKpIgssWLbbX8wq8"
    sheet_name = "Email notif"
    df = ipr_lib.get_sheet(key, sheet_name, drop_unnamed=False)
    df = df.loc[df.other_comments.apply(lambda x: str(x).lower().strip() != 'test'), :]
    df = df.drop([col for col in df.columns if col.startswith('Unnamed')], axis=1)
    df = df.drop([col for col in df.columns if col.startswith('other')], axis=1)
    df = df.drop([col for col in df.columns if col.endswith('mail')], axis=1)
    df = df.drop([col for col in df.columns if col.startswith('evaluator')], axis=1)
    df = df.drop(['Allow send', 'Shift', 'Template 1 - Send Status'], axis=1)
    df.loc[:, 'MT'] = df.MT.map(personnel.set_index('Name').to_dict()['Nickname'])
    df.loc[:, 'CT'] = df.CT.map(personnel.set_index('Name').to_dict()['Nickname'])
    df.loc[:, 'backup'] = df.backup.map(personnel.set_index('Name').to_dict()['Nickname'])
    df.loc[:, 'evaluated_MT'] = df.evaluated_MT.map(personnel.set_index('Name').to_dict()['Nickname'])
    df.loc[:, 'evaluated_CT'] = df.evaluated_CT.map(personnel.set_index('Name').to_dict()['Nickname'])
    df.loc[:, 'evaluated_backup'] = df.evaluated_backup.map(personnel.set_index('Name').to_dict()['Nickname'])
    df = df.dropna(subset=['CT', 'MT'], how='all')
    df.loc[:, 'first_ts'] = pd.to_datetime(df.first_ts)
    df.loc[:, 'last_ts'] = pd.to_datetime(df.last_ts)
    df.loc[:, 'date'] = pd.to_datetime(df.date)
    df.loc[:, 'time'] = df.time.map({'AM shift (7:30 AM - 8:30 PM)': 8, 'PM shift (7:30 PM - 8:30 AM)': 20})
    df.loc[:, 'shift_ts'] = df.loc[:, ['date', 'time']].apply(lambda row: row.date + timedelta(hours=row.time), axis=1)
    
    return df


def eval_timeliness(sched, eval_df):
    monitoring_ipr = pd.read_excel(output_path + 'monitoring_ipr.xlsx', sheet_name=None, dtype=str)
    sched.loc[:, 'data_ts'] = pd.to_datetime(sched.loc[:, 'data_ts'])
    
    
    for name in monitoring_ipr.keys() -set({'Summary'}):
        indiv_ipr = monitoring_ipr[name]
        indiv_ipr.columns = indiv_ipr.columns.astype(str)
        indiv_ipr = indiv_ipr.drop([col for col in indiv_ipr.columns if col.startswith('Unnamed')], axis=1)
        for ts in indiv_ipr.columns[5:]:
            ts = pd.to_datetime(ts)
            sending_status = sched.loc[(sched.data_ts > ts-timedelta(0.5)) & (sched.data_ts <= ts), :]
#            shift_type = indiv_ipr.loc[indiv_ipr.Category == 'Shift', str(ts)].values[0]
            shift_eval = eval_df.loc[(eval_df.shift_ts+timedelta(0.5) >= ts) & (eval_df.shift_ts+timedelta(0.5) <= ts+timedelta(1)) & ((eval_df['MT'] == name) | (eval_df['CT'] == name) | (eval_df['backup'] == name)), :].drop_duplicates('shift_ts', keep='last')
            shift_eval = shift_eval.drop_duplicates('shift_ts', keep='last')[0:1]
            # no eval (required for Apr 2021 onwards or non-AIM)
            if len(shift_eval) == 0 and (ts >= pd.to_datetime('2021-04-01 08:00') or len(sending_status) != 0):
                grade = 0
            # no eval required
            elif len(shift_eval) == 0:
                grade = np.nan
            # on time eval
            elif (shift_eval.last_ts <= ts+timedelta(hours=4)).values[-1]:
                grade = 1
            # late eval
            else:
                # resubmission: with reason
                if any(~shift_eval.resubmission.isnull()):
                    ts_sub = min(shift_eval.loc[:, ['first_ts', 'last_ts']].values[0])
                # late eval
                else:
                    ts_sub = shift_eval.last_ts.values[0]
                if ts_sub <= ts+timedelta(hours=4):
                    grade = 1
                else:
                    deduction = min(15, np.ceil((ts_sub-(ts+timedelta(hours=4))).total_seconds()/3600)*2)
                    grade = np.round((15-deduction)/15, 2)
            sending_status = sched.loc[(sched.data_ts > ts) & (sched.data_ts <= ts+timedelta(0.5)), :]
            if len(sending_status) != 0:
                indiv_ipr.loc[indiv_ipr.Category == 'Monitoring Evaluation', str(ts)] = grade
                indiv_ipr.loc[indiv_ipr.Output1 == 'monitoring evaluation', str(ts)] = 1
            else:
                indiv_ipr.loc[(indiv_ipr.Output2 == 'monitoring eval') & (indiv_ipr.Percentage2 == '0.5'), str(ts)] = grade
                indiv_ipr.loc[(indiv_ipr.Output2 == 'monitoring eval') & (indiv_ipr.Percentage2 == '0.25'), str(ts)] = 1
        monitoring_ipr[name] = indiv_ipr
    
    writer = pd.ExcelWriter(output_path + 'monitoring_ipr.xlsx')
    for sheet_name, xlsxdf in monitoring_ipr.items():
        xlsxdf.to_excel(writer, sheet_name, index=False)
    writer.save()
