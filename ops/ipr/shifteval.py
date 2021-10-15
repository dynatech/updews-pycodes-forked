from datetime import timedelta
import lib
import numpy as np
import pandas as pd


def get_eval_df():
    personnel = lib.get_personnel()

    # 2020 eval_df
    key = "1uS_Jd5JDucXqI3b7edWl5Szm6rK3klj7BVGFkszIq6I"
    sheet_name = "Form responses 1"
    eval_df = lib.get_sheet(key, sheet_name)
    eval_df = eval_df.loc[:, ['First submit timestamp', 'x', 'Column B', 'Column C', 'Column BI',
                        'Column F', 'Column G', 'Column BC']]
    eval_df = eval_df.dropna(how='all')[1::]
    eval_df.columns = ['first_ts', 'last_ts', 'MT', 'CT', 'backup', 'date', 'time', 'remarks']
    eval_df.loc[:, 'MT'] = eval_df.MT.map(personnel.set_index('Name').to_dict()['Nickname'])
    eval_df.loc[:, 'CT'] = eval_df.CT.map(personnel.set_index('Name').to_dict()['Nickname'])
    eval_df.loc[:, 'backup'] = eval_df.backup.map(personnel.set_index('Name').to_dict()['Nickname'])
    fmt = '%d/%m/%Y %H:%M:%S'
    eval_df.loc[:, 'first_ts'] = pd.to_datetime(eval_df.first_ts, format=fmt)
    eval_df.loc[:, 'last_ts'] = pd.to_datetime(eval_df.last_ts, format=fmt)
    eval_df.loc[:, 'date'] = pd.to_datetime(eval_df.date, format='%d/%m/%Y')
    eval_df.loc[:, 'time'] = eval_df.time.map({'AM shift (7:30 AM - 8:30 PM)': 8, 'PM shift (7:30 PM - 8:30 AM)': 20})
    eval_df.loc[:, 'shift_ts'] = eval_df.loc[:, ['date', 'time']].apply(lambda row: row.date + timedelta(hours=row.time), axis=1)
    
    # 2021 eval_df
    key = "1ESijuvovL-yTFY4J_0P5lLQBZjOHKpIgssWLbbX8wq8"
    sheet_name = "Form Responses 1"
    df = lib.get_sheet(key, sheet_name, drop_unnamed=False)
    df.columns = ['first_ts', 'last_ts', 'MT', 'CT', 'backup',
                  'evaluated_MT', 'evaluated_CT', 'evaluated_backup', 
                  'date', 'time', 'Unnamed: 10', 
                  'routine_surficial_data', 'Unnamed: 12', 
                  'routine_web_alert_ts', 'routine_web_alert_level', 'Unnamed: 15',
                  'routine_sms_ts', 'routine_sms_alert', 'routine_sms_typo', 'Unnamed: 19',
                  'routine_tag', 'Unnamed: 21', 'Unnamed: 22', 'Unnamed: 23',
                  'surficial_data', 'Unnamed: 25', 
                  'web_alert_ts', 'web_alert_level', 'Unnamed: 28',
                  'sms_ts', 'sms_alert', 'sms_typo', 'Unnamed: 32',
                  'bul_ts', 'bul_alert', 'bul_typo', 'Unnamed: 36',
                  'rain_det', 'rain_typo', 'Unnamed: 39', 
                  'surficial_tag', 'response_tag', 'rain_tag', 'call_log', 'Unnamed: 44',
                  'plot', 'subsurface', 'surficial', 'moms', 'rain', 'eq', 'Unnamed: 51',
                  'Unnamed: 52', 'Unnamed: 53', 'contacts', 'Unnamed: 55', 'Unnamed: 56',
                  'bug_log', 'relayed', 'Unnamed: 59', 'sc_log', 'responded', 'Unnamed: 62',
                  'resubmission', 'eosr', 'Unnamed: 65', 'Unnamed: 66', 'Unnamed: 67', 
                  'Unnamed: 68', 'Unnamed: 69', 'Unnamed: 70', 'Unnamed: 71', 'Unnamed: 72', 'Unnamed: 73']
    df = df.loc[df['Unnamed: 62'].apply(lambda x: str(x).lower().strip() != 'test'), :]
    df = df.drop([col for col in df.columns if col.startswith('Unnamed')], axis=1)
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
    
    eval_df = eval_df.append(df, ignore_index=True)
    
    return eval_df


def eval_timeliness(eval_df):
    monitoring_ipr = pd.read_excel('output/monitoring_ipr.xlsx', sheet_name=None, dtype=str)
    ewi_sms = pd.read_csv('output/sending_status.csv')
    ewi_sms.loc[:, 'ts_start'] = pd.to_datetime(ewi_sms.loc[:, 'ts_start'])
    
    
    for name in monitoring_ipr.keys() -set({'Summary'}):
        indiv_ipr = monitoring_ipr[name]
        indiv_ipr.columns = indiv_ipr.columns.astype(str)
        indiv_ipr = indiv_ipr.drop([col for col in indiv_ipr.columns if col.startswith('Unnamed')], axis=1)
        for ts in indiv_ipr.columns[5:]:
            ts = pd.to_datetime(ts)
            sending_status = ewi_sms.loc[(ewi_sms.ts_start > ts-timedelta(0.5)) & (ewi_sms.ts_start <= ts), :]
#            shift_type = indiv_ipr.loc[indiv_ipr.Category == 'Shift', str(ts)].values[0]
            shift_eval = eval_df.loc[(eval_df.shift_ts+timedelta(0.5) >= ts) & (eval_df.shift_ts+timedelta(0.5) <= ts+timedelta(1)) & ((eval_df['MT'] == name) | (eval_df['CT'] == name) | (eval_df['backup'] == name)), :].drop_duplicates('shift_ts', keep='last')
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
            sending_status = ewi_sms.loc[(ewi_sms.ts_start > ts) & (ewi_sms.ts_start <= ts+timedelta(0.5)), :]
            if len(sending_status) != 0:
                indiv_ipr.loc[indiv_ipr.Category == 'Monitoring Evaluation', str(ts)] = grade
                indiv_ipr.loc[indiv_ipr.Output1 == 'monitoring evaluation', str(ts)] = 1
            else:
                indiv_ipr.loc[(indiv_ipr.Output2 == 'monitoring eval') & (indiv_ipr.Percentage2 == '0.5'), str(ts)] = grade
                indiv_ipr.loc[(indiv_ipr.Output2 == 'monitoring eval') & (indiv_ipr.Percentage2 == '0.25'), str(ts)] = 1
        monitoring_ipr[name] = indiv_ipr
    
    writer = pd.ExcelWriter('output/monitoring_ipr.xlsx')
    for sheet_name, xlsxdf in monitoring_ipr.items():
        xlsxdf.to_excel(writer, sheet_name, index=False)
    writer.save()
