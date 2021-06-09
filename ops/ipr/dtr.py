from __future__ import print_function
from datetime import timedelta
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

from datetime import time
import numpy as np
import pandas as pd

import lib


def retrieve_dtr():
    personnel = lib.get_personnel()
    ### admin's dtr file list
    gauth = GoogleAuth()
    gauth.LocalWebserverAuth() # client_secrets.json need to be in the same directory as the script
    drive = GoogleDrive(gauth)
    
    folder_dict = {2020: '1IIzPXlKos9GDNHrIXnO0KerW6rCB_8Ij', 
                   2021: '1-0cidZzINCpH2rGse8QJfItnwdxs5HLp'}
    for year in folder_dict.keys():
        writer = pd.ExcelWriter('output/monitoring_dtr.xlsx')
        
        file_list = drive.ListFile({'q': "'{}' in parents and trashed=false".format(folder_dict.get(year))}).GetList()
        for file in file_list:
            title = file['title'].split('.')
            print(title)
            dtr_id = int(title[0])
            if dtr_id in personnel.dtr_id.values:
                sheet_name = personnel.loc[personnel.dtr_id == dtr_id, 'Nickname'].values[0]
                print(sheet_name)
                key = file['id']
                url = 'https://docs.google.com/spreadsheets/d/{key}/export?format=xlsx&id={key}'.format(key=key)
                dct = pd.read_excel(url, sheet_name=None, usecols='A:I', nrows = 34,
                                   names=['ts', 'in1', 'out1', 'in2', 'out2',
                                          'in3', 'out3', 'in4', 'out4'],
                                   header=None, dtype=str)
                dtr_df = pd.DataFrame()
                for month in dct.keys():
                    month_df = dct.get(month)
                    if not month_df.empty:
                        try:
                            ts = pd.to_datetime('{} 1, {}'.format(month, year))
                        except: 
                            ts = pd.to_datetime('{} 1, {}'.format(month_df.ts[0], year))
                        if ts < pd.to_datetime('2020-09-01'):
                            continue
                        month_df = month_df.loc[(month_df.index != 0) & (~month_df.ts.isnull()), :]
                        month_df.loc[:, 'ts'] = month_df.loc[:, 'ts'].apply(lambda x: ts + timedelta(int(x)-1))
                        month_df = month_df.set_index('ts')
                        month_df = month_df.dropna(axis=1, how='all').dropna(axis=0, how='all')
                        dtr_df = dtr_df.append(month_df, ignore_index=False)
                dtr_df = dtr_df.replace({'00:00:00': '24:00:00', '1900-01-01 00:00:00': '24:00:00', '1900-01-01 10:00:00': '10:00:00'})
        
                col_list = dtr_df.columns
                dtr_df = dtr_df.reset_index()
                for col in col_list:
                    dtr_df.loc[~dtr_df[col].isnull(), col] = dtr_df.loc[~dtr_df[col].isnull(), col].apply(lambda x: 60*int(x.split(':')[0])+int(x.split(':')[1]))
                    dtr_df.loc[~dtr_df[col].isnull(), col] = dtr_df.loc[~dtr_df[col].isnull(), ['ts', col]].apply(lambda x: str(x.ts + timedelta(minutes=x[col])), axis=1)
        
                
                try:
                    prev_dtr_df = pd.read_excel('output/monitoring_dtr.xlsx', sheet_name=sheet_name)
                    prev_dtr_df.loc[:, 'ts'] = pd.to_datetime(prev_dtr_df.loc[:, 'ts'])
                except:
                    prev_dtr_df = pd.DataFrame()
                dtr_df = prev_dtr_df.append(dtr_df, ignore_index=True)
                dtr_df = dtr_df.drop_duplicates('ts', keep='last')
                dtr_df.to_excel(writer, sheet_name, index=False)
        
        writer.save()
    

def get_online_dtr():
    key = "1wZhFAvBDMF03fFxlnXoJJ1sH4iOSlN8a2DmOMYW_IxM"
    sheet_name = "dtr"
    online_dtr = lib.get_sheet(key, sheet_name)
    personnel = lib.get_personnel()
    df = pd.merge(online_dtr, personnel, left_on='Name', right_on='Name').loc[:, ['Timestamp', 'Nickname']]
    df.loc[:, 'Timestamp'] = pd.to_datetime(df.Timestamp)
    df = df.rename(columns = {'Timestamp': 'ts', 'Nickname': 'name'})
    return df


def eval_online_dtr(name, ts, ts_end, online_dtr):
    log = online_dtr.loc[(online_dtr.name == name) & (online_dtr.ts.dt.date == ts.date()), 'ts']
    if len(log) == 0:
        return 0
    time_in = min(log)
    if time_in.time() <= time(ts.hour-1, 45):
        grade = 1
    else:
        grade = np.round((ts_end+timedelta(minutes=15) - time_in) / timedelta(hours=12.5), 2)
    return grade


def eval_dtr(ts, ts_end, indiv_dtr):
    shift_dtr = indiv_dtr.loc[(indiv_dtr.index == str(ts.date())) | (indiv_dtr.index == str(ts_end.date())), :]
    if len(shift_dtr) == 0:
        print ('no in', ts)
        return 0
    if ts.time() == time(20,0):
        time_in = max(pd.to_datetime(shift_dtr.in3[0]), ts-timedelta(minutes=15))
        if len(shift_dtr) < 2:
            time_out = ts_end+timedelta(minutes=15)
        else:
            time_out = min(pd.to_datetime(shift_dtr.out1[1]), ts_end+timedelta(minutes=15))
        grade = np.round((time_out - time_in) / timedelta(hours=12.5), 2)
    else:
        shift_dtr.loc[shift_dtr.in1 < (ts-timedelta(minutes=15)), 'in1'] = ts-timedelta(minutes=15)
        col_name = (shift_dtr > ts_end).reset_index().melt(id_vars='ts').query('value == True').variable
        shift_dtr.loc[:, col_name] = ts_end+timedelta(minutes=15)
        if all(shift_dtr.out1.dt.time == time(12,0)) & all(shift_dtr.in2.dt.time == time(13,0)):
            grade = np.round((shift_dtr.out2 - shift_dtr.in1) / timedelta(hours=12.5), 2).values[0]
        else:
            tot = 0
            for i in range(1,int( len(shift_dtr.dropna(axis=1).columns)/2)+1):
                tot += shift_dtr[['in'+str(i), 'out'+str(i)]].diff(axis=1).values[0][-1]
            grade = np.round(tot/np.timedelta64(int(12.5*60), 'm'), 2)
    return grade

def main(update_dtr = False):
    
    if update_dtr:
        retrieve_dtr()
    
    monitoring_ipr = pd.read_excel('output/monitoring_ipr.xlsx', sheet_name=None, dtype=str)
    monitoring_dtr = pd.read_excel('output/monitoring_dtr.xlsx', sheet_name=None, parse_dates=True)
    online_dtr = get_online_dtr()
    ewi_sms = pd.read_csv('output/sending_status.csv')
    ewi_sms.loc[:, 'ts_start'] = pd.to_datetime(ewi_sms.loc[:, 'ts_start'])

    for name in monitoring_ipr.keys() -set({'Summary'}):
        indiv_ipr = monitoring_ipr[name]
        indiv_ipr.columns = indiv_ipr.columns.astype(str)
        indiv_dtr = monitoring_dtr[name].set_index('ts')
        indiv_dtr = indiv_dtr.apply(lambda x: pd.to_datetime(x))
        indiv_dtr = indiv_dtr.sort_index()
        for ts in indiv_ipr.columns[5:]:
            ts = pd.to_datetime(ts)
            ts_end = ts + timedelta(0.5)
            if ts < pd.to_datetime('2020-09-01'):
                grade = eval_online_dtr(name, ts, ts_end, online_dtr)
            else:
                grade = eval_dtr(ts, ts_end, indiv_dtr)
            sending_status = ewi_sms.loc[(ewi_sms.ts_start > ts) & (ewi_sms.ts_start <= ts+timedelta(0.5)), :]
            if len(sending_status) != 0:
                indiv_ipr.loc[indiv_ipr.Category == 'Personnel timeliness', str(ts)] = grade
            else:
                indiv_ipr.loc[indiv_ipr.Output2 == 'personnel timeliness', str(ts)] = grade
                indiv_ipr.loc[indiv_ipr.Category == 'Personnel timeliness', str(ts)] = np.nan
        monitoring_ipr[name] = indiv_ipr
    
    writer = pd.ExcelWriter('output/monitoring_ipr.xlsx')
    for sheet_name, xlsxdf in monitoring_ipr.items():
        xlsxdf.to_excel(writer, sheet_name, index=False)
    writer.save()
