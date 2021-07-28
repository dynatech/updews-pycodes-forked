# -*- coding: utf-8 -*-
"""
Created on Thu Jul  1 11:13:53 2021

@author: Meryll
"""

import os
import pandas as pd
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import dynadb.db as dbio
import gsm.smsparser2.smsclass as smsclass
import analysis.surficial.markeralerts as ma


def get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP):    
    excel_column_number = list(map(lambda x: ord(x.upper()) - 65, excel_column_letter))
    public_alert_column_number = ord(public_alert_column_letter.upper()) - 65
    IOMP_col_num = []
    for col in IOMP:
        col_num = ord(col[-1]) - 65
        if len(col) == 2:
            col_num += 26
        IOMP_col_num += [col_num]
    usecols = [0,1] + excel_column_number + [public_alert_column_number] + IOMP_col_num
    names = ['date', 'time'] + marker_name + ['public_alert', 'MT', 'CT']
    df = pd.read_excel('(NEW) Site Monitoring Database.xlsx', skiprows=[0,1], na_values=['ND', '-'], usecols=usecols, names=names, parse_dates=[[0,1]])
    df = df.dropna(subset=marker_name, how='all')
    df = df.rename(columns={'date_time': 'ts'})
    df.loc[:, 'public_alert'] = df['public_alert'].fillna('A0')
    mon_type = {'A0': 'routine', 'A0-R': 'event', 'ND-R': 'event', 'A1': 'event', 'A2': 'event'}
    df.loc[:, 'meas_type'] = df.public_alert.map(mon_type)
    df.loc[:, 'MT'] = df['MT'].fillna('')
    df.loc[:, 'CT'] = df['CT'].fillna('')
    df.loc[:, 'observer_name'] = df.apply(lambda row: row.MT + ' ' + row.CT, axis=1)
    df = df.loc[:, ['ts'] + marker_name + ['meas_type', 'observer_name']]
    df.loc[:, 'site_code'] = site_code.lower()
    df.to_csv('surficial_{}.csv'.format(site_code.lower()), index=False)
    
    return df


def write_observation(surf_df, site_id):
    mo_df = surf_df.loc[:, ['site_id', 'ts', 'meas_type', 'observer_name']]
    mo_df.loc[:, 'data_source'] = 'ops'
    mo_df.loc[:, 'reliability'] = 1
    mo_df.loc[:, 'weather'] = 'maaraw'
    mo_id = dbio.df_write(data_table=smsclass.DataTable("marker_observations", mo_df), resource='sensor_data', last_insert=True)[0][0]
    
    surf_df = surf_df.dropna(axis=1)
    md_df = surf_df.loc[:, surf_df.columns.astype(str).str.isnumeric()].transpose()
    md_df = md_df.reset_index()
    md_df.columns = ['marker_id', 'measurement']
    md_df.loc[:, 'mo_id'] = mo_id
    dbio.df_write(data_table = smsclass.DataTable("marker_data", 
            md_df), resource='sensor_data')
    
    ma.generate_surficial_alert(site_id, ts = mo_df.ts.values[0])
                             

def write_surficial(site_code):
    df = pd.read_csv('surficial_{}.csv'.format(site_code.lower()))
    
    query = "SELECT * FROM commons_db.sites"    
    sites = dbio.df_read(query, connection='common')
    site_id = sites.loc[sites.site_code == site_code, 'site_id'].values[0]
    
    query = "SELECT * FROM analysis_db.site_markers where site_id = {}".format(site_id)    
    site_markers = dbio.df_read(query, resource='sensor_data')
    
    dct = dict(site_markers.set_index('marker_name')['marker_id'])
    dct['ts'] = 'ts'
    dct['site_code'] = 'site_code'
    dct['meas_type'] = 'meas_type'
    dct['observer_name'] = 'observer_name'
    df.columns = df.columns.map(dct)
    df.loc[:, 'site_id'] = site_id
    df = df.sort_values('ts')
    
    obs = df.groupby('ts', as_index=False)
    obs.apply(write_observation, site_id=site_id)
    

if __name__ == '__main__':
        
    site_code= 'mar'
    sheet_name = 'MAR'
    marker_name = ['A', 'B', 'C', 'D']
    excel_column_letter = ['K', 'L', 'N', 'M']
    public_alert_column_letter = 'Q'
    IOMP = ['Y', 'Z']
    
    df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
    write_surficial(site_code)
