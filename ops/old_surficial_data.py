# -*- coding: utf-8 -*-
"""
Created on Thu Jul  1 11:13:53 2021

@author: Meryll
"""

import numpy as np
import os
import pandas as pd
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import dynadb.db as dbio
import gsm.smsparser2.smsclass as smsclass
import analysis.surficial.markeralerts as ma


def get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP): 
    excel_column_number = np.array(list(map(lambda x: 26*(len(x)-1), excel_column_letter)))
    excel_column_number = list(excel_column_number + np.array(list(map(lambda x: ord(x[-1].upper()) - 65, excel_column_letter))))
    if len(public_alert_column_letter) > 1:
        public_alert_column_letter = public_alert_column_letter[-1]
        public_alert_column_number = 26
    else:
        public_alert_column_number = 0
    public_alert_column_number += ord(public_alert_column_letter.upper()) - 65
    IOMP_col_num = []
    for col in IOMP:
        col_num = ord(col[-1]) - 65
        if len(col) == 2:
            col_num += 26
        IOMP_col_num += [col_num]
    usecols = [0,1] + excel_column_number + [public_alert_column_number] + IOMP_col_num
    names = ['date', 'time'] + marker_name + ['public_alert', 'MT', 'CT']
    df = pd.read_excel('(NEW) Site Monitoring Database.xlsx', sheet_name=sheet_name, skiprows=[0,1], na_values=['ND', '-'], usecols=usecols, names=names, parse_dates=[[0,1]])

    df = df.dropna(subset=marker_name, how='all')
    df = df.rename(columns={'date_time': 'ts'})
    df.loc[:, 'public_alert'] = df['public_alert'].fillna('A0')
    mon_type = {'A0': 'routine', 'A0-R': 'event', 'ND-R': 'event', 'A1': 'event', 'A2': 'event', 'ND-L': 'event'}
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
    if mo_id == 0:
        query = "SELECT marker_observations.mo_id FROM marker_observations "
        query += "WHERE ts = '{ts}' and site_id = '{site_id}'"
        query = query.format(ts=surf_df['ts'].values[0], site_id=site_id)
        mo_id = dbio.read(query, resource='sensor_data')[0][0]
    surf_df = surf_df.dropna(axis=1)
    md_df = surf_df.loc[:, surf_df.columns.astype(str).str.isnumeric()].transpose()
    md_df = md_df.reset_index()
    md_df.columns = ['marker_id', 'measurement']
    md_df.loc[:, 'mo_id'] = mo_id
    dbio.df_write(data_table = smsclass.DataTable("marker_data", 
            md_df), resource='sensor_data')
    ma.generate_surficial_alert(site_id, ts = mo_df.ts.values[0])
                             

def write_surficial(site_code):
    print(site_code)
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
        
#	site_code= 'agb'
#	sheet_name = 'AGB'
#	marker_name = ['A']
#	excel_column_letter = ['K']
#	public_alert_column_letter = 'N'
#	IOMP = ['V', 'W']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'bak'
#	sheet_name = 'BAK'
#	marker_name = ['A', 'B', 'C', 'D', 'E', 'F']
#	excel_column_letter = ['L', 'M', 'N', 'O', 'P', 'Q']
#	public_alert_column_letter = 'T'
#	IOMP = ['AB', 'AC']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'ban'
#	sheet_name = 'BAN'
#	marker_name = ['A', 'B', 'C']
#	excel_column_letter = ['K', 'L', 'M']
#	public_alert_column_letter = 'P'
#	IOMP = ['X', 'Y']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'bar'
#	sheet_name = 'BAR'
#	marker_name = ['A', 'B', 'C', 'D', 'E']
#	excel_column_letter = ['L', 'M', 'N', 'O', 'P']
#	public_alert_column_letter = 'S'
#	IOMP = ['AA', 'AB']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'bay'
#	sheet_name = 'BAY'
#	marker_name = ['A', 'B']
#	excel_column_letter = ['L', 'M']
#	public_alert_column_letter = 'P'
#	IOMP = ['X', 'Y']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'blc'
#	sheet_name = 'Copy of BLC'
#	marker_name = ['A', 'B', 'I', 'J', 'K', 'L']
#	excel_column_letter = ['C', 'D', 'E', 'F', 'G', 'H']
#	public_alert_column_letter = 'I'
#	IOMP = ['J','K']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'bol'
#	sheet_name = 'Copy of BOL'
#	marker_name = ['A', 'B', 'C', 'D', 'E', 'F']
#	excel_column_letter = ['C', 'D', 'E', 'F', 'G', 'H']
#	public_alert_column_letter = 'I'
#	IOMP = ['J', 'K']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'bto'
#	sheet_name = 'BTO'
#	marker_name = ['A', 'B', 'C']
#	excel_column_letter = ['K', 'L', 'M']
#	public_alert_column_letter = 'P'
#	IOMP = ['X', 'Y']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'car'
#	sheet_name = 'Copy of CAR'
#	marker_name = ['A', 'B', 'C', 'D']
#	excel_column_letter = ['C', 'D', 'E', 'F']
#	public_alert_column_letter = 'G'
#	IOMP = ['H', 'I']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'cud'
#	sheet_name = 'Copy of CUD'
#	marker_name = ['A', 'B', 'C', 'D', 'E']
#	excel_column_letter = ['C', 'D', 'E', 'F', 'G']
#	public_alert_column_letter = 'H'
#	IOMP = ['I', 'J']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'dad'
#	sheet_name = 'DAD'
#	marker_name = ['A', 'B', 'C', 'D', 'E']
#	excel_column_letter = ['K', 'L', 'M', 'N', 'O']
#	public_alert_column_letter = 'R'
#	IOMP = ['Z', 'AA']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'gaa'
#	sheet_name = 'GAA'
#	marker_name = ['A', 'B', 'C']
#	excel_column_letter = ['K', 'L', 'M']
#	public_alert_column_letter = 'P'
#	IOMP = ['X', 'Y']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'gam'
#	sheet_name = 'Copy of GAM'
#	marker_name = ['A', 'B']
#	excel_column_letter = ['C', 'D']
#	public_alert_column_letter = 'E'
#	IOMP = ['F', 'G']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'hin'
#	sheet_name = 'Copy of HIN'
#	marker_name = ['A', 'B', 'C', 'D', 'E']
#	excel_column_letter = ['C', 'D', 'E', 'F', 'G']
#	public_alert_column_letter = 'H'
#	IOMP = ['I', 'J']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)

	site_code= 'hum'
	sheet_name = 'HUM'
	marker_name = ['B', 'A']
	excel_column_letter = ['K', 'L']
	public_alert_column_letter = 'O'
	IOMP = ['W', 'X']

	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
	write_surficial(site_code)

#	site_code= 'ime'
#	sheet_name = 'IME'
#	marker_name = ['A', 'B', 'C']
#	excel_column_letter = ['K', 'L', 'M']
#	public_alert_column_letter = 'P'
#	IOMP = ['X', 'Y']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'imu'
#	sheet_name = 'Copy of IMU'
#	marker_name = ['A', 'B', 'C']
#	excel_column_letter = ['C', 'D', 'E']
#	public_alert_column_letter = 'F'
#	IOMP = ['G', 'H']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code = 'ina'
#	sheet_name = 'Copy of INA'
#	marker_name = ['A', 'B', 'C', 'D', 'E', 'F']
#	excel_column_letter = ['C', 'D', 'E', 'F', 'G', 'H']
#	public_alert_column_letter = 'I'
#	IOMP = ['J', 'K']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'jor'
#	sheet_name = 'Copy of JOR'
#	marker_name = ['A', 'B']
#	excel_column_letter = ['C', 'D']
#	public_alert_column_letter = 'E'
#	IOMP = ['F', 'G']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'lab'
#	sheet_name = 'Copy of LAB'
#	marker_name = ['A', 'B', 'C', 'D', 'E']
#	excel_column_letter = ['C', 'D', 'E', 'F', 'G']
#	public_alert_column_letter = 'H'
#	IOMP = ['I','J']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'lay'
#	sheet_name = 'Copy of LAY'
#	marker_name = ['A', 'B', 'C', 'D']
#	excel_column_letter = ['C', 'D', 'E', 'F']
#	public_alert_column_letter = 'G'
#	IOMP = ['H', 'I']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'lip'
#	sheet_name = 'Copy of LIP'
#	marker_name = ['A', 'B', 'C']
#	excel_column_letter = ['C', 'D', 'E']
#	public_alert_column_letter = 'F'
#	IOMP = ['G', 'H']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'loo'
#	sheet_name = 'Copy of LOO'
#	marker_name = ['A', 'B', 'C']
#	excel_column_letter = ['C', 'D', 'E']
#	public_alert_column_letter = 'F'
#	IOMP = ['G', 'H']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'lpa'
#	sheet_name = 'Copy of LPA'
#	marker_name = ['A', 'B', 'C']
#	excel_column_letter = ['C', 'D', 'E']
#	public_alert_column_letter = 'F'
#	IOMP = ['G', 'H']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'lte'
#	sheet_name = 'Copy of LTE'
#	marker_name = ['A', 'B', 'C']
#	excel_column_letter = ['C', 'D', 'E']
#	public_alert_column_letter = 'F'
#	IOMP = ['G', 'H']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'mag'
#	sheet_name = 'MAG'
#	marker_name = ['A', 'B', 'C', 'D', 'E']
#	excel_column_letter = ['M', 'N', 'O', 'P', 'Q']
#	public_alert_column_letter = 'T'
#	IOMP = ['AB', 'AC']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)

	site_code= 'mar'
	sheet_name = 'MAR'
	marker_name = ['A', 'B', 'D', 'C']
	excel_column_letter = ['K', 'L', 'M', 'N']
	public_alert_column_letter = 'Q'
	IOMP = ['Y', 'Z']

	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
	write_surficial(site_code)

#	site_code= 'mca'
#	sheet_name = 'MCA'
#	marker_name = ['A', 'B', 'C', 'D', 'E']
#	excel_column_letter = ['K', 'L', 'M', 'N', 'O']
#	public_alert_column_letter = 'R'
#	IOMP = ['Z', 'AA']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'mng'
#	sheet_name = 'MNG'
#	marker_name = ['A', 'B', 'C', 'D']
#	excel_column_letter = ['K', 'L', 'M', 'N']
#	public_alert_column_letter = 'Q'
#	IOMP = ['Y', 'Z']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'msl'
#	sheet_name = 'MESSB'
#	marker_name = ['A', 'B', 'C', 'D', 'E', 'F']
#	excel_column_letter = ['K', 'L', 'M', 'N', 'O', 'P']
#	public_alert_column_letter = 'S'
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#	IOMP = ['AA', 'AB']
#	site_code= 'msu'
#	sheet_name = 'MSU'
#	marker_name = ['G', 'H', 'I', 'J']
#	excel_column_letter = ['K', 'L', 'M', 'N']
#	public_alert_column_letter = 'Q'
#	IOMP = ['Y', 'Z']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'nur'
#	sheet_name = 'NUR'
#	marker_name = ['A']
#	excel_column_letter = ['K']
#	public_alert_column_letter = 'N'
#	IOMP = ['V', 'W']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'pep'
#	sheet_name = 'PEP'
#	marker_name = ['A', 'B', 'C']
#	excel_column_letter = ['L', 'M', 'N']
#	public_alert_column_letter = 'Q'
#	IOMP = ['Y', 'Z']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'pin'
#	sheet_name = 'PIN'
#	marker_name = ['A', 'B']
#	excel_column_letter = ['K', 'L']
#	public_alert_column_letter = 'O'
#	IOMP = ['W', 'X']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'pla'
#	sheet_name = 'PLA'
#	marker_name = ['A', 'B', 'C']
#	excel_column_letter = ['K', 'L', 'M']
#	public_alert_column_letter = 'P'
#	IOMP = ['X', 'Y']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'png'
#	sheet_name = 'PNG'
#	marker_name = ['A', 'B', 'C']
#	excel_column_letter = ['L', 'M', 'N']
#	public_alert_column_letter = 'Q'
#	IOMP = ['Y', 'Z']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'sum'
#	sheet_name = 'SUM'
#	marker_name = ['A', 'B']
#	excel_column_letter = ['K', 'L']
#	public_alert_column_letter = 'O'
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#	IOMP = ['W', 'X']
#	site_code= 'tal'
#	sheet_name = 'TAL'
#	marker_name = ['A', 'B', 'C']
#	excel_column_letter = ['K', 'L', 'M']
#	public_alert_column_letter = 'P'
#	IOMP = ['X', 'Y']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'tga'
#	sheet_name = 'TAG'
#	marker_name = ['A', 'B']
#	excel_column_letter = ['K', 'L']
#	public_alert_column_letter = 'O'
#	IOMP = ['W', 'X']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'umi'
#	sheet_name = 'UMI'
#	marker_name = ['A', 'B', 'C']
#	excel_column_letter = ['K', 'L', 'M']
#	public_alert_column_letter = 'P'
#	IOMP = ['X', 'Y']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'osl'
#	sheet_name = 'OSL'
#	marker_name = ['A', 'B', 'C', 'D', 'E', 'F']
#	excel_column_letter = ['L', 'M', 'N', 'O', 'P', 'Q']
#	public_alert_column_letter = 'T'
#	IOMP = ['AB', 'AC']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'lun'
#	sheet_name = 'Copy of LUN'
#	marker_name = ['A', 'B', 'C', 'D']
#	excel_column_letter = ['L', 'M', 'N', 'O']
#	public_alert_column_letter = 'U'
#	IOMP = ['AC', 'AD']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	# PAR - 2 sets of data with overlapping timestamps
#
#	site_code= 'par'
#	sheet_name = 'PAR'
#	marker_name = ['A', 'B', 'C', 'D', 'E']
#	excel_column_letter = ['K', 'L', 'M', 'N', 'O']
#	public_alert_column_letter = 'R'
#	IOMP = ['Z', 'AA']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'mam'
#	sheet_name = 'Copy of MAM'
#	marker_name = ['A', 'B', 'C', 'D', 'F']
#	excel_column_letter = ['K', 'L', 'M', 'N', 'O']
#	public_alert_column_letter = 'T'
#	IOMP = ['AB', 'AC']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'sag'
#	sheet_name = 'SAG'
#	marker_name = ['C', 'D', 'E', 'F', 'G', 'K', 'M']
#	excel_column_letter = ['K', 'L', 'M', 'N', 'O', 'P', 'Q']
#	public_alert_column_letter = 'T'
#	IOMP = ['AB', 'AC']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'tue'
#	sheet_name = 'TUE'
#	marker_name = ['A', 'B', 'C', 'E', 'F']
#	excel_column_letter = ['K', 'L', 'M', 'N', 'O']
#	public_alert_column_letter = 'R'
#	IOMP = ['Z', 'AA']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'sin'
#	sheet_name = 'Copy of SIN'
#	marker_name = ['A', 'B', 'C', 'D', 'F', 'G', 'H', 'I']
#	excel_column_letter = ['L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S']
#	public_alert_column_letter = 'V'
#	IOMP = ['AD', 'AE']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'sib'
#	sheet_name = 'SIB'
#	marker_name = ['A', 'B']
#	excel_column_letter = ['K', 'L']
#	public_alert_column_letter = 'O'
#	IOMP = ['W', 'X']
#
#	df = get_surficial_data(site_code, sheet_name, marker_name, excel_column_letter, public_alert_column_letter, IOMP)
#	write_surficial(site_code)
#
#	site_code= 'nag'
#	sheet_name = 'Copy of NAG'
#	marker_name = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J','K', 'L', 'M', 'N', '10', '11', '12']
#	excel_column_letter = ['K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'AA']
#	public_alert_column_letter = 'AI'
#	IOMP = ['AQ', 'AR']
#
