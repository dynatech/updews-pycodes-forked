# -*- coding: utf-8 -*-
"""
Created on Tue Jun  4 14:27:21 2019

@author: DELL
"""

from datetime import datetime
import os
import pandas as pd
import sys


sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import dynadb.db as db
import gsm.gsmserver_dewsl3.sms_data as sms


def get_rain_gauges():
    localdf = 0
    query = "select gauge_name, rain_id from rainfall_gauges where data_source = 'senslope' and date_deactivated is null"
    localdf = db.df_read(query, connection='analysis')
    return localdf

def get_data(lgrname):
    query= "SELECT max(ts) FROM "+ 'rain_' + lgrname + "  where ts > '2010-01-01' order by ts desc limit 1 "
    localdf = db.df_read(query, connection='analysis')
    return localdf

def dftosql(df):
    gdf = get_rain_gauges()
    logger_active = pd.DataFrame()
    for i in range (0,len(gdf)):
        logger_active= logger_active.append(get_data(gdf.gauge_name[i]))

    logger_active = logger_active.reset_index()
    timeNow= datetime.today()
    df['last_data'] = logger_active['max(ts)']
    df['last_data'] = pd.to_datetime(df['last_data'])   
    df['ts_updated'] = timeNow
    df['rain_id'] = gdf.rain_id
    diff = df['ts_updated'] - df['last_data']
    tdta = diff
    fdta = tdta.astype('timedelta64[D]')
    df['diff_days'] = fdta

    df.loc[(df['diff_days'] > -1) & (df['diff_days'] < 3), 'presence'] = 'active' 
    df['presence'] = df['diff_days'].apply(lambda x: '1' if x <= 3 else '0') 

    data_table = sms.DataTable('data_presence_rain_gauges', df)
    db.df_write(data_table, connection='analysis')
    return df

def main():
    columns = ['rain_id', 'presence', 'last_data', 'ts_updated', 'diff_days']
    df = pd.DataFrame(columns=columns)
    
    query = "DELETE FROM data_presence_rain_gauges"
    db.write(query, connection='analysis')
    dftosql(df)

###############################################################################
if __name__ == '__main__':
    main()