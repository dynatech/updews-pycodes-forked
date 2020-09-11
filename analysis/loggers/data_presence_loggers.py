# -*- coding: utf-8 -*-
"""
Created on Mon Aug 17 16:07:27 2020

@author: Dynaslope
"""

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


def get_loggers_v2():
    localdf = 0
    query = """select logger_name, logger_id from loggers as lg
    inner join logger_models using (model_id)
    where date_deactivated is NULL
    and logger_type in ('arq', 'regular', 'router')
	and logger_name not in ('testa', 'testb', 'testc')
    and logger_name not like '%___r_%' 
    and logger_name not like '%___pz%' 
    and logger_name not like '%___x_%'"""
    localdf = db.df_read(query, connection='common')
    return localdf

def get_loggers_v3():
    localdf=0
    query = """select logger_name, logger_id from loggers
    inner join logger_models using (model_id)
    where logger_type in ('gateway','arq')
    and logger_name like '%___r_%'
    or logger_name like '%___g%' 
    and lg.logger_name not in ("madg")"""
    localdf = db.df_read(query, connection='common')
    return localdf

def get_data_rain(lgrname):
    query= "SELECT max(ts) FROM " + 'rain_' + lgrname + "  where ts >= '2010-01-01' order by ts desc limit 1 "
    localdf = db.df_read(query, connection='analysis')
    return localdf

def get_data_tsm(lgrname):
    query= "SELECT max(ts) FROM " + 'tilt_' + lgrname + "  where ts >= '2010-01-01' order by ts desc limit 1 "
    localdf = db.df_read(query, connection='analysis')
    return localdf

def dftosql(df):
    v2df = get_loggers_v2()
    v3df = get_loggers_v3()
    logger_active = pd.DataFrame()
    loggers = v2df.append(v3df).reset_index()
    
    logger_active = pd.DataFrame()
    for i in range (0,len(v2df)):
        logger_active= logger_active.append(get_data_tsm(v2df.logger_name[i]))
    
    for i in range (0,len(v3df)):
        logger_active= logger_active.append(get_data_rain(v3df.logger_name[i]))
    
    logger_active = logger_active.reset_index()   
    timeNow= datetime.today()
    df['last_data'] = logger_active['max(ts)']
    df['last_data'] = pd.to_datetime(df['last_data'])   
    df['ts_updated'] = timeNow
    df['logger_id'] = loggers.logger_id
    diff = df['ts_updated'] - df['last_data']
    tdta = diff
    fdta = tdta.astype('timedelta64[D]')
    df['diff_days'] = fdta
    
    df.loc[(df['diff_days'] > -1) & (df['diff_days'] < 3), 'presence'] = 'active' 
    df['presence'] = df['diff_days'].apply(lambda x: '1' if x <= 3 else '0') 

    data_table = sms.DataTable('data_presence_loggers', df)
    db.df_write(data_table, connection='analysis')
  
    return df

def main():
    columns = ['logger_id', 'presence', 'last_data', 'ts_updated', 'diff_days']
    df = pd.DataFrame(columns=columns)
    
    query = "DELETE FROM data_presence_loggers"
    db.write(query, connection='analysis')
    dftosql(df)

###############################################################################
if __name__ == '__main__':
    main()