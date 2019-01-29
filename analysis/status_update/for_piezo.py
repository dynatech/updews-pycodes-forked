
import time,serial,re,sys,traceback
import MySQLdb, subprocess
from datetime import datetime
from datetime import timedelta as td
import pandas as psql
import numpy as np
import MySQLdb, time 
from time import localtime, strftime
import pandas as pd
#import __init__
import itertools
import os
from sqlalchemy import create_engine
from dateutil.parser import parse

columns = ['health_id', 'piezo_id', 'data_presence', 'last_data', 'ts_updated']
index = np.arange(11)
df = pd.DataFrame(columns=columns, index = index)


def getLoggerList():
    localdf=0
    db = MySQLdb.connect(host = '192.168.150.75', user = 'pysys_local', passwd ='NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg', db = 'senslopedb')
    query = """SELECT 
            pz.piezo_id,
            lg.logger_name
            FROM
            (SELECT 
             *
             FROM
        senslopedb.piezo_sensors) AS pz
        INNER JOIN
        senslopedb.loggers AS lg ON lg.logger_id = pz.logger_id"""
    localdf = psql.read_sql(query,db)
    return localdf

def getPoints(lgrname):
    db = MySQLdb.connect(host = '192.168.150.75', user = 'dyna_staff', passwd = 'accelerometer', db = 'senslopedb')
    query= "SELECT max(ts) FROM "+ 'piezo_' + lgrname + "  WHERE ts > '2010-01-01' and '2019-01-01' order by ts desc limit 1 "
    localdf = psql.read_sql(query,db)
    print localdf
    return localdf


gdf = getLoggerList()
logger_active = pd.DataFrame()
for i in range (0,len(gdf)):
    logger_active= logger_active.append(getPoints(gdf.logger_name[i]))
    print logger_active

logger_active = logger_active.reset_index()
timeNow= datetime.today()
a = getLoggerList()
df['piezo_id'] = a.piezo_id
df['last_data'] = logger_active['max(ts)']
df['ts_updated'] = timeNow
print df

def dftosql(df):
    diff = df['ts_updated'] - df['last_data']
    df['time_delta'] = diff
    tdta = diff
    fdta = tdta.astype('timedelta64[D]')
    days = fdta.astype(int)
    df['time_delta'] = days


    df['data_presence'] = df['time_delta'].apply(lambda x: 'Active' if x <= 3 else 'For Maintenance') 
    print df 
    engine=create_engine('mysql+mysqlconnector://pysys_local:NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg@192.168.150.75:3306/senslopedb', echo = False)
    df.to_sql(name = 'piezo_health', con = engine, if_exists = 'append', index = False)
    return df
dftosql(df)


    


