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

columns = ['health_id', 'rain_id', 'data_presence', 'last_data', 'ts_updated']
#index = np.arange(47)
df = pd.DataFrame(columns=columns)


def getLoggerList():
    localdf=0
    db = MySQLdb.connect(host = '192.168.150.75', user = 'dyna_staff', passwd = 'accelerometer', db = 'senslopedb')
    db = MySQLdb.connect(host = '127.0.0.1', user = 'root', passwd = 'senslope', db = 'senslopedb')

    query = "select gauge_name, rain_id from senslopedb.rainfall_gauges where data_source = 'senslope' and date_deactivated is null"
    localdf = psql.read_sql(query,db)
    return localdf

def getPoints(lgrname):
    db = MySQLdb.connect(host = '192.168.150.75', user = 'dyna_staff', passwd = 'accelerometer', db = 'senslopedb')
#    db = MySQLdb.connect(host = '127.0.0.1', user = 'root', passwd = 'senslope', db = 'senslopedb')

    query= "SELECT max(ts) FROM "+ 'rain_' + lgrname + "  WHERE ts > '2010-01-01' and '2019-01-01' order by ts desc limit 1 "
    localdf = psql.read_sql(query,db)
    print localdf
    return localdf

def getEndTS():
#    db = MySQLdb.connect(host = '192.168.150.128', user = 'dyna_staff', passwd = 'accelerometer', db = 'senslopedb')
    db = MySQLdb.connect(host = '127.0.0.1', user = 'root', passwd = 'senslope', db = 'senslopedb')

    query= "SELECT gauge_name, date_deactivated FROM senslopedb.rainfall_gauges where rain_id in (select max(rain_id) from rainfall_gauges group by gauge_name) and date_deactivated is not null"
    try:
		localdf = psql.read_sql(query,db)
    except pd.io.sql.DatabaseError,e:
		print e
		print 'nag error:'
		localdf = 0
    return localdf

gdf = getLoggerList()
logger_active = pd.DataFrame()
for i in range (0,len(gdf)):
    logger_active= logger_active.append(getPoints(gdf.gauge_name[i]))
    print logger_active

logger_active = logger_active.reset_index()
timeNow= datetime.today()
a = getLoggerList()
df['last_data'] = logger_active['max(ts)']
df['last_data'] = pd.to_datetime(df['last_data'])   
df['ts_updated'] = timeNow
print df
#
def dftosql(df):
    df['rain_id'] = gdf.rain_id
    diff = df['ts_updated'] - df['last_data']
    df['time_delta'] = diff
    tdta = diff
    fdta = tdta.astype('timedelta64[D]')
    days = fdta.astype(int)
    df['time_delta'] = days   
    df['data_presence'] = df['time_delta'].apply(lambda x: 'Active' if x <= 3 else 'For Maintenance') 
    print df 
    engine=create_engine('mysql+mysqlconnector://pysys_local:NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg@192.168.150.75:3306/senslopedb', echo = False)
#    engine=create_engine('mysql+mysqlconnector://root:senslope@127.0.0.1:3306/senslopedb', echo = False)

    df.to_sql(name = 'rain_health', con = engine, if_exists = 'append', index = False)
    return df
dftosql(df)
#    


