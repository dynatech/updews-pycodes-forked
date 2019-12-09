from datetime import datetime
import MySQLdb
import pandas as psql
import pandas as pd
from sqlalchemy import create_engine

import sys
import os


sys.path.append(os.path.dirname(os.path.realpath(__file__)))
#sys.path.append('C:\\Users\\DELL\\Documents\\updews-pycodes\\trunk\\')

import analysis.querydb as qdb


columns = ['health_id', 'tsm_id', 'data_presence', 'last_data', 'ts_updated']
#index = np.arange(111)
df = pd.DataFrame(columns=columns)


def getLoggerList():
    localdf=0
   # db = MySQLdb.connect(host = '192.168.150.75', user = 'pysys_local', passwd = 'NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg', db = 'senslopedb')
#    db = MySQLdb.connect(host = '127.0.0.1', user = 'root', passwd = 'senslope', db = 'senslopedb')
    query = "select tsm_id, tsm_name from senslopedb.tsm_sensors where date_deactivated is null"
    localdf = qdb.get_db_dataframe(query)
    return localdf

def getPoints(lgrname):
    
    query= "SELECT max(ts) FROM "+ 'tilt_' + lgrname + "  where ts > '2010-01-01' and '2019-01-01' order by ts desc limit 1 "
    localdf = qdb.get_db_dataframe(query)
    print (localdf)
    return localdf

gdf = getLoggerList()
logger_active = pd.DataFrame()
for i in range (0,len(gdf)):
    logger_active= logger_active.append(getPoints(gdf.tsm_name[i]))
    print (logger_active)

logger_active = logger_active.reset_index()
timeNow= datetime.today()
a = getLoggerList()
df['last_data'] = logger_active['max(ts)']
df['last_data'] = pd.to_datetime(df['last_data'])   
df['ts_updated'] = timeNow

print (df)



def dftosql(df):
    df['tsm_id'] = gdf.tsm_id
    diff = df['ts_updated'].subtract(df['last_data'])
    df['time_delta'] = diff
    tdta = diff
    tdta = tdta.dropna()
    fdta = tdta.astype('timedelta64[D]')
    days = fdta.astype(int)
    df['time_delta'] = days

    df.loc[(df['time_delta'] > -1) & (df['time_delta'] < 3), 'data_presence'] = 'active' 
    df['data_presence'] = df['time_delta'].apply(lambda x: 'Active' if x <= 3 else 'For Maintenance') 
    print (df)
    engine=create_engine('mysql+mysqlconnector://pysys_local:NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg@192.168.150.75:3306/senslopedb', echo = False)
#    engine=create_engine('mysql+mysqlconnector://root:senslope@127.0.0.1:3306/senslopedb', echo = False)

    df.to_sql(name = 'logger_health', con = engine, if_exists = 'append', index = False)
    return df


dftosql(df)

