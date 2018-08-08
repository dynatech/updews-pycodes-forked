import pandas as pd
import numpy as np
from datetime import timedelta as td
from datetime import datetime as dt
import sqlalchemy
from sqlalchemy import create_engine
import sys
import requests
    

def getDF():

    rsite = sys.argv[1]
    fdate = sys.argv[2].replace("%20"," ")
    tdate = sys.argv[3].replace("%20"," ")
    # rsite = "1069"
    # fdate = "2014-04-25"
    # tdate = "2017-04-25"
    end = pd.to_datetime(tdate)-td(minutes=30)
    engine = create_engine('mysql+pymysql://updews:october50sites@127.0.0.1/senslopedb')
    query = "select timestamp, rval from senslopedb.rain_noah_%s " %rsite
    query += "where timestamp >= '%s' and timestamp < '%s'" %(pd.to_datetime(fdate)-td(3), tdate)
    df = pd.io.sql.read_sql(query,engine)
    df.columns = ['ts','rain']
    df = df[df.rain >= 0]
    df = df.set_index(['ts'])
    df = df.resample('30Min').sum()
    
    df_inst = df.resample('30Min').sum()
    
    if max(df_inst.index) < end:
        new_data = pd.DataFrame({'ts': [end], 'rain': [0]})
        new_data = new_data.set_index(['ts'])
        df = df.append(new_data)
        df = df.resample('30Min').sum()
          
    df1 = pd.rolling_sum(df,48,min_periods=1)
    df3 = pd.rolling_sum(df,144,min_periods=1)
    
    df['rval'] = df_inst
    df['hrs24'] = df1
    df['hrs72'] = df3
    
    df = df[(df.index >= fdate)&(df.index <= end)]
       
    dfajson = df.reset_index().to_json(orient="records",date_format='iso')
    dfajson = dfajson.replace("T"," ").replace("Z","").replace(".000","")
    print dfajson
    
getDF();