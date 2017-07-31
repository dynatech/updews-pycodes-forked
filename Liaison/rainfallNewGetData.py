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
    engine = create_engine('mysql+pymysql://updews:october50sites@127.0.0.1/senslopedb')
    query = "select timestamp, rain from senslopedb.%s where timestamp between '%s' and  '%s'" % (rsite ,fdate,tdate)
    df = pd.io.sql.read_sql(query,engine)
    df.columns = ['ts','rain']
    df = df.set_index(['ts'])
    df = df["rain"].astype(float)
    df = df.resample('15Min', how = "sum")
    dfs = pd.rolling_sum(df,96,min_periods=1)
    dfs1 = pd.rolling_sum(df,288,min_periods=1)
    dfs = dfs[dfs>=0]
    dfs1 = dfs1[dfs1>=0]
    dfa = pd.DataFrame({"rval":df,"hrs24":dfs,"hrs72":dfs1})    
    dfajson = dfa.reset_index().to_json(orient="records",date_format='iso')
    dfajson = dfajson.replace("T"," ").replace("Z","").replace(".000","")
    print dfajson
    
getDF();