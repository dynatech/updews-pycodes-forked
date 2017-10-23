import pandas as pd
import numpy as np
from datetime import timedelta as td
from datetime import datetime as dt
import sqlalchemy
from sqlalchemy import create_engine
import sys
import requests
import json

#
gsite = sys.argv[1]
fdate = sys.argv[2].replace("%20"," ")
tdate = sys.argv[3].replace("%20"," ")

#gsite = 'bar'
#fdate = '2013-01-01'
#tdate = '2017-01-01'
engine = create_engine('mysql+pymysql://root:senslope@127.0.0.1/senslopedb')
query = "SELECT id,timestamp, UPPER(crack_id) AS crack_id,meas FROM senslopedb.gndmeas where timestamp between '%s' and '%s' and site_id ='%s' and meas <= '500' order by site_id asc"%(fdate,tdate,gsite) 
df = pd.io.sql.read_sql(query,engine)
df.columns = ['id','ts','crack_id','meas']
df = df.set_index(['ts'])
dfajson = df.reset_index().to_json(orient='records',date_format='iso')
dfajson = dfajson.replace("T"," ").replace("Z","").replace(".000","")

try:
    dfajson_data = dfajson
    print dfajson_data
except Exception:
   pass
