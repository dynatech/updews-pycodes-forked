import os
import sys
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../updews-pycodes/Analysis/'))
if not path in sys.path:
   sys.path.insert(1, path)
del path
from querySenslopeDb import *
import filterSensorData
import pandas as pd
import numpy as np
from datetime import timedelta as td
from datetime import datetime as dt
import sqlalchemy
from sqlalchemy import create_engine
import sys
import requests 
    

site = sys.argv[1]
fdate = sys.argv[2]
tdate = sys.argv[3]
nid = sys.argv[4].replace("-",",")
ms = sys.argv[5]
#site = "agbta"
#fdate = "2016-02-28"
#tdate = "2017-02-28"
#nid = "1-2-3".replace("-",",")
#ms = "32"
engine = create_engine('mysql+pymysql://updews:october50sites@127.0.0.1/senslopedb')
query = "SELECT timestamp,id,msgid,xvalue,yvalue,zvalue,batt FROM senslopedb.%s where id in (%s) and msgid ='%s' and timestamp between '%s ' and '%s'" % (site,nid,ms,fdate,tdate)
df = pd.io.sql.read_sql(query,engine)
df.columns = ['ts','id','msgid','x','y','z','v']
df['name'] = site
df_filt = filterSensorData.applyFilters(df, orthof=True, rangef=True, outlierf=True)
dfajson = df_filt.reset_index().to_json(orient='records',date_format='iso')
dfajson = dfajson.replace("T"," ").replace("Z","").replace(".000","")
print dfajson
