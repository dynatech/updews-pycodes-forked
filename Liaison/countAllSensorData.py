# -*- coding: utf-8 -*-
"""
Created on Thu Sep 14 14:08:34 2017

@author: USER
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Sep 14 14:01:15 2017

@author: USER
"""

import pandas as pd
import numpy as np
from datetime import timedelta as td
from datetime import datetime as dt
import sqlalchemy
from sqlalchemy import create_engine
import sys
import requests
import json


engine = create_engine('mysql+pymysql://root:senslope@127.0.0.1/senslopedb')
query = "SELECT name FROM senslopedb.site_column_props order by name asc"
df = pd.io.sql.read_sql(query,engine)
data_sets=[]
for site in df.name:
   query_count = "select count(*) from senslopedb.%s"%(site)
   df_count = pd.io.sql.read_sql(query_count,engine)
   df_count.columns = ['count']
   df_count['site'] = site
   query_latest = "select timestamp from senslopedb.%s  order by  timestamp desc limit 1"%(site)
   df_latest = pd.io.sql.read_sql(query_latest,engine)
   df_count['latest'] = df_latest 
   dfajson = df_count.to_json(orient='records',date_format='iso')
   dfajson = dfajson.replace("T"," ").replace("Z","").replace(".000","")
   data_sets.append(dfajson)

print data_sets
