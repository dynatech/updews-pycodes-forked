
import pandas as pd
import numpy as np
from datetime import timedelta as td
from datetime import datetime as dt
import sqlalchemy
from sqlalchemy import create_engine
import sys
import requests
import csv
import json

engine = create_engine('mysql+mysqldb://root:senslope@192.168.150.129/senslopedb')
query = "SELECT name FROM senslopedb.site_column_props order by name asc"
df = pd.io.sql.read_sql(query,engine)
all_data= []
for site in df.name:
   filtered_data =[]
   query_count = "select count(*) from senslopedb.%s"%(site)
   df_count = pd.io.sql.read_sql(query_count,engine)
   df_count.columns = ['count']
   df_count['site'] = site
   query_latest = "select timestamp from senslopedb.%s  order by  timestamp desc limit 1"%(site)
   df_latest = pd.io.sql.read_sql(query_latest,engine)
   filtered_data.append(site)
   filtered_data.append(df_count['count'][0])
   if len( df_latest['timestamp']) == 1:
        filtered_data.append(df_latest['timestamp'][0])
   else:
        filtered_data.append('null')
        
   all_data.append(filtered_data)
   print filtered_data
new_df = pd.DataFrame(all_data)
new_df.columns = ['site','count','latest']
new_df.to_csv('C:\\xampp\\htdocs\\temp\\data\\sensor_data.csv')
#dfajson = new_df.reset_index().to_json(orient="records",date_format='iso')
#dfajson = dfajson.replace("T"," ").replace("Z","").replace(".000","")
#print dfajson


