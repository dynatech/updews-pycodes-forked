
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

engine = create_engine('mysql+pymysql://root:senslope@127.0.0.1/senslopedb')
query = "SELECT name,rain_senslope ,rain_arq,RG1,RG2,RG3 FROM senslopedb.rain_props order by name asc"
df = pd.io.sql.read_sql(query,engine)
#print df
all_rainguage = []
all_data= []
#for name in df.rain_senslope:
#    if(name != None):
#         all_rainguage.append(name)
#         
#for name in df.rain_arq:
#    if(name != None):
#         all_rainguage.append(name)

for name in df.RG1:
    if(name != None):
         all_rainguage.append(name)

for name in df.RG2:
    if(name != None):
         all_rainguage.append(name)

for name in df.RG3:
    if(name != None):
         all_rainguage.append(name)
         
for site in all_rainguage:
   print site
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
print new_df
new_df.to_csv('//var//www//html//temp//data//rainfall_data.csv')
#dfajson = new_df.reset_index().to_json(orient="records",date_format='iso')
#dfajson = dfajson.replace("T"," ").replace("Z","").replace(".000","")
#print dfajson


