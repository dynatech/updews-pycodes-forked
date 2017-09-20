
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
import os

list_days =['3days']
t_num =[3,7,14,31,90,180]
total_data = []
for i, day in enumerate(list_days):
    
    days = day
    tdate = dt.strptime('2017-09-18', "%Y-%m-%d")
    fdate = tdate - td(days=t_num[i])
    print fdate,tdate
    engine = create_engine('mysql+pymysql://root:senslope@127.0.0.1/senslopedb')
    query = "SELECT name FROM senslopedb.site_column_props order by name asc"
    df = pd.io.sql.read_sql(query,engine)
    all_data= []
    for site in df.name:
       collected = []
       query_latest = "select timestamp,id from senslopedb.%s  where timestamp between '%s' and '%s' order by timestamp desc"%(site,fdate,tdate)
       df_latest = pd.io.sql.read_sql(query_latest,engine)
       dfa = pd.DataFrame(df_latest)
       dfajson = dfa.reset_index().to_json(orient="records",date_format='iso')
       dfajson = dfajson.replace("T"," ").replace("Z","").replace(".000","")
       collected.append({'site':site,'data':dfajson})
       all_data.append({'item':collected})
    all_data = pd.DataFrame(all_data)
    dfajson_all = all_data.to_json(orient="records",date_format='iso')
    dfajson_all = dfajson_all.replace("T"," ").replace("Z","").replace(".000","")
#    print dfajson_all
    script_dir = os.path.dirname(__file__)
    file_path = os.path.join(script_dir, '//var//www//html//temp//data//json_sensor_data.json')
    with open(file_path, "w") as json_file:
       json_string = json.dumps(dfajson_all)
       json_file.write(json_string)
       