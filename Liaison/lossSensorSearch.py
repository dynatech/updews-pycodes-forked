
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

list_days =['3days','1week','2weeks','1month','3months','6months']
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
       filtered_data =[]
       query_latest = "select * from senslopedb.%s  where timestamp between '%s' and '%s' order by timestamp desc"%(site,fdate,tdate)
       df_latest = pd.io.sql.read_sql(query_latest,engine)
       print site
       filtered_data.append(site)
       filtered_data.append(len(df_latest))   
       all_data.append(filtered_data)
    new_df = pd.DataFrame(all_data)
    new_df.columns = ['site','count']
    new_df['range']= days
    new_df.to_csv('//var//www//html//temp//data//sensor_data_%s.csv'%(list_days[i]))