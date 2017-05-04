import pandas as pd
import numpy as np
from datetime import timedelta as td
from datetime import datetime as dt
import sqlalchemy
from sqlalchemy import create_engine
import sys
import requests
import json
import os
    
fdate = dt.now()
tdate = dt.now() - td(hours=4)
#tdate = '2017-03-16 09:30:00'
#fdate ='2017-03-16 06:30:00'
engine = create_engine('mysql+pymysql://updews:october50sites@127.0.0.1/senslopedb')
query = "select * from senslopedb.node_level_alert where timestamp between '%s' and  '%s'" % (fdate,tdate)
df = pd.io.sql.read_sql(query,engine)   
dfajson = df.to_json(orient="records",date_format='iso')
dfajson = dfajson.replace("T"," ").replace("Z","").replace(".000","")
script_dir = os.path.dirname(__file__)
file_path = os.path.join(script_dir, '//var//www//html//temp//data//node_alert_json.json')
with open(file_path, "w") as json_file:
    json_string = json.dumps(dfajson)
    json_file.write(json_string)


    
