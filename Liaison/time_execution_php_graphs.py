import pandas as pd
import numpy as np
from datetime import timedelta as td
from datetime import datetime as dt
import sqlalchemy
from sqlalchemy import create_engine
import sys
import requests
import time
import csv
import webbrowser
engine = create_engine('mysql+pymysql://updews:october50sites@127.0.0.1/senslopedb')
query = "select * from senslopedb.rain_props"
dfnew = pd.io.sql.read_sql(query,engine)
all_time = []
all_rainguage_noah = []
#fdate = "2017-10-22 00:00:00"
#tdate = "2017-10-29 00:00:00"

fdate = raw_input('from: ')
tdate = raw_input('to: ')
#tdate = dt.strptime(tdata, "%Y-%m-%d %H:%M:%S")
#fdate = dt.strptime(fdata, "%Y-%m-%d %H:%M:%S")

rain_noah = []
rain_arq  = []
rain_senslope = []
for site in dfnew.name:
       print site
       webbrowser.get("C:/Program Files (x86)/Google/Chrome/Application/chrome.exe %s").open("http://localhost/data_analysis/Eos_onModal/20/rain/%s/%s/%s/test"%(site,fdate,tdate))
       time.sleep(45)