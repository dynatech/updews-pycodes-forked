
import os
import sys
import pandas as pd
import numpy as np
from datetime import timedelta as td
from datetime import datetime as dt
import sqlalchemy
from sqlalchemy import create_engine
import requests
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../updews-pycodes/Analysis/Soms'))
if not path in sys.path:
   sys.path.insert(1, path)
del path
import SomsRangeFilter 
import ConvertSomsRaw as CSR
    
site = sys.argv[1]
fdate = sys.argv[2]
tdate = sys.argv[3]
nid = sys.argv[4]
mode = sys.argv[5]

#site = 'mngsa'
#fdate = '2016-12-28'
#tdate = '2016-12-29'
#nid = '4'
#mode = '1'

if mode == '0':
   df = CSR.getsomsrawdata(column=site,gid=int(nid),fdate=fdate,tdate=tdate,if_multi = False)
else:
   df = CSR.getsomscaldata(column=site,gid=int(nid),fdate=fdate,tdate=tdate,is_debug= False,if_multi = False)

df_filt = SomsRangeFilter.f_outlier(df,site,int(mode))
dfajson = df_filt.reset_index().to_json(orient='records',date_format='iso')
dfajson = dfajson.replace("T"," ").replace("Z","").replace(".000","")
print dfajson
