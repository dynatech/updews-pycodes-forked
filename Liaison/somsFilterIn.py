
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
    
#site = "mngsa"
#fdate = "2016-12-28"
#tdate = "2016-12-29"
#mode = '1'

site = sys.argv[1]
fdate = sys.argv[2]
tdate = sys.argv[3]
mode = sys.argv[4]

if mode == '0':
   df = CSR.getsomsrawdata(column=site, fdate=fdate, tdate=tdate, if_multi=True )
else:
   df = CSR.getsomscaldata(column=site, fdate=fdate, tdate=tdate ,is_debug= False,if_multi = True)

df_filt = SomsRangeFilter.f_outlier(df,site,int(mode))
dfajson = df_filt.reset_index().to_json(orient='records',date_format='iso')
dfajson = dfajson.replace("T"," ").replace("Z","").replace(".000","")
print dfajson
