
import os
import sys
import pandas as pd
import numpy as np
from datetime import timedelta as td
from datetime import datetime as dt
import sqlalchemy
from sqlalchemy import create_engine
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../updews-pycodes/Analysis/'))
if not path in sys.path:
   sys.path.insert(1, path)
del path
import ConvertSomsRaw as CSR
import SomsRangeFilter  
def getDF():

        site = sys.argv[1]
        fdate = sys.argv[2]
        tdate = sys.argv[3]
        nid = sys.argv[4]
        mode = sys.argv[5]
#        site = 'agbsb'
#        fdate = '2016-04-02'
#        tdate = '2016-04-10'
#        nid = '2'
#        mode = '1'
        df= CSR.getsomsrawdata(site,nid,fdate,tdate)
        df = pd.DataFrame(df,columns=['raw'])
        if mode == '0':
            dfajson = df.reset_index().to_json(orient="records",date_format='iso')
            dfajson = dfajson.replace("T"," ").replace("Z","").replace(".000","")
        else:
            df_filt = SomsRangeFilter.f_outlier(df,site,int(nid),int(mode))
            dfajson = df_filt.reset_index().to_json(orient='records',date_format='iso')
            dfajson = dfajson.replace("T"," ").replace("Z","").replace(".000","")
            
        print dfajson
getDF();