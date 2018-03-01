import os
import sys
import time
from datetime import datetime
import pandas as pd
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../updews-pycodes/Data Analysis/GroundAlert/'))
if not path in sys.path:
   sys.path.insert(1, path)
del path
import GroundDataAlertLibWithTrending as test


    
def getDF():
        site = sys.argv[1]
        cid = sys.argv[2].replace("%20"," ");
        end_date = sys.argv[3].replace("%20"," ");
        df= test.GroundDataTrendingPlotJSON(site, cid, end_date)
        df = pd.DataFrame(df,columns=['raw'])
        dfajson = df.reset_index().to_json(orient="records",date_format='iso')
        dfajson = dfajson.replace("T"," ").replace("Z","").replace(".000","")
getDF();