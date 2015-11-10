import pandas as pd
import numpy as np
from datetime import timedelta as td
from datetime import datetime as dt
import sqlalchemy
from sqlalchemy import create_engine
import sys
import outputFilteredData as ofd

def getLsbChange():
    df = ofd.getFilteredData()
    isDFempty = df.empty
    
    if isDFempty == True:
        print 'No Data Available...'
    else:
        del df['id']
        df = df.set_index(['ts'])

        df2 = df.copy()
        dfa = []

        df3 = df2.resample('30Min').fillna(method='pad')
        dfv = df3 - df3.shift(12) 

        if len(dfa) == 0:
            dfa = dfv.copy()
        else:
            dfa = dfa.append(dfv)
            
        dfa = dfa[pd.notnull(dfa.x)]

        dfajson = dfa.reset_index().to_json(orient="records",date_format='iso')
        dfajson = dfajson.replace("T"," ").replace("Z","").replace(".000","")
        print dfajson  
   
getLsbChange()