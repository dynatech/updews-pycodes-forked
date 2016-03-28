# -*- coding: utf-8 -*-
"""
Created on Wed Jan 20 18:10:45 2016

@author: SENSLOPEY
"""

import pandas as pd
import os
import sys

#include the path of "Data Analysis" folder for the python scripts searching
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
    sys.path.insert(1,path)
del path   

import querySenslopeDb as qs

#column = raw_input('Enter column name: ')
#gid = int(raw_input('Enter id: '))

def getsomsrawdata(column="", gid=0, fdate="", tdate=""):
    ''' 
        only for landslide sensors v2 and v3
        output:  df = unfiltered SOMS data (calibrated and raw) of a specific node of the defined column 
        param:
            column = column name (ex. laysa)
            gid = geographic id of node [1-40]
    '''
    
    v2=['NAGSAM', 'BAYSBM', 'AGBSBM', 'MCASBM', 'CARSBM', 'PEPSBM','BLCSAM']
    df = pd.DataFrame(columns=['sraw', 'scal'])
    
    try:
        soms = qs.GetSomsData(siteid=column, fromTime=fdate, toTime=tdate, targetnode=gid)
    except:
        print 'No data available for ' + column.upper()
        return df
        
    soms.index = soms.ts

    if column.upper() in v2:
        if column.upper()=='NAGSAM':
            df.sraw =(((8000000/(soms.mval1[(soms.msgid==21)]))-(8000000/(soms.mval2[(soms.msgid==21)])))*4)/10
            df.scal=soms.mval1[(soms.msgid==26)]
        else:
            df.sraw =(((20000000/(soms.mval1[(soms.msgid==111)]))-(20000000/(soms.mval2[(soms.msgid==111)])))*4)/10
            df.scal=soms.mval1[(soms.msgid==112)]
        
    else: # if version 3
        df.sraw=soms.mval1[(soms.msgid==110)]
        df.scal=soms.mval1[(soms.msgid==113)]
        
    return df

#test = soms.getsomsrawdata(column, gid, fdate, tdate)
#print test