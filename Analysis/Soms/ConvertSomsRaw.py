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

def getsomsrawdata(column="", gid=0, fdate="", tdate="", if_multi = False):
    ''' 
        only for landslide sensors v2 and v3
        output:  sraw = series of unfiltered SOMS data (raw) of a specific node of the defined column 
        param:
            column = column name (ex. laysam)
            gid = geographic id of node [1-40]
    '''
    
    v2=['NAGSA', 'BAYSB', 'AGBSB', 'MCASB', 'CARSB', 'PEPSB','BLCSA']
    v3=[ 'lpasa','lpasb','laysa','laysb','imesb','barsc','messb','imusc','oslsc',
         'mngsa','gaasa','gaasb','hinsa','hinsb','talsa' ]
    df = pd.DataFrame(columns=['sraw', 'scal'])
#    print 'getsomsdata: ' + column + ',' + str(gid)
    try:
        df = qs.GetSomsData(siteid=column+'m', fromTime=fdate, toTime=tdate, targetnode=gid)

    except:
        print 'No data available for ' + column.upper()
        return df
        
    df.index = df.ts

    if column.upper() in v2:
        if column.upper()=='NAGSA':
		 if if_multi:
			df = df[(df.msgid == 21)]	
			df['output'] =(((8000000/(df.mval1))-(8000000/(df.mval2)))*4)/10
			sraw = df[['id','mval1']]
		 else:
			sraw =(((8000000/(df.mval1[(df.msgid==21)]))-(8000000/(df.mval2[(df.msgid==21)])))*4)/10
					
        else:
		if if_multi:
			df = df[(df.msgid == 111)]
			df['output'] =(((20000000/(df.mval1))-(20000000/(df.mval2)))*4)/10    
			sraw = df[['id','output']]   
		else:
			sraw =(((20000000/(df.mval1[(df.msgid==111)]))-(20000000/(df.mval2[(df.msgid==111)])))*4)/10 
			
    elif column.lower() in v3: # if version 3
        if if_multi:
		df = df[(df.msgid == 110)]
		sraw = df[['id','mval1']]
        else:
		sraw=df.mval1[(df.msgid==110)]
		
		
    else:
        sraw=pd.Series()
        pass
    
    return sraw

def getsomscaldata(column="", gid=0, fdate="", tdate="",is_debug= False, if_multi = False):
    ''' 
        only for landslide sensors v2 and v3
        output:  df = series of unfiltered SOMS data (calibrated/normalized) of a specific node of the defined column 
        param:
            column = column name (ex. laysa)
            gid = geographic id of node [1-40]
    '''
    
    v2=['NAGSA', 'BAYSB', 'AGBSB', 'MCASB', 'CARSB', 'PEPSB','BLCSA']
    v3=[ 'lpasa','lpasb','laysa','laysb','imesb','barsc','messb','imusc','oslsc',
         'mngsa','gaasa','gaasb','hinsa','hinsb','talsa' ]
    df = pd.DataFrame(columns=['sraw', 'scal'])
    df = pd.DataFrame()

    if column.upper() in v2:
        if column.upper()=='NAGSA':
            msgid = 26
        else:
            msgid = 112
    elif column.lower() in v3: # if version 3
            msgid = 113
    else:
	  if (is_debug == True):
	        print 'No data available for ' + column.upper()
	        return df  
	  else:
              return df
        
    try:
        df = qs.GetSomsData(siteid=column+'m', fromTime=fdate, toTime=tdate, targetnode=gid, msgid=msgid)
        df.index=df.ts      
        if if_multi:
	        df = df[['id','mval1']]
        else:
		  df= df.mval1
	          
	   
								
    except:
        if (is_debug == True):
	        print 'No data available for ' + column.upper()
	        return df  
        else:
              return df

    return df
