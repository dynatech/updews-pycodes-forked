# -*- coding: utf-8 -*-
"""
Created on Wed Jan 20 18:10:45 2016

@author: SENSLOPEY
"""

import pandas as pd
import SomsRangeFilter as SRF
#import os
#import sys

#include the path of "Data Analysis" folder for the python scripts searching
#path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
#if not path in sys.path:
#    sys.path.insert(1,path)
#del path   

import querySenslopeDb as qs

#column = raw_input('Enter column name: ')
#gid = int(raw_input('Enter id: '))

def getsomsrawdata(column="", gid=0, fdate="", tdate=""):
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
    print 'getsomsdata: ' + column + ',' + str(gid)
    try:
	df = qs.GetSomsData(siteid=column, fromTime=fdate, toTime=tdate, targetnode=gid)

				 
    except:
        print 'No data available for ' + column.upper()
        return df
        
    df.index = df.ts

    if column.upper() in v2:
        if column.upper()=='NAGSA':
            sraw =(((8000000/(df.mval1[(df.msgid==21)]))-(8000000/(df.mval2[(df.msgid==21)])))*4)/10
        else:
            sraw =(((20000000/(df.mval1[(df.msgid==111)]))-(20000000/(df.mval2[(df.msgid==111)])))*4)/10           

    elif column.lower() in v3: # if version 3
	sraw=df.mval1[(df.msgid==110)]

    else:
        sraw=pd.Series()
        pass
    
    return sraw

def getsomscaldata(column="", gid=0, fdate="", tdate=""):
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
        print 'No data available for ' + column.upper()
        return df  
        
    try:
	df = qs.GetSomsData(siteid=column, fromTime=fdate, toTime=tdate, targetnode=gid, msgid=msgid)
	df.index = df.ts
#	df = df.set_index("ts")
	df= df.mval1
	
	
    except:
        print 'No data available for ' + column.upper()
        return df  

    return df

#For filter data

def f_outlier(df,column,node,mode): 
	
	v2=['NAGSA', 'BAYSB', 'AGBSB', 'MCASB', 'CARSB', 'PEPSB','BLCSA']
	
	smin=[[2000,500],[0,0]]                         #format: [[v2raw_min, v3raw_min], [v2calib_min,v3calib_min]]
	smax=[[7800,1600],[1700,1500]]
	
	if column.upper() in v2:
		ver = 0
	else:
		ver = 1
            
	df= df[(df>smin[mode][ver])&(df<smax[mode][ver])]
	try:   
		df= pd.DataFrame(df.resample('30Min',base=0))
	except:
		return df
    
	return df

def f_undervoltage(df,column,node,mode):
    '''for v3 only'''
    v2=['NAGSA', 'BAYSB', 'AGBSB', 'MCASB', 'CARSB', 'PEPSB','BLCSA']
#    seek_undervoltage(df,column,node,mode)
    if column in v2:
        v_a1= qs.GetRawAccelData(siteid=column,targetnode=node, msgid=32, batt=1)
        v_a2= qs.GetRawAccelData(siteid=column,targetnode=node, msgid=33, batt=1)
    else:
        v_a1= qs.GetRawAccelData(siteid=column,targetnode=node, msgid=11, batt=1)
        v_a2= qs.GetRawAccelData(siteid=column,targetnode=node, msgid=12, batt=1)        
        
#    v_a1.index = v_a1.ts
    v_a1 = v_a1.set_index("ts")
    v_a1.rename(columns={'v':'v1'}, inplace=True)
    v_a1=v_a1.resample('30Min',base = 0).first()

#    v_a2 = v_a2.set_index("ts")
    v_a2.index = v_a2.ts
    v_a2.rename(columns={'v':'v2'}, inplace=True)
    v_a2=v_a2.resample('30Min', base =0).first()
    
    x=pd.concat([df,v_a1.v1,v_a2.v2],axis=1,ignore_index=True)
    x.columns=['mval1','v1','v2']
    x=x.resample('30Min').first()
    df=x.mval1[((x.v1>3.2) & (x.v1<3.4) & (x.v2>3.2) & (x.v2<3.4)) | (x.v1.isnull() & x.v2.isnull())]
    df = df.resample('30Min',base=0).first()
    return df