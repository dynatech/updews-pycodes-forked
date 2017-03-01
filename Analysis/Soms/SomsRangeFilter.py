# -*- coding: utf-8 -*-
"""
Created on Thu Apr 07 09:29:47 2016

@author: SENSLOPEY
"""
import numpy as np
import ConvertSomsRaw as CSR
import pandas as pd
import datetime
import os
#import matplotlib.pyplot as plt
import querySenslopeDb as qDb

v2=['NAGSA', 'BAYSB', 'AGBSB', 'MCASB', 'CARSB', 'PEPSB','BLCSA']
#'absolute' minimum and maximum values for SOMS v2 and v3
m=[['RAW v2', 'RAW v3'],['CAL v2', 'CAL v3']]   #format for smin and smax



#plt.style.use('ggplot')

v2=['NAGSA', 'BAYSB', 'AGBSB', 'MCASB', 'CARSB', 'PEPSB','BLCSA']

#'absolute' minimum and maximum values for SOMS v2 and v3
m=[['RAW v2', 'RAW v3'],['CAL v2', 'CAL v3']]   #format for smin and smax



def seek_outlier(df,column, node, mode):
    smin=[[2000,500],[0,0]]                         #format: [[v2raw_min, v3raw_min], [v2calib_min,v3calib_min]]
    smax=[[7800,1600],[1700,1500]]
    outlier = []
    if column.upper() in v2:
        ver = 0
    else:
        ver = 1
        
    outlier =  ~((df > smin[mode][ver]) & (df < smax[mode][ver]))
   
    return outlier


def f_outlier(df,column,mode): 
    smin=[[2000,500],[0,0]]                         #format: [[v2raw_min, v3raw_min], [v2calib_min,v3calib_min]]
    smax=[[7800,1600],[1700,1500]]

    if column.upper() in v2:
        ver = 0
    else:
        ver = 1
            
    df= df[(df>smin[mode][ver])&(df<smax[mode][ver])]
    try:   
	   
        df= df.resample('30Min',base=0).apply(lambda x:x)
    except:
        return df
    
    return df
    
def seek_undervoltage(df,column,node,mode):
    
    if column in v2:
        v_a1= qDb.GetRawAccelData(siteid=column,targetnode=node, msgid=32, batt=1)
        v_a2= qDb.GetRawAccelData(siteid=column,targetnode=node, msgid=33, batt=1)
    else:
        v_a1= qDb.GetRawAccelData(siteid=column,targetnode=node, msgid=11, batt=1)
        v_a2= qDb.GetRawAccelData(siteid=column,targetnode=node, msgid=12, batt=1)
        
    v_a1.index = v_a1.ts
    v_a1.rename(columns={'v':'v1'}, inplace=True)
    v_a1=v_a1.resample('30Min',base=0)
    
    v_a2.index = v_a2.ts
    v_a2.rename(columns={'v':'v2'}, inplace=True)
    v_a2=v_a2.resample('30Min',base=0)
    
    x=pd.concat([df,v_a1.v1,v_a2.v2],axis=1)   
    x=x.resample('30Min',base=0)
    undervoltage =  (x.v1<3.26) | (x.v1>3.40) | (x.v2<3.26) | (x.v2>3.40)
    
    return undervoltage
    
def f_undervoltage(df,column,node,mode):
    '''for v3 only'''
#    seek_undervoltage(df,column,node,mode)
    if column in v2:
        v_a1= qDb.GetRawAccelData(siteid=column,targetnode=node, msgid=32, batt=1)
        v_a2= qDb.GetRawAccelData(siteid=column,targetnode=node, msgid=33, batt=1)
    else:
        v_a1= qDb.GetRawAccelData(siteid=column,targetnode=node, msgid=11, batt=1)
        v_a2= qDb.GetRawAccelData(siteid=column,targetnode=node, msgid=12, batt=1)        
        
    v_a1.index = v_a1.ts
    v_a1.rename(columns={'v':'v1'}, inplace=True)
    v_a1=v_a1.resample('30Min',base=0)

    v_a2.index = v_a2.ts
    v_a2.rename(columns={'v':'v2'}, inplace=True)
    v_a2=v_a2.resample('30Min',base=0)
    
    x=pd.concat([df,v_a1.v1,v_a2.v2],axis=1,ignore_index=True)
    x.columns=['mval1','v1','v2']
    x=x.resample('30Min',base=0)
    df=x.mval1[((x.v1>3.2) & (x.v1<3.4) & (x.v2>3.2) & (x.v2<3.4)) | (x.v1.isnull() & x.v2.isnull())]
    df = df.resample('30Min',base=0)
    return df
    
#column = 'gaasb'
#node = 2
#mode = 0
#fdate='2016-04-01'
#if mode==0:
#    df = CSR.getsomsrawdata(column+'m',node,fdate)
#else:
#    df = CSR.getsomscaldata(column+'m',node,fdate)
#
#f,ax=plt.subplots(4,sharex=True)
#ax[0].plot(df,color='b')
#out=seek_outlier(df,column,node,mode)
##df[out].plot(style='ro')
#
##plt.subplot(312)  
#filtered=f_outlier(df,column,node,mode)
#ax[1].plot(filtered,color='m')
##filtered.plot(color='m')
#  
#filtered2= f_undervoltage(filtered,column,node,mode)
#ax[2].plot(filtered2,color='g')
#wmean=pd.ewma(filtered2,span=48,min_periods=1)
#
#ax[3].plot(wmean,color='y')
#plt.pcolor(wmean)
#filtered2.plot(color='g')

