# -*- coding: utf-8 -*-
"""
Created on Thu Feb 23 10:44:42 2017

@author: Brainerd D. Cruz
"""

import pandas as pd
import numpy as np
from datetime import timedelta as td
from datetime import datetime as dt
import querySenslopeDb as qdb
import filterSensorData as fsd
import XYZrealtimeplot as xyz

def inputinfo():
    #info
    col= raw_input('Enter sensor name: ')
    nid= input('Enter node id: ')
    
    while True:
        realtime=raw_input('Real time [y/n]? ').lower()
        if realtime=='y':
            time=dt.now()
            break
        elif realtime=='n':
            time=raw_input('Enter timestamp (format: 2017-12-31 23:30): ')
            break
    
    time=pd.to_datetime(time)
    return col, nid, time

def querydf(col,nid,time):
    fromTS=time-td(weeks=1)
    try:
        #query data then filter
        df_node=qdb.GetRawAccelData(siteid=col,fromTime=fromTS,toTime=time,batt=1)    
        return df_node
    except:
        print ("Wrong Input!!!")
        col,nid,time=inputinfo()
        df_node=querydf(col,nid,time)


def process(df_node):
    dff=fsd.applyFilters(df_node)
    
    #time
    
    fromTime=time-td(days=1, hours=4)
    toTime=time-td(hours=4)
    dfr = df_node[(df_node.id==nid)&(df_node.ts>=fromTime) & (df_node.ts<=toTime)] 
    df = dff[(dff.id==nid)&(dff.ts>=fromTime) & (dff.ts<=toTime)]
    df = df.set_index('ts')  
    
    
    #Integer index
    N=len(df.index)
    df['i']=range(1,N+1,1)
    
    x_corr = (df['x'].corr(df.i))**2 
    y_corr = (df['y'].corr(df.i))**2
    z_corr = (df['z'].corr(df.i))**2
    
    stx,sty,stz=df[['x','y','z']].std()
    
    dft=df_node[(df_node.ts>=toTime)&(df_node.ts<=time)&(df_node.id==nid)]
    c= 100*dfr.x.count()/48
    cf= 100*df.x.count()/dfr.x.count()
                      
    if not dft.empty:
        df1=dft[['x','y','z']].loc[max(dft.index)]
        x,y,z=df1
        xdeg,ydeg,zdeg=np.degrees(np.arcsin(df1/1024.0))
    else:
        print ("No data!\n")
        return 1
        x,y,z=np.nan,np.nan,np.nan
        xdeg,ydeg,zdeg=np.nan,np.nan,np.nan
        
    dfft=dff[(dff.ts>=toTime)&(dff.ts<=time)&(dff.id==nid)]
    dfft=dfft[['x','y','z']].loc[max(dfft.index)]
    
    delx=dfft.x-df.x.mean()
    dely=dfft.y-df.y.mean()
    delz=dfft.z-df.z.mean()
    
    xdegdel=np.degrees(np.arcsin(dfft.x/1024))-np.degrees(np.arcsin(df.x.mean()/1024))
    ydegdel=np.degrees(np.arcsin(dfft.y/1024))-np.degrees(np.arcsin(df.y.mean()/1024))
    zdegdel=np.degrees(np.arcsin(dfft.z/1024))-np.degrees(np.arcsin(df.z.mean()/1024))
    
    
    #slope, intercept, r_value, p_value, std_err = stats.linregress(df.x,df.i)
    print("########################################################################\n")
    print(col+str(nid)+' ('+str(time)+')')
    #tag    
    query='''SELECT * FROM senslopedb.node_status
                where site='%s' and node=%d and inUse=1
                order by date_of_identification desc'''%(col,nid) 
    dfs=qdb.GetDBDataFrame(query)
    if not dfs.empty:
        print ('\nStatus:\t\t%s'%dfs.status[0])
        print ('Comment:\t%s'%dfs.comment[0])
    else:
        print ('\nStatus:\t\tOK')    
    
    print('\t\tx\ty\tz')
    print('raw data=\t\t(%d,\t%d,\t%d)' %(x,y,z))
    
    print("standard dev= \t%.2f,\t%.2f,\t%.2f" %(stx,sty,stz))
    print("correlation= \t%.2f, \t%.2f, \t%.2f" %(x_corr,y_corr,z_corr))
    print("bit delta= \t\t%.2f, \t%.2f, \t%.2f" %(delx,dely,delz))
    print('%%data sent count= \t%.0f%%' %c)
    print('%%filter/raw count= \t%.0f%%' %cf)
    
    print('data theta(deg)= \t(%.2f,\t%.2f,\t%.2f)' %(xdeg,ydeg,zdeg))
    print("delta theta(deg)= \t%.2f, \t%.2f, \t%.2f" %(xdegdel,ydegdel,zdegdel))
    
    if len(col)==5:
        batt=dft.batt.loc[max(dft.index)]
        query='''SELECT site_name,node_id,version,vmax,vmin FROM senslopedb.node_accel_table
                where site_name='%s' and node_id=%d'''%(col,nid) 
        dfb=qdb.GetDBDataFrame(query)
        print('\nBattery:')
        print('battery (min,max)= \t(%.2fV,\t%.2fV)'%(dfb.vmin[0],dfb.vmax[0]))
        print('battery voltage= \t%.2fV' %batt)
        if batt>dfb.vmax[0]:
            batt_del=batt-dfb.vmax[0]
        elif batt<dfb.vmin[0]:
            batt_del=batt-dfb.vmin[0]
        else:
            batt_del=0
        print('Delta Battery= \t\t%.2fV'%batt_del)
    
    print("\n########################################################################\n")
    
        
    xyz.xyzplot(dff,col,nid,time)


###################################################
#    try:
col, nid, time=inputinfo()
df_node=querydf(col,nid,time)
process(df_node)
#    except KeyboardInterrupt:
#        break
