# -*- coding: utf-8 -*-
"""
Created on Fri Jun 10 14:06:04 2016

@author: kennex
"""

import pandas as pd
import pymysql as sql
from datetime import timedelta
#import numpy as np
import scipy.fftpack as sff
import scipy.signal as ss
from matplotlib import pyplot as plt
import matplotlib.dates as mdates

def filt(df,keep_orig=False):
    df1 = df.copy()
    if keep_orig:
        df1['xorig'] = df.x
        df1['yorig'] = df.y
        df1['zorig'] = df.z
    df1 = df1[df1.id == 1]
        
    f1 = 1.0 #target freq
    lower = 0.15
    upper = 0.35
#    f2 = 2.0
    sf = 48.0 #sampling freq
#    Rp = 1.0
#    As = 2.0
    butter_order = 5
    
    ws = [((f1-lower)/(sf*0.5)),((f1+upper)/(0.5*sf))]
    b,a = ss.butter(butter_order,ws,'bandstop')
#    df1['xfilt'] = ss.filtfilt(b,a,df1.x,axis=-1)
#    df1['yfilt'] = ss.filtfilt(b,a,df1.y,axis=-1)
#    df1['zfilt'] = ss.filtfilt(b,a,df1.z,axis=-1)
    df1['x'] = ss.filtfilt(b,a,df1.x,axis=-1)
    df1['y'] = ss.filtfilt(b,a,df1.y,axis=-1)
    df1['z'] = ss.filtfilt(b,a,df1.z,axis=-1)
    for i in [2,3,4,5,6,7,8,9,10]:
        ws = [(((f1*i)-lower)/(sf*0.5)),((f1*i)+upper)/(0.5*sf)]
        b,a = ss.butter(butter_order,ws,'bandstop')
        df1['x'] = ss.filtfilt(b,a,df1.x,axis=-1)
        df1['y'] = ss.filtfilt(b,a,df1.y,axis=-1)
        df1['z'] = ss.filtfilt(b,a,df1.z,axis=-1)
    df2 =df.copy()

    df2['xorig'] = df2.x
    df2['yorig'] = df2.y
    df2['zorig'] = df2.z
    
    df2 = df2[df2.id > 1]
#    pd.concat([df1,df2])
    return pd.concat([df1,df2])

def resample_df(df):
    df.ts = pd.to_datetime(df['ts'], unit = 's')
    df = df.set_index('ts')
    df = df.resample('30min').first().ffill()
    df = df.reset_index()
    return df

def plotter(df1,fname=''):
    ax1 = plt.subplot2grid((3,3), (0,0), rowspan=1, colspan=2)
    ax2 = plt.subplot2grid((3,3), (1,0), rowspan=1, colspan=2)
    ax3 = plt.subplot2grid((3,3), (2,0), rowspan=1, colspan=2)
    
    ax4 = plt.subplot2grid((3,3), (0,2), rowspan=1, colspan=1)
    ax5 = plt.subplot2grid((3,3), (1,2), rowspan=1, colspan=1)
    ax6 = plt.subplot2grid((3,3), (2,2), rowspan=1, colspan=1)
    
    ax1.plot(df1.ts,df1.xorig,linewidth=0.2)
    ax2.plot(df1.ts,df1.yorig,linewidth=0.2)
    ax3.plot(df1.ts,df1.zorig,linewidth=0.2)
    #ax2.plot(df1.ts,df1.y, color = 'red', linestyle='-', marker='')
    #ax3.plot(df1.ts,df1.z, color = 'green', linestyle='-', marker='')
    
    
    ax1.axes.get_xaxis().set_ticks([])    
    ax2.axes.get_xaxis().set_ticks([])
    ax4.axes.get_xaxis().set_ticks([]) 
    ax5.axes.get_xaxis().set_ticks([])
    
    ax4.axes.get_yaxis().set_ticks([]) 
    ax5.axes.get_yaxis().set_ticks([])
    ax6.axes.get_yaxis().set_ticks([]) 
    
    ax1.axes.set_ylabel('X', fontsize = 20.0,rotation='horizontal')
    ax2.axes.set_ylabel('Y', fontsize = 20.0,rotation='horizontal')
    ax3.axes.set_ylabel('Z', fontsize = 20.0,rotation='horizontal')
    
    xp1 = max(df1.x)
    #xp2 = max(df2.x)    
    xp3 = min(df1.x)
    #xp4 = min(df2.x)
    xpeak1 = min(xp1,xp3)
    xpeak2 = max(xp1,xp3)
    xheight = abs(xpeak1 - xpeak2)
    xmin = (xpeak1- (0.1*xheight))
    xmax = (xpeak2+ (0.1*xheight))
    ax1.set_ylim( xmin ,xmax) 
    
    p1 = max(df1.y)
    #p2 = max(df2.y)    
    p3 = min(df1.y)
    #p4 = min(df2.y)
    ypeak1 = min(p1,p3)
    ypeak2 = max(p1,p3)
    yheight = abs(ypeak1 - ypeak2)
    ymin = (ypeak1-(0.1*yheight))
    ymax = (ypeak2+(0.1*yheight))
    ax2.set_ylim( ymin ,ymax)
    
    p1 = max(df1.z)
    #p2 = max(df2.z)    
    p3 = min(df1.z)
    #p4 = min(df2.z)
    zpeak1 = min(p1,p3)
    zpeak2 = max(p1,p3)
    zheight = abs(zpeak1 - zpeak2)
    zmin = (zpeak1-(0.1*zheight))
    zmax = (zpeak2+(0.1*zheight))
    ax3.set_ylim( zmin ,zmax)
    
    ax3.xaxis.set_major_formatter(mdates.DateFormatter('%Y/%m/%d'))
    for label in ax3.xaxis.get_ticklabels():
        label.set_rotation(45)
        
    f_df1x = sff.fft(df1.xorig)
    f_xfilt = sff.fft(df1.x)
    
    f_df1y = sff.fft(df1.yorig)
    f_yfilt = sff.fft(df1.y)
    
    f_df1z = sff.fft(df1.zorig)
    f_zfilt = sff.fft(df1.z)
    
    fft_freq = sff.fftfreq(len(df1.xorig),d=30) #(30?)
    
    ax4.plot(fft_freq,abs(f_df1x), linestyle='-')
    ax4.plot(fft_freq,abs(f_xfilt), linestyle='-')
    ax4.set_ylim(-10,3000)
    
    ax5.plot(fft_freq,abs(f_df1y), linestyle='-')
    ax5.plot(fft_freq,abs(f_yfilt), linestyle='-')
    ax5.set_ylim(-10,3000)
    
    ax6.plot(fft_freq,abs(f_df1z), linestyle='-')
    ax6.plot(fft_freq,abs(f_zfilt), linestyle='-')
    ax6.set_ylim(-10,3000)
    
    ax1.plot(df1.ts,df1.x)
    ax2.plot(df1.ts,df1.y)
    ax3.plot(df1.ts,df1.z)
    
    plt.tight_layout()
    if (len(fname) > 1):
        plt.savefig(fname)
        plt.close()
    
def aim(target,dataframe=False):
    df_sa = pd.DataFrame.from_csv('sensor_alerts_cl.csv')
    if (target < len(df_sa)):
        site = df_sa['site_name'].iloc[target]
        t_time = df_sa.index[target]
        node = df_sa['node_id'].iloc[target]
        print "site: %s" %str(site)
        print "t_time: %s" %str(t_time)
        time_init = 15 # number of days before timestamp
        
        fromTime = pd.to_datetime(t_time) - timedelta(days=time_init)
        
        conn = sql.connect(host='localhost', port=3306, user='root', passwd='senslope', db='senslopedb')
        
        if len(site) == 5:
            query = " SELECT timestamp,id,msgid,xvalue,yvalue,zvalue FROM senslopedb.%s " %str(site)
            query = query + " WHERE id = '%d' " %node
            query = query + " AND msgid in (11,32,12,33) "
        elif len(site) == 4:
            query = " SELECT timestamp,id,xvalue,yvalue,zvalue FROM senslopedb.%s " %str(site)
            query = query + " WHERE id = '%d' " %node
            
        query = query + " AND timestamp >= '%s' "  %str(fromTime)
        query = query + " AND timestamp <= '%s' "  %str(pd.to_datetime(t_time))
        
        #print query
        #query = query + " where msgid in ('11','12','32','33') and timestamp > '2015-08-08 23:30:00' and timestamp < '2016-12-09 00:30:00'"
        df = pd.read_sql_query(query,conn)
        df1 = df.copy()
        
        if len(site) == 5:
            df1.columns = ['ts','id','msgid','x','y','z']
            df1 = df1.groupby(['msgid']).apply(resample_df)
            df1.reset_index(level=1)
            df1 = df1.groupby(['msgid']).apply(filt)
            df1.reset_index(level=1)
#            df1 = df1.groupby(['msgid']).apply(plotter)
            
        elif len(site) == 4:
            df1.columns = ['ts','id','x','y','z']
            df1 = resample_df(df1)
            df1 = filt(df1)
#            plotter(df1)
            
        if dataframe:
            df1 = df1[['ts','id','xfilt','yfilt','zfilt']]
            df1.columns = [['ts','id','x','y','z']]
            return df1
        else:
            return site,t_time
    else:
        print "Error: target is not found within sensor_alerts_cl.csv"
