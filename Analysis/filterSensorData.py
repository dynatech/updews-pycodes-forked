# -*- coding: utf-8 -*-
"""
Created on Thu Jun 18 14:39:48 2015

@author: senslope
"""

import pandas as pd
import numpy as np
#import time
#from datetime import timedelta as td
#from datetime import datetime as dt
#import sqlalchemy
#from sqlalchemy import create_engine
#import sys
#import ConfigParser
import querySenslopeDb as qdb

def volt_filter(dfc):
    #assume for a single node lang ito
    df = dfc.copy()
#    print df
    name = str(df.head(1).iloc[0][1])
    n_id = int(df.head(1).iloc[0][2])
    query = """
    select vmax,vmin from senslopedb.node_accel_table where site_name = '%s' and node_id = %d limit 1""" %(name,n_id)
    dfv = qdb.GetDBDataFrame(query)
    vmin = dfv.head(1).iloc[0][1]
    vmax = dfv.head(1).iloc[0][0]
    df = df[(df.batt >= vmin) & (df.batt <= vmax)]
    return df

def checkAccelDrift(df):
    df['mag'] = np.nan
    df['ave'] = np.nan
    df['stdev'] = np.nan
    df['vel'] = np.nan
    df['acc'] = np.nan
    df['week'] = np.nan
    #df.columns = ['id','x','y','z','mag','ave','stdev','vel','acc','week']
#    df.set_index('ts')
    
    # Compute accelerometer raw value
    df.x[df.x<-2048] = df.x[df.x<-2048] + 4096
    df.y[df.y<-2048] = df.y[df.y<-2048] + 4096
    df.z[df.z<-2048] = df.z[df.z<-2048] + 4096
    
    # Compute accelerometer magnitude
    df.mag = (((df.x/1024)*(df.x/1024) + (df.y/1024)*(df.y/1024) + (df.z/1024)*(df.z/1024) ).apply(np.sqrt))
    
    # Filter data with very big/small magnitude (change from 5 to 1.5)
    df[df.mag>3.0] = np.nan
    df[df.mag<0.5] = np.nan
    
    # Compute mean and standard deviation in time frame
    df.ave = pd.stats.moments.rolling_mean(df.mag, 12, min_periods=None, freq=None, center=False)
    df.stdev = pd.stats.moments.rolling_std(df.mag, 12, min_periods=None, freq=None, center=False)
    
    # Adjust index to represent mid data
    df.ave = df.ave.shift(-6)
    df.stdev = df.stdev.shift(-6)
    
    # Filter data with outlier values in time frame
    df[(df.mag>df.ave+3*df.stdev) & (df.stdev!=0)] = np.nan
    df[(df.mag<df.ave-3*df.stdev) & (df.stdev!=0)] = np.nan
    
    # Resample every six hours
    df = df.resample('6H')
    
    # Recompute standard deviation after resampling
    df.stdev = pd.stats.moments.rolling_std(df.mag, 2, min_periods=None, freq=None, center=False)
    df.stdev = df.stdev.shift(-1)
    df.stdev = pd.stats.moments.rolling_mean(df.stdev, 2, min_periods=None, freq=None, center=False)
    
    # Filter data with large standard deviation
    df[df.stdev>0.05] = np.nan
    
    # Compute velocity and acceleration of magnitude
    df.vel = df.mag - df.mag.shift(1)
    df.acc = df.vel - df.vel.shift(1)
    
    # Compute rolling sum 
    df.week = pd.stats.moments.rolling_mean(df.acc, 28, min_periods=None, freq=None, center=False)

    try:    
        # Print node data that exceed threshold
        dft = df[(abs(df.week)>0.000035) & ((df.mag>1.1) | (df.mag<0.9))]
        dft = dft.reset_index(level='ts')
#        print dft.reset_index(level='ts')
        return dft.min()
    except IndexError:
        return
        
def outlierFilter(dff):
    df = dff.copy()
#    df['ts'] = pandas.to_datetime(df['ts'], unit = 's')
#    df = df.set_index('ts')
#    df = df.resample('30min').first()
##    df = df.reset_index()
#    df = df.resample('30Min', how='first', fill_method = 'ffill')
    
#    dfmean = pd.stats.moments.rolling_mean(df[['x','y','z']],48, min_periods=1, freq=None, center=False)
    dfmean = df[['x','y','z']].rolling(min_periods=1,window=48,center=False).mean()
#    dfsd = pd.stats.moments.rolling_std(df[['x','y','z']],48, min_periods=1, freq=None, center=False)
    dfsd = df[['x','y','z']].rolling(min_periods=1,window=48,center=False).std()
    #setting of limits
    dfulimits = dfmean + (3*dfsd)
    dfllimits = dfmean - (3*dfsd)

    df.x[(df.x > dfulimits.x) | (df.x < dfllimits.x)] = np.nan
    df.y[(df.y > dfulimits.y) | (df.y < dfllimits.y)] = np.nan
    df.z[(df.z > dfulimits.z) | (df.z < dfllimits.z)] = np.nan
    
    dflogic = df.x * df.y * df.z
    
    df = df[dflogic.notnull()]
   
    return df

def rangeFilterAccel(df):
    dff = df.copy()
    ## adjust accelerometer values for valid overshoot ranges
    dff.x[(dff.x<-2970) & (dff.x>-3072)] = dff.x[(dff.x<-2970) & (dff.x>-3072)] + 4096
    dff.y[(dff.y<-2970) & (dff.y>-3072)] = dff.y[(dff.y<-2970) & (dff.y>-3072)] + 4096
    dff.z[(dff.z<-2970) & (dff.z>-3072)] = dff.z[(dff.z<-2970) & (dff.z>-3072)] + 4096
    
    
    dff.x[abs(dff.x) > 1126] = np.nan
    dff.y[abs(dff.y) > 1126] = np.nan
    dff.z[abs(dff.z) > 1126] = np.nan

    
#    return dff[dfl.x.notnull()]
    return dff[dff.x.notnull()]
    
### Prado - Created this version to remove warnings
def rangeFilterAccel2(dff):
    
    x_index = (dff.x<-2970) & (dff.x>-3072)
    y_index = (dff.y<-2970) & (dff.y>-3072)
    z_index = (dff.z<-2970) & (dff.z>-3072)
    
    ## adjust accelerometer values for valid overshoot ranges
    dff.loc[x_index,'x'] = dff.loc[x_index,'x'] + 4096
    dff.loc[y_index,'y'] = dff.loc[y_index,'y'] + 4096
    dff.loc[z_index,'z'] = dff.loc[z_index,'z'] + 4096
    
#    x_range = ((dff.x > 1126) | (dff.x < 100))
    x_range = abs(dff.x) > 1126
    y_range = abs(dff.y) > 1126
    z_range = abs(dff.z) > 1126
    
    ## remove all invalid values
    dff.loc[x_range,'x'] = np.nan
    dff.loc[y_range,'y'] = np.nan
    dff.loc[z_range,'z'] = np.nan
    
    return dff[dff.x.notnull()]
    
def orthogonalFilter(df):

    # remove all non orthogonal value
    dfo = df[['x','y','z']]/1024.0
    mag = (dfo.x*dfo.x + dfo.y*dfo.y + dfo.z*dfo.z).apply(np.sqrt)
    lim = .08
    
    return df[((mag>(1-lim)) & (mag<(1+lim)))]

def resample_df(df):
    df.ts = pd.to_datetime(df['ts'], unit = 's')
    df = df.set_index('ts')
    df = df.resample('30min').first()
    df = df.reset_index()
    return df
    
def applyFilters(dfl, orthof=True, rangef=True, outlierf=True):

    if dfl.empty:
        return dfl[['ts','name','id','x','y','z']]
        
  
    if rangef:
        dfl = dfl.groupby(['id'])
        dfl = dfl.apply(rangeFilterAccel)  
        dfl = dfl.reset_index(drop=True)
        dfl = dfl.reset_index(level=['ts'])
        if dfl.empty:
            return dfl[['ts','name','id','x','y','z']]

    if orthof: 
        dfl = dfl.groupby(['id'])
        dfl = dfl.apply(orthogonalFilter)
        dfl = dfl.reset_index(drop=True)
        if dfl.empty:
            return dfl[['ts','name','id','x','y','z']]
            
    
    if outlierf:
        dfl = dfl.groupby(['id'])
        dfl = dfl.apply(resample_df)
        dfl = dfl.set_index('ts').groupby('id').apply(outlierFilter)
        dfl = dfl.reset_index(level = ['ts'])
        if dfl.empty:
            return dfl[['ts','name','id','x','y','z']]

    
    dfl = dfl.reset_index(drop=True)     
    dfl = dfl[['ts','name','id','x','y','z']]
    return dfl