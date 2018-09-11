# -*- coding: utf-8 -*-
"""
Created on Thu Jun 18 14:39:48 2015

@author: senslope
"""

import numpy as np
import os
import pandas as pd
import sys

#include the path of outer folder for the python scripts searching
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
    sys.path.insert(1,path)
del path   

#import querydb as q
#
#def volt_filter(dfc):
#    #assume for a single node lang ito
#    df = dfc.copy()
##    print df
#    tsm_name = str(df.head(1).iloc[0][1])
#    n_id = int(df.head(1).iloc[0][2])
#    query = """
#    select vmax,vmin from senslopedb.node_accel_table where site_tsm_name = '%s' and node_id = %d limit 1""" %(tsm_name,n_id)
#    dfv = q.GetDBDataFrame(query)
#    vmin = dfv.head(1).iloc[0][1]
#    vmax = dfv.head(1).iloc[0][0]
#    df = df[(df.batt >= vmin) & (df.batt <= vmax)]
#    return df

def check_accel_drift(df):
    df['mag'] = np.nan
    df['ave'] = np.nan
    df['stdev'] = np.nan
    df['vel'] = np.nan
    df['acc'] = np.nan
    df['week'] = np.nan
    #df.columns = ['node_id','x','y','z','mag','ave','stdev','vel','acc','week']
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
        
def outlier_filter(df):
#    df = dff.copy()
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

def range_filter_accel(df):
#    dff = df.copy()
    ## adjust accelerometer values for valid overshoot ranges
    df.x[(df.x<-2970) & (df.x>-3072)] = df.x[(df.x<-2970) & (df.x>-3072)] + 4096
    df.y[(df.y<-2970) & (df.y>-3072)] = df.y[(df.y<-2970) & (df.y>-3072)] + 4096
    df.z[(df.z<-2970) & (df.z>-3072)] = df.z[(df.z<-2970) & (df.z>-3072)] + 4096
    
    
    df.x[abs(df.x) > 1126] = np.nan
    df.y[abs(df.y) > 1126] = np.nan
    df.z[abs(df.z) > 1126] = np.nan

    
#    return df[dfl.x.notnull()]
    return df[df.x.notnull()]
    
### Prado - Created this version to remove warnings
def range_filter_accel2(df):
    
    x_index = (df.x<-2970) & (df.x>-3072)
    y_index = (df.y<-2970) & (df.y>-3072)
    z_index = (df.z<-2970) & (df.z>-3072)
    
    ## adjust accelerometer values for valid overshoot ranges
    df.loc[x_index,'x'] = df.loc[x_index,'x'] + 4096
    df.loc[y_index,'y'] = df.loc[y_index,'y'] + 4096
    df.loc[z_index,'z'] = df.loc[z_index,'z'] + 4096
    
#    x_range = ((df.x > 1126) | (df.x < 100))
    x_range = abs(df.x) > 1126
    y_range = abs(df.y) > 1126
    z_range = abs(df.z) > 1126
    
    ## remove all invalid values
    df.loc[x_range,'x'] = np.nan
    df.loc[y_range,'y'] = np.nan
    df.loc[z_range,'z'] = np.nan
    
    return df[df.x.notnull()]
    
def orthogonal_filter(df):

    # remove all non orthogonal value
    dfo = df[['x','y','z']]/1024.0
    mag = (dfo.x*dfo.x + dfo.y*dfo.y + dfo.z*dfo.z).apply(np.sqrt)
    lim = .08
    
    return df[((mag>(1-lim)) & (mag<(1+lim)))]

def orthogonal_filter2(df):

    # remove all non orthogonal value
#    df_temp = df[['x','y','z']]
#    df["magnitude"] = (df_temp.x**2 + df_temp.y**2 +
#                       df_temp.z**2).apply(np.sqrt)/1024.0
      
    df["magnitude"]=(df[['x','y','z']]**2).sum(axis=1).apply(np.sqrt)/1024.0
    lim = .08
    
    return df[((df.magnitude>(1-lim)) & (df.magnitude<(1+lim)))]

def resample_df(df):
    df.ts = pd.to_datetime(df['ts'], unit = 's')
    df = df.set_index('ts').resample('30min').first().reset_index()
#    df = df.resample('30min').first()
#    df = df.reset_index()
    return df
    
def apply_filters(dfl, orthof=True, rangef=True, outlierf=True):

    if dfl.empty:
        return dfl[['ts','tsm_name','node_id','x','y','z']]
        
  
    if rangef:
        dfl = dfl.groupby(['node_id'])
        dfl = dfl.apply(range_filter_accel)  
        dfl = dfl.reset_index(drop=True)
        #dfl = dfl.reset_index(level=['ts'])
        if dfl.empty:
            return dfl[['ts','tsm_name','node_id','x','y','z']]

    if orthof: 
        dfl = dfl.groupby(['node_id'])
        dfl = dfl.apply(orthogonal_filter)
        dfl = dfl.reset_index(drop=True)
        if dfl.empty:
            return dfl[['ts','tsm_name','node_id','x','y','z']]
            
    
    if outlierf:
        dfl = dfl.groupby(['node_id'])
        dfl = dfl.apply(resample_df)
        dfl = dfl.set_index('ts').groupby('node_id').apply(outlier_filter)
        dfl = dfl.reset_index(level = ['ts'])
        if dfl.empty:
            return dfl[['ts','tsm_name','node_id','x','y','z']]

    
    dfl = dfl.reset_index(drop=True)     
    dfl = dfl[['ts','tsm_name','node_id','x','y','z']]
    return dfl


def apply_filters2(df, orthof=True, rangef=True, outlierf=True):

    if df.empty:
        return df[['ts','tsm_name','node_id','x','y','z']]
        
  
    if rangef:
        df = range_filter_accel(df)  
        if df.empty:
            return df[['ts','tsm_name','node_id','x','y','z']]

    if orthof: 
        df = orthogonal_filter(df)
        if df.empty:
            return df[['ts','tsm_name','node_id','x','y','z']]
            
    
    if outlierf:
        df = df.groupby(['node_id']).apply(resample_df).reset_index(drop=True)
        df = df.groupby(['node_id']).apply(outlier_filter).reset_index(drop=True)
        if df.empty:
            return df[['ts','tsm_name','node_id','x','y','z']]

    
    df = df.reset_index(drop=True)     
    df = df[['ts','tsm_name','node_id','x','y','z']]
    return df
