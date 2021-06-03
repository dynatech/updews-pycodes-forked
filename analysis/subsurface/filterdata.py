# -*- coding: utf-8 -*-
"""
Created on Thu Jun 18 14:39:48 2015

@author: senslope
"""

import numpy as np
import pandas as pd
import memcache


def volt_filter(df):
    dff = df.copy()
    if not dff.empty:
        if len(dff['tsm_name'].values[0]) == 4:
            return dff
        else:
            memc = memcache.Client(['127.0.0.1:11211'], debug=1)
            accelerometers = memc.get('DF_ACCELEROMETERS')
            dff = dff.merge(accelerometers[["accel_id","voltage_min","voltage_max"]],
							how = 'inner', on='accel_id')
            dff.batt[dff.batt >=56] = (dff.batt[dff.batt >=56]+200)/100                 #if not parsed properly
            dff = dff[(dff.batt==2) | ((dff.batt>=dff['voltage_min']) &
					   (dff.batt<=dff['voltage_max']))]
        return dff
    else:
        return dff

def outlier_filter(df):
    dff = df.copy()
#    df['ts'] = pandas.to_datetime(df['ts'], unit = 's')
#    df = df.set_index('ts')
#    df = df.resample('30min').first()
##    df = df.reset_index()
#    df = df.resample('30Min', how='first', fill_method = 'ffill')
    
    dfmean = dff[['x','y','z']].rolling(min_periods=1,window=48,center=False).mean()
    dfsd = dff[['x','y','z']].rolling(min_periods=1,window=48,center=False).std()
    #setting of limits
    dfulimits = dfmean + (3*dfsd)
    dfllimits = dfmean - (3*dfsd)

    dff.x[(dff.x > dfulimits.x) | (dff.x < dfllimits.x)] = np.nan
    dff.y[(dff.y > dfulimits.y) | (dff.y < dfllimits.y)] = np.nan
    dff.z[(dff.z > dfulimits.z) | (dff.z < dfllimits.z)] = np.nan
    
    dflogic = dff.x * dff.y * dff.z
    
    dff = dff[dflogic.notnull()]
   
    return dff

def range_filter_accel(df):
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
def range_filter_accel2(df):
    dff = df.copy()
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
    
def orthogonal_filter(df):

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
    try:
        dfl = dfl[['ts','tsm_name','node_id','x','y','z','batt']]
    except KeyError:
        dfl = dfl[['ts','tsm_name','node_id','x','y','z']]
        
    return dfl