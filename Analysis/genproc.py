import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pandas.stats.api import ols

import cfgfileio as cfg
import rtwindow as rtw
import querySenslopeDb as q
import filterSensorData as flt

class procdata:
    def __init__ (self, colprops, disp, vel):
        self.colprops = colprops
        self.vel = vel
        self.disp = disp
        
def resamplenode(df):
    df = df.resample('30T').ffill()
    df = df.reset_index(level=1).set_index('ts')
    return df    
    
    
def GetNodesWithNoInitialData(df,num_nodes,offsetstart):
    allnodes=np.arange(1,num_nodes+1)*1.
    with_init_val=df[df.ts<offsetstart+timedelta(hours=0.5)]['id'].values
    no_init_val=allnodes[np.in1d(allnodes, with_init_val, invert=True)]
    return no_init_val

def accel_to_lin_xz_xy(seg_len,xa,ya,za):

    #DESCRIPTION
    #converts accelerometer data (xa,ya,za) to corresponding tilt expressed as horizontal linear displacements values, (xz, xy)
    
    #INPUTS
    #seg_len; float; length of individual column segment
    #xa,ya,za; array of integers; accelerometer data (ideally, -1024 to 1024)
    
    #OUTPUTS
    #xz, xy; array of floats; horizontal linear displacements along the planes defined by xa-za and xa-ya, respectively; units similar to seg_len
    

    x=seg_len/np.sqrt(1+(np.tan(np.arctan(za/(np.sqrt(xa**2+ya**2))))**2+(np.tan(np.arctan(ya/(np.sqrt(xa**2+za**2))))**2)))
    xz=x*(za/(np.sqrt(xa**2+ya**2)))
    xy=x*(ya/(np.sqrt(xa**2+za**2)))
    
    return np.round(xz,4),np.round(xy,4)
    
#def fillsmooth (df, offsetstart, end, roll_window_numpts, to_smooth):
#
#    df=df.fillna(method='pad')
#    
#    df = df[(df.index>=offsetstart)&(df.index<=end)]
#    
#    if to_smooth:
#        df=pd.rolling_mean(df,window=roll_window_numpts,min_periods=1)
#        
#    return np.round(df, 4)
    

#def fill_smooth_df(proc_monitoring, offsetstart, end, roll_window_numpts, to_smooth):
#
#    ##DESCRIPTION:
#    ##returns filled and smoothened xz and xy within monitoring window
#
#    ##INPUT:
#    ##proc_monitoring; dataframe; index: ts, columns: [id, xz, xy]
#    ##num_dodes; integer; number of nodes
#    ##monwin; monitoring window dataframe
#    ##roll_window_numpts; integer; number of data points per rolling window
#    ##to_fill; filling NAN values
#    ##to_smooth; smoothing dataframes with moving average
#
#    ##OUTPUT:
#    ##proc_monitoring; dataframe; index: ts, columns: [id, filled and smoothened (fs) xz, fs xy]
#
#    #filling NAN values
#    try:
#        proc_monitoring = proc_monitoring.reset_index(level=1)
#        NodesWithVal = list(set(proc_monitoring.dropna().id.values))
#        blank_df = pd.DataFrame({'ts': [end]*len(NodesWithVal), 'id': NodesWithVal}).set_index('ts')
#        proc_monitoring = proc_monitoring.append(blank_df)
#        proc_monitoring = proc_monitoring.reset_index().drop_duplicates(['ts','id']).set_index('ts')
#        proc_monitoring.index = pd.to_datetime(proc_monitoring.index)
#        proc_monitoring = proc_monitoring.resample('30Min', base=0, how='ffill')
#        proc_monitoring = proc_monitoring.fillna(method='pad')
#        proc_monitoring = proc_monitoring.fillna(method='bfill')
#     
#        #dropping rows outside monitoring window
#        proc_monitoring = proc_monitoring[(proc_monitoring.index>=offsetstart)&(proc_monitoring.index<=end)]
#        
#        if to_smooth:
#            #smoothing dataframes with moving average
#            proc_monitoring=pd.rolling_mean(proc_monitoring,window=roll_window_numpts,min_periods=1)[roll_window_numpts-1:]
#    except:
#        pass
#
#    return np.round(proc_monitoring, 4)
    
def smooth (df, offsetstart, end, roll_window_numpts, to_smooth):
    if to_smooth and len(df)>1:
        df=pd.rolling_mean(df,window=roll_window_numpts,min_periods=1)[roll_window_numpts-1:]
        return np.round(df, 4)
    else:
        return df
def node_inst_vel(filled_smoothened, roll_window_numpts, start):

    try:          
        lr_xz=ols(y=filled_smoothened.xz,x=filled_smoothened.td,window=roll_window_numpts,intercept=True)
        lr_xy=ols(y=filled_smoothened.xy,x=filled_smoothened.td,window=roll_window_numpts,intercept=True)
                
        filled_smoothened = filled_smoothened.loc[filled_smoothened.ts >= (start-timedelta(hours=0.5))]
                   
        filled_smoothened['vel_xz'] = np.round(lr_xz.beta.x.values,4)
        filled_smoothened['vel_xy'] = np.round(lr_xy.beta.x.values,4)
    
    except:
        print " ERROR in computing velocity"
        filled_smoothened['vel_xz'] = np.zeros(len(filled_smoothened))
        filled_smoothened['vel_xy'] = np.zeros(len(filled_smoothened))
    
    return filled_smoothened

def genproc(col, end=datetime.now()):
    
    window,config = rtw.getwindow()
    
    monitoring = q.GetRawAccelData(col.name, window.offsetstart)
    
    try:
        monitoring = flt.applyFilters(monitoring)
        LastGoodData = q.GetLastGoodData(monitoring,col.nos)
        q.PushLastGoodData(LastGoodData,col.name)		
        LastGoodData = q.GetLastGoodDataFromDb(col.name)
    	
    except:	
        LastGoodData = q.GetLastGoodDataFromDb(col.name)
        print 'error'		
        
    if len(LastGoodData)<col.nos: print col.name, " Missing nodes in LastGoodData"
    
    monitoring = monitoring.append(LastGoodData)
    
    #assigns timestamps from LGD to be timestamp of offsetstart
    monitoring.loc[monitoring.ts < window.offsetstart, ['ts']] = window.offsetstart
    
    monitoring['xz'],monitoring['xy'] = accel_to_lin_xz_xy(col.seglen,monitoring.x.values,monitoring.y.values,monitoring.z.values)
    
    
    monitoring = monitoring.drop(['x','y','z'],axis=1)
    monitoring = monitoring.drop_duplicates(['ts', 'id'])
    monitoring = monitoring.set_index('ts')
    monitoring = monitoring[['name','id','xz','xy']]
    
    #resamples xz and xy values per node using forward fill
    monitoring = monitoring.groupby('id').apply(resamplenode).reset_index(level=1).set_index('ts')
    
    nodal_proc_monitoring = monitoring.groupby('id')
    
    filled_smoothened = nodal_proc_monitoring.apply(smooth, offsetstart=window.offsetstart, end=window.end, roll_window_numpts=window.numpts, to_smooth=config.io.to_smooth)
    filled_smoothened = filled_smoothened[['xz', 'xy','name']].reset_index()
    
    monitoring = filled_smoothened.set_index('ts')   
    
    filled_smoothened['td'] = filled_smoothened.ts.values - filled_smoothened.ts.values[0]
    filled_smoothened['td'] = filled_smoothened['td'].apply(lambda x: x / np.timedelta64(1,'D'))
    #
    nodal_filled_smoothened = filled_smoothened.groupby('id') 
    
    asd = nodal_filled_smoothened.get_group
    
    disp_vel = nodal_filled_smoothened.apply(node_inst_vel, roll_window_numpts=window.numpts, start=window.start)
    disp_vel = disp_vel[['ts', 'xz', 'xy', 'vel_xz', 'vel_xy','name']].reset_index()
    disp_vel = disp_vel[['ts', 'id', 'xz', 'xy', 'vel_xz', 'vel_xy','name']]
    disp_vel = disp_vel.set_index('ts')
    disp_vel = disp_vel.sort_values('id', ascending=True)
    
    
    print disp_vel.sort()
    
    return procdata(col,monitoring.sort(),disp_vel.sort())

#def main():
#    col=q.GetSensorList('agbta')
#    monitoring = genproc(col[0])
#    print monitoring.veldisp
#
#if __name__ == "__main__":
#    main()
