import pandas as pd
import numpy as np
from datetime import timedelta
from pandas.stats.api import ols

import querySenslopeDb as q
import filterSensorData as flt
import errorAnalysis as err

class procdata:
    def __init__ (self, colprops, disp, vel, max_min_df, max_min_cml):
        self.colprops = colprops
        self.vel = vel
        self.disp = disp
        self.max_min_df = max_min_df
        self.max_min_cml = max_min_cml
        
def resamplenode(df, window):
    blank_df = pd.DataFrame({'ts': [window.end,window.offsetstart], 'id': [df['id'].values[0]]*2, 'name': [df['name'].values[0]]*2}).set_index('ts')
    df = df.append(blank_df)
    df = df.reset_index().drop_duplicates(['ts','id']).set_index('ts')
    df.index = pd.to_datetime(df.index)
    df = df.sort_index(ascending = True)
    df = df.resample('30Min', base=0, how='pad')
    df = df.reset_index(level=1)
    return df    
      
def GetNodesWithNoInitialData(df,num_nodes,offsetstart):
    allnodes=np.arange(1,num_nodes+1)*1.
    with_init_val=df[df.ts<offsetstart+timedelta(hours=0.5)]['id'].values
    no_init_val=allnodes[np.in1d(allnodes, with_init_val, invert=True)]
    return no_init_val

def GetNodesWithNoData(df, num_nodes):
    allnodes = np.arange(1,num_nodes+1)
    withval = sorted(set(df.id))
    noval = allnodes[np.in1d(allnodes, withval, invert=True)]
    return noval

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

def fill_smooth (df, offsetstart, end, roll_window_numpts, to_smooth, to_fill):    
    if to_fill:
        # filling NAN values
        df = df.fillna(method = 'pad')
        
        #Checking, resolving and reporting fill process    
#        print 'Post-filling report: '
        if df.isnull().values.any():
            for n in ['xz', 'xy']:
                if df[n].isnull().values.all():
#                    print '     ',n, 'NaN all values'
                    df[n]=0
                elif np.isnan(df[n].values[0]):
#                    print '     ',n, 'NaN 1st value'
                    df[n]=df[n].fillna(method='bfill')
#        else: 
#            print '     All numerical values.'

    #dropping rows outside monitoring window
    df=df[(df.index>=offsetstart)&(df.index<=end)]
    
    if to_smooth and len(df)>1:
        df=pd.rolling_mean(df,window=roll_window_numpts,min_periods=1)[roll_window_numpts-1:]
        return np.round(df, 4)
    else:
        return df
        
def node_inst_vel(filled_smoothened, roll_window_numpts, start):
    try:          
        lr_xz=ols(y=filled_smoothened.xz,x=filled_smoothened.td,window=roll_window_numpts,intercept=True)
        lr_xy=ols(y=filled_smoothened.xy,x=filled_smoothened.td,window=roll_window_numpts,intercept=True)
                
        filled_smoothened = filled_smoothened.loc[filled_smoothened.ts >= start]
        
        filled_smoothened['vel_xz'] = np.round(lr_xz.beta.x.values[0:len(filled_smoothened)],4)
        filled_smoothened['vel_xy'] = np.round(lr_xy.beta.x.values[0:len(filled_smoothened)],4)
    
    except:
        print " ERROR in computing velocity"
        filled_smoothened['vel_xz'] = np.zeros(len(filled_smoothened))
        filled_smoothened['vel_xy'] = np.zeros(len(filled_smoothened))
    
    return filled_smoothened

def genproc(col, window, config, fixpoint, realtime=False):
    
    monitoring = q.GetRawAccelData(col.name, window.offsetstart, window.end)
    
    #identify the node ids with no data at start of monitoring window
    NodesNoInitVal=GetNodesWithNoInitialData(monitoring,col.nos,window.offsetstart)
    
    #get last good data prior to the monitoring window (LGDPM)
    lgdpm = pd.DataFrame()
    for node in NodesNoInitVal:
        temp = q.GetSingleLGDPM(col.name, node, window.offsetstart.strftime("%Y-%m-%d %H:%M"))
        lgdpm = lgdpm.append(temp,ignore_index=True)
    monitoring=monitoring.append(lgdpm)
    
    try:
        monitoring = flt.applyFilters(monitoring)
        LastGoodData = q.GetLastGoodData(monitoring,col.nos)
        q.PushLastGoodData(LastGoodData,col.name)		
        LastGoodData = q.GetLastGoodDataFromDb(col.name)
    	
    except:	
        LastGoodData = q.GetLastGoodDataFromDb(col.name)
        print 'error'		
        
    if len(LastGoodData)<col.nos: print col.name, " Missing nodes in LastGoodData"
    
    monitoring = monitoring.loc[monitoring.id <= col.nos]
    
    #assigns timestamps from LGD to be timestamp of offsetstart
    monitoring.loc[(monitoring.ts < window.offsetstart)|(pd.isnull(monitoring.ts)), ['ts']] = window.offsetstart
    
    monitoring['xz'],monitoring['xy'] = accel_to_lin_xz_xy(col.seglen,monitoring.x.values,monitoring.y.values,monitoring.z.values)
    
    monitoring = monitoring.drop(['x','y','z'],axis=1)
    monitoring = monitoring.drop_duplicates(['ts', 'id'])
    monitoring = monitoring.set_index('ts')
    monitoring = monitoring[['name','id','xz','xy']]

    nodes_noval = GetNodesWithNoData(monitoring, col.nos)
    nodes_nodata = pd.DataFrame({'name': [0]*len(nodes_noval), 'id': nodes_noval, 'xy': [np.nan]*len(nodes_noval), 'xz': [np.nan]*len(nodes_noval), 'ts': [window.offsetstart]*len(nodes_noval)})
    nodes_nodata = nodes_nodata.set_index('ts')
    monitoring = monitoring.append(nodes_nodata)
    
    max_min_df, max_min_cml = err.cml_noise_profiling(monitoring, config, fixpoint, col.nos)
        
    #resamples xz and xy values per node using forward fill
    monitoring = monitoring.groupby('id').apply(resamplenode, window = window).reset_index(level=1).set_index('ts')
    
    nodal_proc_monitoring = monitoring.groupby('id')
    
    if not realtime:
        to_smooth = config.io.to_smooth
        to_fill = config.io.to_fill
    else:
        to_smooth = config.io.rt_to_smooth
        to_fill = config.io.rt_to_fill
    
    filled_smoothened = nodal_proc_monitoring.apply(fill_smooth, offsetstart=window.offsetstart, end=window.end, roll_window_numpts=window.numpts, to_smooth=to_smooth, to_fill=to_fill)
    filled_smoothened = filled_smoothened[['xz', 'xy','name']].reset_index()
    
    monitoring = filled_smoothened.set_index('ts')   
    
    filled_smoothened['td'] = filled_smoothened.ts.values - filled_smoothened.ts.values[0]
    filled_smoothened['td'] = filled_smoothened['td'].apply(lambda x: x / np.timedelta64(1,'D'))
    
    nodal_filled_smoothened = filled_smoothened.groupby('id') 
    
    disp_vel = nodal_filled_smoothened.apply(node_inst_vel, roll_window_numpts=window.numpts, start=window.start)
    disp_vel = disp_vel[['ts', 'xz', 'xy', 'vel_xz', 'vel_xy','name']].reset_index()
    disp_vel = disp_vel[['ts', 'id', 'xz', 'xy', 'vel_xz', 'vel_xy','name']]
    disp_vel = disp_vel.set_index('ts')
    disp_vel = disp_vel.sort_values('id', ascending=True)
    
    return procdata(col,monitoring.sort(),disp_vel.sort(),max_min_df,max_min_cml)