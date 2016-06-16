import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pandas.stats.api import ols

import cfgfileio as cfg
import rtwindow as rtw
import querySenslopeDb as q
import filterSensorData as flt

class procdata:
    def __init__ (self, colprops, vel, disp):
        self.colprops = colprops
        self.vel = vel
        self.disp = disp

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
    
def compute_node_inst_vel(xz,xy,roll_window_numpts): 

    ##DESCRIPTION:
    ##returns rounded-off values of velocity of xz and xy

    ##INPUT:
    ##xz; dataframe; horizontal linear displacements along the planes defined by xa-za
    ##xy; dataframe; horizontal linear displacements along the planes defined by xa-ya
    ##roll_window_numpts; integer; number of data points per rolling window

    ##OUTPUT:
    ##np.round(vel_xz,4), np.round(vel_xy,4)

##    uncomment to trim xz and xy for a more efficient run
#    end_xz = xz.index[-1]
#    end_xy = xy.index[-1]
#    start_xz = end_xz - timedelta(days=1)    
#    start_xy = end_xy - timedelta(days=1)
#    xz = xz.loc[start_xz:end_xz]
#    xy = xy.loc[start_xy:end_xy]    
    
    #setting up time units in days
    td=xz.index.values-xz.index.values[0]
    td=pd.Series(td/np.timedelta64(1,'D'),index=xz.index)

    #setting up dataframe for velocity values
    vel_xz=pd.DataFrame(data=None, index=xz.index[roll_window_numpts-1:])
    vel_xy=pd.DataFrame(data=None, index=xy.index[roll_window_numpts-1:])
 
    #performing moving window linear regression
    num_nodes=len(xz.columns.tolist())
    for n in range(1,1+num_nodes):
        try:
            lr_xz=ols(y=xz[n],x=td,window=roll_window_numpts,intercept=True)
            lr_xy=ols(y=xy[n],x=td,window=roll_window_numpts,intercept=True)

            vel_xz[n]=np.round(lr_xz.beta.x.values,4)
            vel_xy[n]=np.round(lr_xy.beta.x.values,4)

        except:
            print " ERROR in computing velocity" 
            vel_xz[n]=np.zeros(len(vel_xz.index))
            vel_xy[n]=np.zeros(len(vel_xy.index))

    #returning rounded-off values
    return np.round(vel_xz,4), np.round(vel_xy,4)

#def create_fill_smooth_df(df,num_nodes,monwin, roll_window_numpts, to_fill, to_smooth):
    




def genproc(col, end=datetime.now()):
    
    window = rtw.getwindow(end)
    
    monitoring = q.GetRawAccelData(col.name, window.offsetstart)
    
    NodesNoInitVal=GetNodesWithNoInitialData(monitoring,col.nos, window.offsetstart)
    
    lgdpm = pd.DataFrame()
    for node in NodesNoInitVal:
        temp = q.GetSingleLGDPM(col.name, node, window.offsetstart.strftime("%Y-%m-%d %H:%M"))
        lgdpm = lgdpm.append(temp,ignore_index=True)
    
    monitoring=monitoring.append(lgdpm)
    
    try:
        monitoring = flt.applyFilters(monitoring)
        LastGoodData=GetLastGoodData(monitoring,col.nos)
        print 'Done'		
    except:			
        print 'error'		
    
    if len(LastGoodData)<col.nos: print col.name, " Missing nodes in LastGoodData"		
    
    monitoring=monitoring.append(LastGoodData)    
    
    monitoring.loc[monitoring.ts < window.offsetstart, ['ts']] = window.offsetstart
    
    monitoring['xz'],monitoring['xy'] = accel_to_lin_xz_xy(col.seglen,monitoring.x.values,monitoring.y.values,monitoring.z.values)

    monitoring=monitoring.drop(['x','y','z'],axis=1)
    monitoring = monitoring.drop_duplicates(['ts', 'id'])
    
    monitoring=monitoring.set_index('ts')
    
    monitoring=monitoring[['id','xz','xy']]
    
    disp = monitoring    

    monitoring['fillxz'], monitoring['fillxy'] 
    
    monitoring['vel_xz'], monitoring['vel_xy'] = compute_node_inst_vel(monitoring.xz.values, monitoring.xy.values, window.numpts)
    
    return monitoring
def main():
    col=q.GetSensorList('agbta')
    monitoring = genproc(col[0])
    print monitoring

if __name__ == "__main__":
    main()