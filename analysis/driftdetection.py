# -*- coding: utf-8 -*-
"""
Created on Wed Aug 22 09:05:11 2018

@author: Brainerd Cruz
"""

import analysis.querydb as q
import volatile.memory as memory 
from datetime import timedelta as td
from datetime import datetime as dt
import numpy as np
import pandas as pd
import gsm.smsparser2.smsclass as smsclass
import dynadb.db as db


def drift_detection(acc_id="",f_time=pd.to_datetime(dt.now()-td(weeks=12))):
    accelerometers = memory.get('DF_ACCELEROMETERS')
    acc_det = accelerometers[accelerometers.accel_id==acc_id].iloc[0]
    
    try:
        df=q.get_raw_accel_data(tsm_id=acc_det.tsm_id,node_id=acc_det.node_id, 
                            accel_number=acc_det.accel_number,from_time=f_time)
    #lagpas yung node_id
    except ValueError:
        return 0
    #walang table ng tilt_***** sa db 
    except AttributeError:
        return 0
    
    #walang laman yung df
    if df.empty:
        return 0
    
    #Resample 30min
    df = df.set_index('ts').resample('30min').first()
    
    #Integer index
    N=len(df.index)
    df['i']=range(1,N+1,1)
    
    # Compute accelerometer raw value
    df.x[df.x<-2048] = df.x[df.x<-2048] + 4096
    df.y[df.y<-2048] = df.y[df.y<-2048] + 4096
    df.z[df.z<-2048] = df.z[df.z<-2048] + 4096
    
    # Compute accelerometer magnitude
    df['mag'] = ( (df.x/1024)*(df.x/1024)
                + (df.y/1024)*(df.y/1024)
                + (df.z/1024)*(df.z/1024) ).apply(np.sqrt)
    
    #count number of data    
    dfw= pd.DataFrame()    
    dfw['count']=df.mag.resample('1W').count()        
    
    # Filter data with very big/small magnitude 
    df[df.mag>3.0] = np.nan
    df[df.mag<0.5] = np.nan
    
    # Compute mean and standard deviation in time frame
    df['ave'] = df.mag.rolling(window=12,center=True).mean()
    df['stdev'] = df.mag.rolling(window=12,center=True).std()
    
    # Filter data with outlier values in time frame
    df[(df.mag>df.ave+3*df.stdev) & (df.stdev!=0)] = np.nan
    df[(df.mag<df.ave-3*df.stdev) & (df.stdev!=0)] = np.nan
    
    #interpolate missing data
    df=df.interpolate()    
    
    
    # Resample every six hours    
    df = df.resample('6H').mean()
     
    # Recompute standard deviation after resampling
    df.stdev = df.mag.rolling(window=2,center=False).std()
    df.stdev = df.stdev.shift(-1)
    df.stdev = df.stdev.rolling(window=2,center=False).mean()
    
    # Filter data with large standard deviation
    df[df.stdev>0.05] = np.nan
    
    # Compute velocity and acceleration of magnitude
    df['vel'] = df.mag - df.mag.shift(1)
    df['acc'] = df.vel - df.vel.shift(1)   
    
    #Resample 1week
    dfw['vel_week']=df.vel.resample('1W').mean()        
    dfw['acc_week']=df.acc.resample('1W').mean()
    dfw['corr'] = df.resample('1W').mag.corr(df.i) 
    dfw['corr']=dfw['corr']**2
    
       
    # Get the data that exceeds the threshold value   
    dfw = dfw[(abs(dfw['acc_week'])>0.000003) &
              (dfw['corr']>0.7) & (dfw['count']>=84)]
    
    #Compute the difference for each threshold data
    if len(dfw)>0:    
        dfw = dfw.reset_index()    
        dfw['diff_TS']=dfw.ts-dfw.ts.shift(1)
        dfw['sign']=dfw.vel_week*dfw.vel_week.shift(1)
    
    #Check if there are 4 weeks consecutive threshold data
    week=1
    days=td(days=0)
    while days<td(days=28) and week<len(dfw.index):
        if ((dfw.loc[week]['diff_TS']<=td(days=14)) & (dfw.loc[week]['sign']>0)):
            days=days+dfw.loc[week]['diff_TS']
        else:
            days=td(days=0)
        week=week+1
    
    
    if days>=td(days=28):
        print acc_id,dfw.ts[week-1]

#    df['mag'].plot()
#    plt.savefig(OutputFP+col+nids+a+"-mag")
#    plt.close()

        dft= pd.DataFrame(columns=['accel_id','ts_identified'])                
        dft.loc[0]=[acc_id,dfw.ts[week-1]]

        #save to db        
        db.df_write(smsclass.DataTable("drift_detection",dft))
        
        print("very nice!")
        
def main():
    tsm_details = memory.get('DF_TSM_SENSORS')
    accelerometers = memory.get('DF_ACCELEROMETERS')
    
    dfa=accelerometers.merge(tsm_details,how='inner', on='tsm_id')
    dfa=dfa[dfa.date_deactivated.isnull()]
    #dfa=dfa[dfa.accel_id>=1240]
    
    for i in dfa.accel_id:
        try:
            drift_detection(acc_id=i)
            print i
        except TypeError:
            pass
        
#        if (i==12):
#            print "tama na!"
#            break

if __name__ == "__main__":
    start = dt.now()
    main()
    print dt.now()-start