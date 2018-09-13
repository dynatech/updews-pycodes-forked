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


def drift_detection(acc_id = "", from_time = pd.to_datetime(dt.now() -td(weeks=12))):
    accelerometers = memory.get('DF_ACCELEROMETERS')
    accel_details = accelerometers[accelerometers.accel_id == acc_id].iloc[0]
    
    try:
        df = q.get_raw_accel_data(tsm_id = accel_details.tsm_id,
                                  node_id = accel_details.node_id, 
                                  accel_number = accel_details.accel_number,
                                  from_time = from_time)
    #lagpas yung node_id
    except ValueError:
        
        return 
    #walang table ng tilt_***** sa db 
    except AttributeError:
        return 
    
    #walang laman yung df
    if df.empty:
        raise ValueError("Empty DataFrame")
#        return 0
    
    #Resample 30min
    df = df.set_index('ts').resample('30min').first()
    
    #Integer index
    N = len(df.index)
    df['i'] = range(1,N+1,1)
    
    # Compute accelerometer raw value
#    df.x[df.x<-2048] = df.x[df.x<-2048] + 4096
#    df.y[df.y<-2048] = df.y[df.y<-2048] + 4096
#    df.z[df.z<-2048] = df.z[df.z<-2048] + 4096    
    x_index = (df.x<-2970) & (df.x>-3072)
    y_index = (df.y<-2970) & (df.y>-3072)
    z_index = (df.z<-2970) & (df.z>-3072)
    
    ## adjust accelerometer values for valid overshoot ranges
    df.loc[x_index,'x'] = df.loc[x_index,'x'] + 4096
    df.loc[y_index,'y'] = df.loc[y_index,'y'] + 4096
    df.loc[z_index,'z'] = df.loc[z_index,'z'] + 4096
          
    # Compute accelerometer magnitude
    df['mag'] = (df[['x','y','z']]**2).sum(axis = 1).apply(np.sqrt) / 1024.0
    
    #count number of data    
    df_week = pd.DataFrame()    
    df_week['count'] = df.mag.resample('1W').count()        
    
    # Filter data with very big/small magnitude 
    df[df.mag>3.0] = np.nan
    df[df.mag<0.5] = np.nan
    
    # Compute mean and standard deviation in time frame
    df['ave'] = df.mag.rolling(window = 12, center = True).mean()
    df['stdev'] = df.mag.rolling(window = 12, center = True).std()
    
    # Filter data with outlier values in time frame
    df[(df.mag > df.ave + 3 * df.stdev) & (df.stdev != 0)] = np.nan
    df[(df.mag < df.ave - 3 * df.stdev) & (df.stdev != 0)] = np.nan
    
    #interpolate missing data
    df = df.interpolate()    
    
    
    # Resample every six hours    
    df = df.resample('6H').mean()
     
    # Recompute standard deviation after resampling
    df.stdev = df.mag.rolling(window = 2, center = False).std()
    df.stdev = df.stdev.shift(-1)
    df.stdev = df.stdev.rolling(window = 2,center = False).mean()
    
    # Filter data with large standard deviation
    df[df.stdev > 0.05] = np.nan
    
    # Compute velocity and acceleration of magnitude
    df['vel'] = df.mag - df.mag.shift(1)
    df['acc'] = df.vel - df.vel.shift(1)   
    
    #Resample 1week
    df_week['vel_week'] = df.vel.resample('1W').mean()        
    df_week['acc_week'] = df.acc.resample('1W').mean()
    df_week['corr'] = df.resample('1W').mag.corr(df.i) 
    df_week['corr'] = df_week['corr']**2
    
       
    # Get the data that exceeds the threshold value   
    df_week = df_week[(abs(df_week['acc_week']) > 0.000003) &
              (df_week['corr'] > 0.7) & (df_week['count'] >= 84)]
    
    #Compute the difference for each threshold data
    if len(df_week) > 0:    
        df_week = df_week.reset_index()    
        df_week['diff_TS'] = df_week.ts - df_week.ts.shift(1)
        df_week['sign'] = df_week.vel_week * df_week.vel_week.shift(1)
    
    #Check if there are 4 weeks consecutive threshold data
    week = 1
    days = td(days = 0)
    while days < td(days = 28) and week < len(df_week.index):
        if ((df_week.loc[week]['diff_TS'] <= td(days = 14)) & 
            (df_week.loc[week]['sign'] > 0)):
            days = days + df_week.loc[week]['diff_TS']
        else:
            days = td(days = 0)
        week = week + 1
    
    
    if days >= td(days = 28):
        print acc_id, df_week.ts[week - 1]

#    df['mag'].plot()
#    plt.savefig(OutputFP+col+nids+a+"-mag")
#    plt.close()

        df_drift = pd.DataFrame(columns = ['accel_id', 'ts_identified'])                
        df_drift.loc[0] = [acc_id, df_week.ts[week - 1]]
        
        try:
            #save to db        
            db.df_write(smsclass.DataTable("drift_detection", df_drift))
            print "Successfully written to DB"
        except TypeError:
            raise ValueError("Connection Fail")
        
def main():
    tsm_sensors = memory.get('DF_TSM_SENSORS')
    accelerometers = memory.get('DF_ACCELEROMETERS')
    
    dfa = accelerometers.merge(tsm_sensors,how='inner', on='tsm_id')
    dfa = dfa[dfa.date_deactivated.isnull()]
    #dfa=dfa[dfa.accel_id>=1240]
    
    for i in dfa.accel_id:
        try:
            drift_detection(acc_id = i)
            print i
#        except TypeError:
#            pass
        except ValueError:
            pass
        
#        if (i==12):
#            print "tama na!"
#            break

if __name__ == "__main__":
    start = dt.now()
    main()
    print dt.now() - start