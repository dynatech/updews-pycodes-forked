from datetime import datetime, date, time, timedelta
import pandas as pd
from pandas.stats.api import ols
import numpy as np
import matplotlib.pyplot as plt

import generic_functions as gf






def zero_plot(df,ax,off):
    df0=df-df.loc[(df.index==df.index[0])].values.squeeze()
    df0.plot(ax=ax,marker='.',legend=False)
    






    

##set/get values from config file

#time interval between data points, in hours
data_dt=0.5

#length of real-time monitoring window, in days
rt_window_length=3.

#length of rolling/moving window operations in hours
roll_window_length=3.

#number of rolling window operations in the whole monitoring analysis
num_roll_window_ops=2

#INPUT/OUTPUT FILES

#local file paths
columnproperties_path='/home/dynaslope-l5a/SVN/Dynaslope/updews-pycodes/Stable Versions/'
purged_path='/home/dynaslope-l5a/Dropbox/Senslope Data/Purged/New/'
monitoring_path='/home/dynaslope-l5a/Dropbox/Senslope Data/Purged/Monitoring/'
LastGoodData_path='/home/dynaslope-l5a/Dropbox/Senslope Data/Purged/LastGoodData/'
proc_monitoring_path='/home/dynaslope-l5a/Dropbox/Senslope Data/Proc2/Monitoring/'

#file names
columnproperties_file='column_properties.csv'
purged_file='.csv'
monitoring_file='.csv'
LastGoodData_file='.csv'
proc_monitoring_file='.csv'

#file headers
columnproperties_headers=['colname','num_nodes','seg_len']
purged_file_headers=['ts','id','x', 'y', 'z', 'm']
monitoring_file_headers=['ts','id','x', 'y', 'z', 'm']
LastGoodData_file_headers=['ts','id','x', 'y', 'z', 'm']
proc_monitoring_file_headers=['ts','id','x', 'y', 'z', 'm']











#MAIN

#1. setting date boundaries for real-time monitoring window
roll_window_numpts=int(1+roll_window_length/data_dt)
end, start, offsetstart=gf.get_rt_window(rt_window_length,roll_window_numpts,num_roll_window_ops)

#2. getting all column properties
sensors=pd.read_csv(columnproperties_path+columnproperties_file,names=columnproperties_headers,index_col=None)

print "Generating PROC monitoring data for:"
for s in range(len(sensors)):
    
    
    #3. getting current column properties
    colname,num_nodes,seg_len=sensors['colname'][s],sensors['num_nodes'][s],sensors['seg_len'][s]
    
##    print "\nDATA for ",colname," as of ", end.strftime("%Y-%m-%d %H:%M")

    try:
        #4. importing monitoring csv file of current column to dataframe
        monitoring=pd.read_csv(monitoring_path+colname+monitoring_file,names=monitoring_file_headers,parse_dates=[0],index_col=[1])

        #5. extracting data within monitoring window
        monitoring=monitoring[(monitoring.ts>=offsetstart)&(monitoring.ts<=end)]
        
        #6. importing LastGoodData csv file of current column to dataframe
        LastGoodData=pd.read_csv(LastGoodData_path+colname+LastGoodData_file,names=LastGoodData_file_headers,parse_dates=[0],index_col=[1])

        #7. extracting last data outside monitoring window
        LastGoodData=LastGoodData[(LastGoodData.ts<offsetstart)]

        #8. appending LastGoodData to monitoring
        monitoring=monitoring.append(LastGoodData)
        
        #9. replacing date of data outside monitoring window with first date of monitoring window
        monitoring.loc[monitoring.ts < offsetstart, ['ts']] = offsetstart

        #10. computing corresponding horizontal linear displacements (xz,xy), and appending as columns to dataframe
        monitoring['xz'],monitoring['xy']=gf.accel_to_lin_xz_xy(seg_len,monitoring.x.values,monitoring.y.values,monitoring.z.values)
        
        #11. removing unnecessary columns x,y,z
        monitoring=monitoring.drop(['x','y','z'],axis=1)

        #12. setting ts as index
        monitoring['id']=monitoring.index.values
        monitoring=monitoring.set_index('ts')

        #13. reordering columns
        monitoring=monitoring[['id','xz','xy','m']]

        
    ##    print "\n",colname
    ##    print monitoring.tail(20)

        monitoring.to_csv(proc_monitoring_path+colname+proc_monitoring_file,sep=',', header=False,mode='w')

        print "     ",colname

    except:
        #ERROR ENCOUNTERED
        print "     ",colname, "...FAILED"
    
    
    
    


    
