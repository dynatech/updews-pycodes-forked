from datetime import datetime, date, time, timedelta
import pandas as pd
from pandas.stats.api import ols
import numpy as np
import matplotlib.pyplot as plt

import generic_functions as gf

import generate_proc_monitoring as genproc






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
proc_monitoring_file_headers=['ts','id','xz', 'yz', 'm']







genproc.generate_proc()



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
        #4. importing proc_monitoring csv file of current column to dataframe
        monitoring=pd.read_csv(proc_monitoring_path+colname+proc_monitoring_file,names=proc_monitoring_file_headers,parse_dates=[0],index_col=[0])

        print colname
        print monitoring.tail(20)

    except:
        print "     ",colname, "...FAILED"
    
    break
    

    
   

    #creating dataframe ts vs id

        #initializing lists
    xz_series_list=[]
    xy_series_list=[]
    m_series_list=[]

        #creating empty series with datetime index equivalent to monitoring window
    monwin_time=pd.date_range(start=offsetstart, end=end, freq='30Min',name=['blank'], closed=None)
    monwin=pd.DataFrame(data=np.nan*np.ones(len(monwin_time)), index=monwin_time)

        #appending empty series to list
    xz_series_list.append(monwin)
    xy_series_list.append(monwin)
    m_series_list.append(monwin)
    
    for n in range(1,1+num_nodes):
        #appending per node series to list
        xz_series_list.append(monitoring.loc[monitoring.id==n,['xz']])
        xy_series_list.append(monitoring.loc[monitoring.id==n,['xy']])
        m_series_list.append(monitoring.loc[monitoring.id==n,['m']])

    
    #concatenating series list into dataframe
    xzdf=pd.concat(xz_series_list, axis=1, join='outer', names=None)
    xzdf.columns=[a for a in np.arange(0,1+num_nodes)]
    xydf=pd.concat(xy_series_list, axis=1, join='outer', names=None)
    xydf.columns=[a for a in np.arange(0,1+num_nodes)]
    mdf=pd.concat(m_series_list, axis=1, join='outer', names=None)
    mdf.columns=[a for a in np.arange(0,1+num_nodes)]

    

    #removing empty series 'monwin'
    xzdf=xzdf.drop(0,1)
    xydf=xydf.drop(0,1)
    mdf=mdf.drop(0,1)

    #reordering columns
    revcols=xzdf.columns.tolist()[::-1]
    xzdf=xzdf[revcols]

   

   
   
    
    #resampling dataframes to 30-minute intervals
    r_xzdf=xzdf.resample('30Min',how='mean',base=0)
    r_xydf=xydf.resample('30Min',how='mean',base=0)
    r_mdf=mdf.resample('30Min',how='mean',base=0)
    

    #filling XZ,XY  dataframes
    fr_xzdf=r_xzdf.fillna(method='pad')
    fr_xydf=r_xydf.fillna(method='pad')
    
    fr_xzdf=fr_xzdf.fillna(method='bfill')
    fr_xydf=fr_xydf.fillna(method='bfill')
    


    #smoothing dataframes with moving average
    rm_xzdf=pd.rolling_mean(fr_xzdf,window=roll_window_size)
    rm_xydf=pd.rolling_mean(fr_xydf,window=roll_window_size)

    #computing instantaneous velocity from moving linear regression
    #setting up time units in days
    td=rm_xzdf.index.values-rm_xzdf.index.values[0]
    td=pd.Series(td/np.timedelta64(1,'D'),index=rm_xzdf.index)
    #setting up dataframe for velocity values
    vel_xzdf=pd.DataFrame(data=None, index=rm_xzdf.index[2*roll_window_size-2:])
    vel_xydf=pd.DataFrame(data=None, index=rm_xydf.index[2*roll_window_size-2:])

    #print len(rm_xzdf)

    try:
        for cur_node_ID in range(1,1+num_nodes):
            lr_xzdf=ols(y=rm_xzdf[num_nodes-cur_node_ID+1],x=td,window=roll_window_size,intercept=True, min_periods=7)
            lr_xydf=ols(y=rm_xydf[num_nodes-cur_node_ID+1],x=td,window=roll_window_size,intercept=True, min_periods=7)
            
            vel_xzdf[str(num_nodes-cur_node_ID+1)]=np.round(lr_xzdf.beta.x.values,4)
            vel_xydf[str(num_nodes-cur_node_ID+1)]=np.round(lr_xydf.beta.x.values,4)
    except:
        print len(vel_xzdf)

    rm_xzdf=rm_xzdf[(rm_xzdf.index>=start)]
    vel_xzdf=vel_xzdf[(vel_xzdf.index>=start)]
    rm_xydf=rm_xydf[(rm_xydf.index>=start)]
    vel_xydf=vel_xydf[(vel_xydf.index>=start)]

    try:
        fig,ax=plt.subplots(nrows=2,ncols=2,sharex=True,sharey=False)
        plt.sca(ax[0,0])

        tilt_offset=0.02
        vel_offset=0.05
        
        zero_plot(rm_xzdf,ax[0,0],tilt_offset)
        plt.ylabel('disp, m')
        plt.ylim(-0.1,0.1)
        plt.title(colname+' XZ')

        plt.sca(ax[0,1])
        zero_plot(rm_xydf,ax[0,1],tilt_offset)
        plt.ylabel('disp, m')
        plt.ylim(-0.1,0.1)
        plt.title(colname+' XY')
        
        plt.sca(ax[1,0])
        zero_plot(vel_xzdf,ax[1,0],vel_offset)
        plt.ylabel('vel, m/day')
        plt.ylim(-0.01,0.01)

        plt.sca(ax[1,1])
        zero_plot(vel_xydf,ax[1,1],vel_offset)
        plt.ylabel('vel, m/day')
        plt.ylim(-0.01,0.01)
    except:
        print len(vel_xzdf), len(vel_xydf)
    fig.tight_layout()
    
##    for cur_node_ID in range(1,1+num_nodes):
##        fig,ax=plt.subplots(nrows=2,ncols=1,sharex=True)
##        
##        try:
##            print vel_xzdf[:,cur_node_ID]
##            zero_plot(vel_xzdf[:,cur_node_ID],ax[1],str(cur_node_ID),'vel',[-0.06,0.06])
##            zero_plot(rm_xzdf[:,cur_node_ID],ax[0],str(cur_node_ID),'tilt',[-0.2,0.2])
##        except:
##            print rm_xzdf[cur_node_ID]#zero_plot(rm_xzdf[cur_node_ID],ax[0],str(cur_node_ID),'tilt',[-0.2,0.2])
##            
##        plt.show()
    plt.show()
    
    

    
    


    
