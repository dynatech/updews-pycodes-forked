from datetime import datetime, date, time, timedelta
import pandas as pd
from pandas.stats.api import ols
import numpy as np
import matplotlib.pyplot as plt

import generic_functions as gf

import generate_proc_monitoring as genproc

import alert_evaluation as alert






def zero_plot(df,ax,off):
    df0=df-df.loc[(df.index==df.index[0])].values.squeeze()
    df0.plot(ax=ax,legend=False)
    






    

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
proc_monitoring_file_headers=['ts','id','xz', 'xy', 'm']


#ALERT CONSTANTS
T_disp=0.05  #m
T_velA1=0.005 #m/day
T_velA2=0.5  #m/day
k_ac_ax=0.1
num_nodes_to_check=5











#MAIN

#0. Uncomment if you want to update proc_monitoring CSVs
#genproc.generate_proc()

#1. setting date boundaries for real-time monitoring window
roll_window_numpts=int(1+roll_window_length/data_dt)
end, start, offsetstart=gf.get_rt_window(rt_window_length,roll_window_numpts,num_roll_window_ops)
#creating empty series with datetime index equivalent to monitoring window
monwin_time=pd.date_range(start=offsetstart, end=end, freq='30Min',name=['blank'], closed=None)
monwin=pd.DataFrame(data=np.nan*np.ones(len(monwin_time)), index=monwin_time)

#2. getting all column properties
sensors=pd.read_csv(columnproperties_path+columnproperties_file,names=columnproperties_headers,index_col=None)

print "Generating plots and alerts for:"
for s in range(len(sensors)):
    if s<10:continue

    #3. getting current column properties
    colname,num_nodes,seg_len=sensors['colname'][s],sensors['num_nodes'][s],sensors['seg_len'][s]

    #4. importing proc_monitoring csv file of current column to dataframe
    try:
        proc_monitoring=pd.read_csv(proc_monitoring_path+colname+proc_monitoring_file,names=proc_monitoring_file_headers,parse_dates=[0],index_col=[0])
        print "\n", colname
    except:
        print "     ",colname, "ERROR..missing/empty proc monitoring csv"

    #5. creating dataframe ts vs id

    #5a. initializing lists
    xz_series_list=[]
    xy_series_list=[]
    m_series_list=[]

    #5b.appending monitoring window dataframe to lists
    xz_series_list.append(monwin)
    xy_series_list.append(monwin)
    m_series_list.append(monwin)
   
    for n in range(1,1+num_nodes):
        #5c.creating node series        
        curxz=proc_monitoring.loc[proc_monitoring.id==n,['xz']]
        curxy=proc_monitoring.loc[proc_monitoring.id==n,['xy']]
        curm=proc_monitoring.loc[proc_monitoring.id==n,['m']]  
        #5d.resampling node series to 30-min exact intervals
        try:
            curxz=curxz.resample('30Min',how='mean',base=0)
            curxy=curxy.resample('30Min',how='mean',base=0)
            curm=curm.resample('30Min',how='mean',base=0)
        except:
            print colname, n, "ERROR missing node data"
            #zeroing tilt data if node data is missing
            curxz=pd.DataFrame(data=np.zeros(len(monwin)), index=monwin.index)
            curxy=pd.DataFrame(data=np.zeros(len(monwin)), index=monwin.index)
            curm=pd.DataFrame(data=2500*np.zeros(len(monwin)), index=monwin.index)      
        #5e. appending node series to list
        xz_series_list.append(curxz)
        xy_series_list.append(curxy)
        m_series_list.append(curm)
    
    #5f. concatenating series list into dataframe
    xzdf=pd.concat(xz_series_list, axis=1, join='outer', names=None)
    xydf=pd.concat(xy_series_list, axis=1, join='outer', names=None)
    mdf=pd.concat(m_series_list, axis=1, join='outer', names=None)

    #5g. renaming columns
    xydf.columns=[a for a in np.arange(0,1+num_nodes)]
    xzdf.columns=[a for a in np.arange(0,1+num_nodes)]
    mdf.columns=[a for a in np.arange(0,1+num_nodes)]

    #5h. dropping monwin from df
    xzdf=xzdf.drop(0,1)
    xydf=xydf.drop(0,1)
    mdf=mdf.drop(0,1)    

    #5i. reordering columns
    revcols=xzdf.columns.tolist()[::-1]
    xzdf=xzdf[revcols]
    xydf=xydf[revcols]
    mdf=mdf[revcols]


    #6.filling XZ,XY  dataframes
    fr_xzdf=xzdf.fillna(method='pad')
    fr_xydf=xydf.fillna(method='pad')   
    fr_xzdf=fr_xzdf.fillna(method='bfill')
    fr_xydf=fr_xydf.fillna(method='bfill')
    
    #7.dropping rows outside monitoring window
    fr_xzdf=fr_xzdf[(fr_xzdf.index>=monwin.index[0])&(fr_xzdf.index<=monwin.index[-1])]
    fr_xydf=fr_xydf[(fr_xydf.index>=monwin.index[0])&(fr_xydf.index<=monwin.index[-1])]
    mdf=mdf[(mdf.index>=monwin.index[0])&(mdf.index<=monwin.index[-1])]    


    #8.smoothing dataframes with moving average
    rm_xzdf=pd.rolling_mean(fr_xzdf,window=roll_window_numpts)[roll_window_numpts-1:]
    rm_xydf=pd.rolling_mean(fr_xydf,window=roll_window_numpts)[roll_window_numpts-1:]

##    #checking for finite values
##    for n in range(1,1+num_nodes):
##        print " ",n, len(np.where(np.isfinite(fr_xzdf[n].values))[0]), len(np.where(np.isfinite(rm_xzdf[n].values))[0])
  

    #9.computing instantaneous velocity from moving linear regression
    #9a. setting up time units in days
    td=rm_xzdf.index.values-rm_xzdf.index.values[0]
    td=pd.Series(td/np.timedelta64(1,'D'),index=rm_xzdf.index)
    #9b. setting up dataframe for velocity values
    vel_xzdf=pd.DataFrame(data=None, index=rm_xzdf.index[roll_window_numpts-1:])
    vel_xydf=pd.DataFrame(data=None, index=rm_xydf.index[roll_window_numpts-1:])

    for cur_node_ID in range(1,1+num_nodes):
        try:
            lr_xzdf=ols(y=rm_xzdf[num_nodes-cur_node_ID+1],x=td,window=roll_window_numpts,intercept=True)
            lr_xydf=ols(y=rm_xydf[num_nodes-cur_node_ID+1],x=td,window=roll_window_numpts,intercept=True)
        
            vel_xzdf[str(num_nodes-cur_node_ID+1)]=np.round(lr_xzdf.beta.x.values,4)
            vel_xydf[str(num_nodes-cur_node_ID+1)]=np.round(lr_xydf.beta.x.values,4)
        except:
            print colname, n, " ERROR in computing velocity" 
            vel_xzdf[str(num_nodes-cur_node_ID+1)]=np.zeros(len(vel_xzdf.index))
            vel_xydf[str(num_nodes-cur_node_ID+1)]=np.zeros(len(vel_xydf.index))
        
    #10. resizing dataframes for plotting and alert generation
    rm_xzdf=rm_xzdf[(rm_xzdf.index>=start)]
    vel_xzdf=vel_xzdf[(vel_xzdf.index>=start)]
    rm_xydf=rm_xydf[(rm_xydf.index>=start)]
    vel_xydf=vel_xydf[(vel_xydf.index>=start)]
    print len(rm_xzdf), len(rm_xydf),len(vel_xzdf), len(vel_xydf)
    

    #11.Alert generation
    alert_out=alert.node_alert(colname,rm_xzdf,rm_xydf,vel_xzdf,vel_xydf,num_nodes, T_disp, T_velA1, T_velA2, k_ac_ax)
    alert_out=alert.column_alert(alert_out, num_nodes_to_check)
    print colname
    print alert_out

    
    #12. Plotting
    try:
        fig=plt.figure(1)
        ax_xzd=fig.add_subplot(221)
        ax_xyd=fig.add_subplot(222,sharex=ax_xzd,sharey=ax_xzd)
        ax_xzv=fig.add_subplot(223,sharex=ax_xzd)
        ax_xyv=fig.add_subplot(224,sharex=ax_xzd,sharey=ax_xzv)


        tilt_offset=0.02
        vel_offset=0.05

        curax=ax_xzd
        zero_plot(rm_xzdf,curax,tilt_offset)
        curax.set_ylabel('disp, m')
        curax.set_title(colname+' XZ')

        curax=ax_xyd
        zero_plot(rm_xydf,curax,tilt_offset)
        curax.set_ylabel('disp, m')
        curax.set_title(colname+' XY')
        curax.set_ylim(-0.1,0.1)
        
        curax=ax_xzv
        zero_plot(vel_xzdf,curax,vel_offset)
        curax.set_ylabel('vel, m')
        
        curax=ax_xyv
        zero_plot(vel_xydf,curax,vel_offset)
        curax.set_ylabel('vel, m')
        curax.set_ylim(-0.01,0.01)
        
        fig.tight_layout()
        plt.show()
        
    except:
        
        print colname, "ERROR in plotting displacements and velocities"
    
    

    
    
    

    
    


    
