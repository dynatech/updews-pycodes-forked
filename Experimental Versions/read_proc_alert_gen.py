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
##    num_nodes=len(df0)
##
##    try:
##        for n in range(1,1+num_nodes):
##            
##            
##            df0[str(n)]=df0[str(n)].values+(num_nodes-n+1)*off
##        print df0
##
##        print "OFFSET OK"
##        print df0.plot(ax=ax,legend=False)
##        
##    except:
    df0.plot(ax=ax,legend=False)

def create_series_list(input_df,monwin,colname,num_nodes):
    #a. initializing lists
    xz_series_list=[]
    xy_series_list=[]
    m_series_list=[]

    #b.appending monitoring window dataframe to lists
    xz_series_list.append(monwin)
    xy_series_list.append(monwin)
    m_series_list.append(monwin)
   
    for n in range(1,1+num_nodes):
        #c.creating node series        
        curxz=input_df.loc[input_df.id==n,['xz']]
        curxy=input_df.loc[input_df.id==n,['xy']]
        curm=input_df.loc[input_df.id==n,['m']]  
        #d.resampling node series to 30-min exact intervals
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

    return xz_series_list,xy_series_list,m_series_list



def create_fill_smooth_df(series_list,num_nodes,monwin, roll_window_numpts):
    
    #concatenating series list into dataframe
    df=pd.concat(series_list, axis=1, join='outer', names=None)

    #renaming columns
    df.columns=[a for a in np.arange(0,1+num_nodes)]

    #dropping column "monwin" from df
    df=df.drop(0,1)

    #reordering columns
    revcols=df.columns.tolist()[::-1]
    df=df[revcols]

    #filling NAN values
    df=df.fillna(method='pad')
    df=df.fillna(method='bfill')

    #dropping rows outside monitoring window
    df=df[(df.index>=monwin.index[0])&(df.index<=monwin.index[-1])]

    #smoothing dataframes with moving average
    df=pd.rolling_mean(df,window=roll_window_numpts)[roll_window_numpts-1:]

    return df
    
def compute_col_pos(xz,xy,col_pos_end, col_pos_interval, col_pos_number):

    #computing x from xz and xy
    x=pd.DataFrame(data=None,index=xz.index)
    num_nodes=len(xz.columns.tolist())
    for n in np.arange(1,1+num_nodes):
        cur_xz=xz.loc[:,num_nodes-n+1]
        cur_xy=xy.loc[:,num_nodes-n+1]
        x[num_nodes-n+1]=gf.x_from_xzxy(seg_len, cur_xz.values, cur_xy.values)

    #getting dates for column positions
    colposdates=pd.date_range(end=col_pos_end, freq=col_pos_interval,periods=col_pos_number, name='ts',closed=None)

    #getting cumulative displacements
    cs_x=pd.DataFrame()
    cs_xz=pd.DataFrame()
    cs_xy=pd.DataFrame()
    for i in colposdates:
        cs_x=cs_x.append(x[(x.index==i)].cumsum(axis=1),ignore_index=True)
        cs_xz=cs_xz.append(xz[(xz.index==i)].cumsum(axis=1),ignore_index=True)
        cs_xy=cs_xy.append(xy[(xy.index==i)].cumsum(axis=1),ignore_index=True)
    cs_x=cs_x.set_index(colposdates)
    cs_xz=cs_xz.set_index(colposdates)
    cs_xy=cs_xy.set_index(colposdates)
    cs_x[num_nodes+1]=0
    cs_xz[num_nodes+1]=0
    cs_xy[num_nodes+1]=0

    #print cs_x.columns.tolist()
    cols=np.asarray(cs_x.columns.tolist())
    sortcols=np.sort(cols)[::-1]

    cs_x=cs_x[sortcols]
    cs_xz=cs_xz[sortcols]
    cs_xy=cs_xy[sortcols]

    return cs_x, cs_xz, cs_xy

def compute_node_inst_vel(xz,xy,roll_window_numpts): 
    #setting up time units in days
    td=xz.index.values-xz.index.values[0]
    td=pd.Series(td/np.timedelta64(1,'D'),index=xz.index)

    #setting up dataframe for velocity values
    vel_xz=pd.DataFrame(data=None, index=xz.index[roll_window_numpts-1:])
    vel_xy=pd.DataFrame(data=None, index=xy.index[roll_window_numpts-1:])

    #performing moving window linear regression
    num_nodes=len(xz.columns.tolist())
    for cur_node_ID in range(1,1+num_nodes):
        try:
            lr_xz=ols(y=xz[num_nodes-cur_node_ID+1],x=td,window=roll_window_numpts,intercept=True)
            lr_xy=ols(y=xy[num_nodes-cur_node_ID+1],x=td,window=roll_window_numpts,intercept=True)
        
            vel_xz[num_nodes-cur_node_ID+1]=np.round(lr_xz.beta.x.values,4)
            vel_xy[num_nodes-cur_node_ID+1]=np.round(lr_xy.beta.x.values,4)
        except:
            print " ERROR in computing velocity" 
            vel_xz[num_nodes-cur_node_ID+1]=np.zeros(len(vel_xz.index))
            vel_xy[num_nodes-cur_node_ID+1]=np.zeros(len(vel_xy.index))

    return vel_xz, vel_xy





    

##set/get values from config file

#time interval between data points, in hours
data_dt=0.5

#length of real-time monitoring window, in days
rt_window_length=3.

#length of rolling/moving window operations in hours
roll_window_length=3.

#number of rolling window operations in the whole monitoring analysis
num_roll_window_ops=2

#number of column positions to plot
col_pos_num=4

#time interval between adjacent column position dates
col_pos_interval='1D'


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
genproc.generate_proc()

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

    #3. getting current column properties
    colname,num_nodes,seg_len=sensors['colname'][s],sensors['num_nodes'][s],sensors['seg_len'][s]

    #4. importing proc_monitoring csv file of current column to dataframe
    try:
        proc_monitoring=pd.read_csv(proc_monitoring_path+colname+proc_monitoring_file,names=proc_monitoring_file_headers,parse_dates=[0],index_col=[0])
        print "\n", colname
    except:
        print "     ",colname, "ERROR...missing/empty proc monitoring csv"
        continue

    #5. creating series lists per node
    xz_series_list,xy_series_list,m_series_list = create_series_list(proc_monitoring,monwin,colname,num_nodes)
    
    #6. create, fill and smooth dataframes from series lists
    xz=create_fill_smooth_df(xz_series_list,num_nodes,monwin, roll_window_numpts)
    xy=create_fill_smooth_df(xy_series_list,num_nodes,monwin, roll_window_numpts)
    m=create_fill_smooth_df(m_series_list,num_nodes,monwin, roll_window_numpts)

    #7. computing instantaneous velocity 
    vel_xz, vel_xy = compute_node_inst_vel(xz,xy,roll_window_numpts)
        
    #8. computing cumulative displacements
    cs_x, cs_xz, cs_xy=compute_col_pos(xz,xy,monwin.index[-1], col_pos_interval, col_pos_num)

    #9. processing dataframes for output
    df_list=[xz,xy,vel_xz,vel_xy,cs_x,cs_xz,cs_xy]
    df_name_list=['xz','xy','xz_vel','xy_vel','x_cs','xz_cs','xy_cs']
    for d in range(len(df_list)):
        #resizing and rounding dataframes
        df_list[d]=np.round(df_list[d],4)[(df_list[d].index>=start)]
        #writing to csv
        df=df_list[d]
        df.to_csv(proc_monitoring_path+"/csv_to_plot/"+colname+"_"+df_name_list[d]+proc_monitoring_file,
                  sep=',', header=False,mode='w')

    #10. Alert generation
    #processing node-level alerts
    alert_out=alert.node_alert(colname,xz,xy,vel_xz,vel_xy,num_nodes, T_disp, T_velA1, T_velA2, k_ac_ax)
    #processing column-level alerts
    alert_out=alert.column_alert(alert_out, num_nodes_to_check)
    #adding 'ts' and setting it as index
    alert_out['ts']=end
    alert_out=alert_out.set_index(['ts','node_ID'])
    #writing to csv
    alert_out.to_csv(proc_monitoring_path+"/alerts/"+colname+proc_monitoring_file,
                  sep=',', header=False,mode='a')





    #11. Plotting column positions
    try:
        fig=plt.figure(1)
        plt.suptitle(colname+" absolute position")
        ax_xz=fig.add_subplot(121)
        ax_xy=fig.add_subplot(122,sharex=ax_xz,sharey=ax_xz)

        for i in cs_x.index:
            curcolpos_x=cs_x[(cs_x.index==i)].values

            curax=ax_xz
            curcolpos_xz=cs_xz[(cs_xz.index==i)].values
            curax.plot(curcolpos_xz[0],curcolpos_x[0],'.-')
            curax.set_xlabel('xz')
            curax.set_ylabel('x')

            curax=ax_xy
            curcolpos_xy=cs_xy[(cs_xy.index==i)].values
            curax.plot(curcolpos_xy[0],curcolpos_x[0],'.-', label=i)
            curax.set_xlabel('xy')

        fig.tight_layout()
        plt.legend(fontsize='x-small')
    
    except:        
        print colname, "ERROR in plotting column position"

    
    #11. Plotting displacement and velocity
    try:
        fig=plt.figure(2)
        ax_xzd=fig.add_subplot(141)
        ax_xyd=fig.add_subplot(143,sharex=ax_xzd,sharey=ax_xzd)
        ax_xzv=fig.add_subplot(142,sharex=ax_xzd)
        ax_xyv=fig.add_subplot(144,sharex=ax_xzd,sharey=ax_xzv)

        tilt_offset=0.15
        vel_offset=0.1

        curax=ax_xzd
        zero_plot(xz,curax,tilt_offset)
##        for hl in [-0.05,0.05]:
##            curax.axhline(y=hl, color='r', linestyle=':', linewidth=2)
        curax.set_ylabel('disp, m')
        curax.set_title(colname+' XZ')

        curax=ax_xyd
        zero_plot(xy,curax,tilt_offset)
##        for hl in [-0.05,0.05]:
##            curax.axhline(y=hl, color='r', linestyle=':', linewidth=2)
        curax.set_ylabel('disp, m')
        curax.set_title(colname+' XY')
        curax.set_ylim(-0.1,0.1)
        
        curax=ax_xzv
        zero_plot(vel_xz,curax,vel_offset)
##        for hl in [-0.5,-0.005,0.005,0.5]:
##            curax.axhline(y=hl, color='r', linestyle=':', linewidth=2)
        curax.set_ylabel('vel, m')
        curax.set_title(colname+' XZ')
        
        curax=ax_xyv
        zero_plot(vel_xy,curax,vel_offset)
##        for hl in [-0.5,-0.005,0.005,0.5]:
##            curax.axhline(y=hl, color='r', linestyle=':', linewidth=2)
        curax.set_ylabel('vel, m')
        curax.set_ylim(-0.01,0.01)
        curax.set_title(colname+' XY')
        
##        fig.tight_layout()
        plt.show()
        
    except:
        
        print colname, "ERROR in plotting displacements and velocities"

    
        
    
    
    

    
    
    

    
    


    
