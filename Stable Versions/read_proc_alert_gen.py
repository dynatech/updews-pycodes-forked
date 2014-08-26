import os
from datetime import datetime, date, time, timedelta
import pandas as pd
from pandas.stats.api import ols
import numpy as np
import matplotlib.pyplot as plt
import ConfigParser

import generic_functions as gf
import generate_proc_monitoring as genproc
import alert_evaluation as alert

from timelyTrigger import *


def set_monitoring_window(roll_window_length,data_dt,rt_window_length,num_roll_window_ops):

    roll_window_numpts=int(1+roll_window_length/data_dt)
    end, start, offsetstart=gf.get_rt_window(rt_window_length,roll_window_numpts,num_roll_window_ops)
    monwin_time=pd.date_range(start=offsetstart, end=end, freq='30Min',name=['ts'], closed=None)
    monwin=pd.DataFrame(data=np.nan*np.ones(len(monwin_time)), index=monwin_time)

    return roll_window_numpts, end, start, offsetstart, monwin
    

def set_monitoring_window(roll_window_length,data_dt,rt_window_length,num_roll_window_ops):

    roll_window_numpts=int(1+roll_window_length/data_dt)
    end, start, offsetstart=gf.get_rt_window(rt_window_length,roll_window_numpts,num_roll_window_ops)
    monwin_time=pd.date_range(start=offsetstart, end=end, freq='30Min',name='ts', closed=None)
    monwin=pd.DataFrame(data=np.nan*np.ones(len(monwin_time)), index=monwin_time)

    return roll_window_numpts, end, start, offsetstart, monwin

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
        finite_data=np.sum(np.where(np.isfinite(curxz.values))[0])
        if finite_data>0:
            curxz=curxz.resample('30Min',how='mean',base=0)
            curxy=curxy.resample('30Min',how='mean',base=0)
            curm=curm.resample('30Min',how='mean',base=0)
        else:
            print colname, n, "ERROR missing node data"
            #zeroing tilt and moisture data if node data is missing
            curxz=pd.DataFrame(data=np.zeros(len(monwin)), index=monwin.index)
            curxy=pd.DataFrame(data=np.zeros(len(monwin)), index=monwin.index)
            curm=pd.DataFrame(data=np.zeros(len(monwin)), index=monwin.index)     
        #5e. appending node series to list
        xz_series_list.append(curxz)
        xy_series_list.append(curxy)
        m_series_list.append(curm)

    return xz_series_list,xy_series_list,m_series_list

def create_fill_smooth_df(series_list,num_nodes,monwin, roll_window_numpts, to_fill, to_smooth):
    
    #concatenating series list into dataframe
    df=pd.concat(series_list, axis=1, join='outer', names=None)

    #renaming columns
    df.columns=[a for a in np.arange(0,1+num_nodes)]

    #dropping column "monwin" from df
    df=df.drop(0,1)

    if to_fill:
        #filling NAN values
        df=df.fillna(method='pad')
        df=df.fillna(method='bfill')

    #dropping rows outside monitoring window
    df=df[(df.index>=monwin.index[0])&(df.index<=monwin.index[-1])]

    if to_smooth:
        #smoothing dataframes with moving average
        df=pd.rolling_mean(df,window=roll_window_numpts)[roll_window_numpts-1:]

    #returning rounded-off values within monitoring window
    return np.round(df[(df.index>=monwin.index[0])&(df.index<=monwin.index[-1])],4)

def compute_col_pos(xz,xy,col_pos_end, col_pos_interval, col_pos_number, seg_len):

    #computing x from xz and xy
    x=pd.DataFrame(data=None,index=xz.index)
    num_nodes=len(xz.columns.tolist())
    for n in np.arange(1,1+num_nodes):
        x[n]=gf.x_from_xzxy(seg_len, xz.loc[:,n].values, xy.loc[:,n].values)

    #getting dates for column positions
    colposdates=pd.date_range(end=col_pos_end, freq=col_pos_interval,periods=col_pos_number, name='ts',closed=None)

    #reversing column order
    revcols=xz.columns.tolist()[::-1]
    xz=xz[revcols]
    xy=xy[revcols]
    x=x[revcols]

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
    
    
    #returning to original column order
    cols=cs_x.columns.tolist()[::-1]
    cs_xz=cs_xz[cols]
    cs_xy=cs_xy[cols]
    cs_x=cs_x[cols]

    #appending 0 values to bottom of column (last node)
    cs_x[num_nodes+1]=0
    cs_xz[num_nodes+1]=0
    cs_xy[num_nodes+1]=0

    
    return np.round(cs_x,4), np.round(cs_xz,4), np.round(cs_xy,4)
    
def compute_node_inst_vel(xz,xy,roll_window_numpts): 
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

def df_to_out(colname,xz,xy,
              vel_xz,vel_xy,
              cs_x,cs_xz,cs_xy,
              proc_monitoring_path,
              proc_monitoring_file):

    monwindate=(xz.index>=vel_xz.index[0])&(xz.index<=vel_xz.index[-1])

    #resizing dataframes
    xz=xz[(xz.index>=vel_xz.index[0])&(xz.index<=vel_xz.index[-1])]
    xy=xy[(xy.index>=vel_xz.index[0])&(xy.index<=vel_xz.index[-1])]
    cs_x=cs_x[(cs_x.index>=vel_xz.index[0])&(cs_x.index<=vel_xz.index[-1])]
    cs_xz=cs_xz[(cs_xz.index>=vel_xz.index[0])&(cs_xz.index<=vel_xz.index[-1])]
    cs_xy=cs_xy[(cs_xy.index>=vel_xz.index[0])&(cs_xy.index<=vel_xz.index[-1])]

    #creating zeroed and offset dataframes
    xz_0off=df_add_offset_col(df_zero_initial_row(xz),0.15)
    xy_0off=df_add_offset_col(df_zero_initial_row(xy),0.15)
    vel_xz_0off=df_add_offset_col(df_zero_initial_row(vel_xz),0.015)
    vel_xy_0off=df_add_offset_col(df_zero_initial_row(vel_xy),0.015)
    cs_xz_0=df_zero_initial_row(cs_xz)
    cs_xy_0=df_zero_initial_row(cs_xy)

    #writing to csv
    df_list=np.asarray([[xz,'xz'],
             [xy,'xy'],
             [xz_0off,'xz_0off'],
             [xy_0off,'xy_0off'],
             [vel_xz,'xz_vel'],
             [vel_xy,'xy_vel'],
             [vel_xz_0off,'xz_vel_0off'],
             [vel_xy_0off,'xy_vel_0off'],
             [cs_x,'x_cs'],
             [cs_xz,'xz_cs'],
             [cs_xy,'xy_cs'],
             [cs_xz_0,'xz_cs_0'],
             [cs_xy_0,'xy_cs_0']])

    
    for d in range(len(df_list)):
        df=df_list[d,0]
        fname=df_list[d,1]
        df.to_csv(proc_monitoring_path+colname+'/'+colname+" "+fname+proc_monitoring_file,
                  sep=',', header=False,mode='w')
        # for uploading to web server
        df.to_csv("D:\\csvForUpload\\"+colname+" "+fname+proc_monitoring_file,
                  sep=',', header=True,mode='w')

    return xz,xy,   xz_0off,xy_0off,   vel_xz,vel_xy, vel_xz_0off, vel_xy_0off, cs_x,cs_xz,cs_xy,   cs_xz_0,cs_xy_0

def alert_generation(colname,xz,xy,vel_xz,vel_xy,num_nodes, T_disp, T_velA1, T_velA2, k_ac_ax,
                     num_nodes_to_check,end,proc_monitoring_path,proc_monitoring_file):
        
    #processing node-level alerts
    alert_out=alert.node_alert(colname,xz,xy,vel_xz,vel_xy,num_nodes, T_disp, T_velA1, T_velA2, k_ac_ax)

    #processing column-level alerts
    alert_out=alert.column_alert(alert_out, num_nodes_to_check)

    #adding 'ts' 
    alert_out['ts']=end
    
    #setting ts and node_ID as indices
    alert_out=alert_out.set_index(['ts','id'])
    timeseries_alert=alert_out.iloc[:,-2:]
    #checks if file exist, append latest alert; else, write new file
##    if os.path.exists(proc_monitoring_path+colname+'/'+colname+" "+"alert"+proc_monitoring_file):
##        alert_written=pd.read_csv(proc_monitoring_path+colname+'/'+colname+" "+"alert"+proc_monitoring_file, header=None)
##        check_time=pd.Series(alert_written[0])
##        if check_time.values[-1]<str(end):
##            alert_out.to_csv(proc_monitoring_path+colname+'/'+colname+" "+"alert"+proc_monitoring_file,
##                             sep=',', header=False,mode='a')
##            timeseries_alert.to_csv(proc_monitoring_path+colname+'/'+colname+" "+"timeseries"+proc_monitoring_file,
##                             sep=',', header=False,mode='a')
##    else:
    alert_out.to_csv(proc_monitoring_path+colname+'/'+colname+" "+"alert"+proc_monitoring_file,
                     sep=',', header=False,mode='a')
    timeseries_alert.to_csv(proc_monitoring_path+colname+'/'+colname+" "+"timeseries"+proc_monitoring_file,
                     sep=',', header=False,mode='a')
    
    return alert_out
    
def plot_column_positions(colname,x,xz,xy):
    try:
        fig=plt.figure(1)
        plt.clf()
        plt.suptitle(colname+" absolute position")
        ax_xz=fig.add_subplot(121)
        ax_xy=fig.add_subplot(122,sharex=ax_xz,sharey=ax_xz)

        for i in x.index:
            curcolpos_x=x[(x.index==i)].values

            curax=ax_xz
            curcolpos_xz=xz[(xz.index==i)].values
            curax.plot(curcolpos_xz[0],curcolpos_x[0],'.-')
            curax.set_xlabel('xz')
            curax.set_ylabel('x')

            curax=ax_xy
            curcolpos_xy=xy[(xy.index==i)].values
            curax.plot(curcolpos_xy[0],curcolpos_x[0],'.-', label=i)
            curax.set_xlabel('xy')

        fig.tight_layout()
        plt.legend(fontsize='x-small')
        
    
    except:        
        print colname, "ERROR in plotting column position"
    return
    
def plot_disp_vel(colname, xz,xy,xz_vel,xy_vel):
    try:
        fig=plt.figure(2)
        plt.clf()
        ax_xzd=fig.add_subplot(141)
        ax_xyd=fig.add_subplot(142,sharex=ax_xzd,sharey=ax_xzd)

        ax_xzv=fig.add_subplot(143,sharex=ax_xzd)
        ax_xyv=fig.add_subplot(144,sharex=ax_xzv,sharey=ax_xzv)

        curax=ax_xzd
        plt.sca(curax)
        xz.plot(ax=curax,legend=False)
        curax.set_title(colname+' XZ')
        curax.set_ylabel('disp, m', fontsize='small')
        #curax.set_ylim(-0.1,0.1)
        
        curax=ax_xyd
        plt.sca(curax)
        xy.plot(ax=curax,legend=False)
        curax.set_title(colname+' XY')
        
        curax=ax_xzv
        plt.sca(curax)
        xz_vel.plot(ax=curax,legend=False)
        curax.set_ylabel('vel, m/day', fontsize='small')
        #curax.set_ylim(-0.01,0.01)
        
        curax=ax_xyv
        plt.sca(curax)
        xy_vel.plot(ax=curax,legend=False)
        
        fig.tight_layout()
        
        
    except:      
        print colname, "ERROR in plotting displacements and velocities"
    return


def df_zero_initial_row(df):
    #zeroing time series to initial value;
    #essentially, this subtracts the value of the first row
    #from all the rows of the dataframe
    return np.round(df-df.loc[(df.index==df.index[0])].values.squeeze(),4)

def df_add_offset_col(df,offset):
    #adding offset value based on column value (node ID);
    #topmost node (node 1) has largest offset
    for n in range(1,1+len(df.columns)):
        df[n]=df[n] + (len(df.columns)-n)*offset
    return np.round(df,4)
        
    

cfg = ConfigParser.ConfigParser()
cfg.read('server-config.txt')    

##set/get values from config file

#time interval between data points, in hours
data_dt = cfg.getfloat('I/O','data_dt')

#length of real-time monitoring window, in days
rt_window_length = cfg.getfloat('I/O','rt_window_length')

#length of rolling/moving window operations in hours
roll_window_length = cfg.getfloat('I/O','roll_window_length')

#number of rolling window operations in the whole monitoring analysis
num_roll_window_ops = cfg.getfloat('I/O','num_roll_window_ops')

#string expression indicating interval between two adjacent column position dates ex: '1D'= 1 day
col_pos_interval= cfg.get('I/O','col_pos_interval') 
#number of column position dates to plot
col_pos_num= cfg.getfloat('I/O','num_col_pos')             

#INPUT/OUTPUT FILES

#local file paths
columnproperties_path = cfg.get('I/O','ColumnPropertiesPath')
purged_path = cfg.get('I/O','InputFilePath')
monitoring_path = cfg.get('I/O','MonitoringPath')
LastGoodData_path = cfg.get('I/O','LastGoodData')
proc_monitoring_path = cfg.get('I/O','OutputFilePathMonitoring')

#file names
columnproperties_file = cfg.get('I/O','ColumnProperties')
purged_file = cfg.get('I/O','CSVFormat')
monitoring_file = cfg.get('I/O','CSVFormat')
LastGoodData_file = cfg.get('I/O','CSVFormat')
proc_monitoring_file = cfg.get('I/O','CSVFormat')

#file headers
columnproperties_headers = cfg.get('I/O','columnproperties_headers').split(',')
purged_file_headers = cfg.get('I/O','purged_file_headers').split(',')
monitoring_file_headers = cfg.get('I/O','monitoring_file_headers').split(',')
LastGoodData_file_headers = cfg.get('I/O','LastGoodData_file_headers').split(',')
proc_monitoring_file_headers = cfg.get('I/O','proc_monitoring_file_headers').split(',')

#ALERT CONSTANTS
T_disp = cfg.getfloat('I/O','T_disp')  #m
T_velA1 = cfg.getfloat('I/O','T_velA1') #m/day
T_velA2 = cfg.getfloat('I/O','T_velA2')  #m/day
k_ac_ax = cfg.getfloat('I/O','k_ac_ax')
num_nodes_to_check = cfg.getint('I/O','num_nodes_to_check')

#global variables
colname,num_nodes,seg_len = '',0,0

def main():
    #MAIN

    #0. Uncomment if you want to update proc_monitoring CSVs
    genproc.generate_proc()

    #1. setting monitoring window
    roll_window_numpts, end, start, offsetstart, monwin = set_monitoring_window(roll_window_length,data_dt,rt_window_length,num_roll_window_ops)

    #2. getting all column properties
    sensors=pd.read_csv(columnproperties_path+columnproperties_file,names=columnproperties_headers,index_col=None)

    print "Generating plots and alerts for:"
    for s in range(len(sensors)):
        #if s!=7: continue
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
        xz=create_fill_smooth_df(xz_series_list,num_nodes,monwin, roll_window_numpts,1,1)
        xy=create_fill_smooth_df(xy_series_list,num_nodes,monwin, roll_window_numpts,1,1)
        m=create_fill_smooth_df(m_series_list,num_nodes,monwin, roll_window_numpts,0,0)
          
        #7. computing instantaneous velocity 
        vel_xz, vel_xy = compute_node_inst_vel(xz,xy,roll_window_numpts)
        
        #8. computing cumulative displacements
        cs_x, cs_xz, cs_xy=compute_col_pos(xz,xy,monwin.index[-1], col_pos_interval, col_pos_num, seg_len)
        
        #9. processing dataframes for output
        xz,xy,xz_0off,xy_0off,vel_xz,vel_xy, vel_xz_0off, vel_xy_0off,cs_x,cs_xz,cs_xy,cs_xz_0,cs_xy_0 = df_to_out(colname,xz,xy,
                                                                                                               vel_xz,vel_xy,
                                                                                                               cs_x,cs_xz,cs_xy,
                                                                                                               proc_monitoring_path,
                                                                                                               proc_monitoring_file)
        
        
        #10. Plotting column positions
        #relative position
        plot_column_positions(colname,cs_x,cs_xz_0,cs_xy_0)
        
        #absolute position
        #plot_column_positions(colname,cs_x,cs_xz,cs_xy)
        
        #fixing initial x-axis limits
        curax=plt.gca()
        curax.set_xlim(-0.05,0.05)

        #11. Plotting displacement and velocity
        #no offset and zeroing  
        #plot_disp_vel(colname, xz,xy, vel_xz, vel_xy)
        
        #with offset and zeroing  
        plot_disp_vel(colname, xz_0off,xy_0off, vel_xz_0off, vel_xy_0off)
        
        
        #12. Alert generation
        xz=xz[(xz.index>=end-timedelta(days=3))]  #sorry, hard-coded value again
        xy=xy[(xy.index>=end-timedelta(days=3))]
        vel_xz=vel_xz[(vel_xz.index>=end-timedelta(days=3))]
        vel_xy=vel_xy[(vel_xy.index>=end-timedelta(days=3))]
        alert_out=alert_generation(colname,xz,xy,vel_xz,vel_xy,num_nodes, T_disp, T_velA1, T_velA2, k_ac_ax,
                                   num_nodes_to_check,end,proc_monitoring_path,proc_monitoring_file)
        #print alert_out

       

        #plt.show()


if __name__ == '__main__':
    main()






    
    
    

    
    


    
