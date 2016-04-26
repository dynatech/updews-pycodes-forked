##### IMPORTANT matplotlib declarations must always be FIRST to make sure that matplotlib works with cron-based automation
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
plt.ioff()

import os
from datetime import datetime, timedelta
import pandas as pd
from pandas.stats.api import ols
import numpy as np
import ConfigParser
from collections import Counter
import csv
import fileinput
import sys

import generic_functions as gf
import generateProcMonitoring as genproc
import alertEvaluation as alert

#include the path of "Data Analysis" folder for the python scripts searching
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
    sys.path.insert(1,path)
del path   

from querySenslopeDb import *
from filterSensorData import *





def set_monitoring_window(roll_window_length,data_dt,rt_window_length,num_roll_window_ops):
    
    ##DESCRIPTION:    
    ##returns number of data points per rolling window, endpoint of interval, starting point of interval, time interval for real-time monitoring, monitoring window dataframe
    
    ##INPUT:
    ##roll_window_length; float; length of rolling/moving window operations, in hours
    ##data_dt; float; time interval between data points, in hours    
    ##rt_window_length; float; length of real-time monitoring window, in days
    ##num_roll_window_ops
    
    ##OUTPUT:
    ##roll_window_numpts, end, start, offsetstart, monwin
    
    roll_window_numpts=int(1+roll_window_length/data_dt)
    end, start, offsetstart=gf.get_rt_window(rt_window_length,roll_window_numpts,num_roll_window_ops)
    monwin_time=pd.date_range(start=offsetstart, end=end, freq='30Min',name='ts', closed=None)
    monwin=pd.DataFrame(data=np.nan*np.ones(len(monwin_time)), index=monwin_time)
    return roll_window_numpts, end, start, offsetstart, monwin
    
def create_series_list(input_df,monwin,colname,num_nodes):
    
    ##DESCRIPTION:
    ##returns list of xz node series, xy node series and m node series
    
    ##INPUT:
    ##input_df; array of float
    ##monwin; empty dataframe
    ##colname; array; list of sites
    ##num_nodes; integer; number of nodes

    ##OUTPUT:
    ##xz_series_list, xy_series_list, m_series_list
    
    #a. initializing lists
    xz_series_list=[]
    xy_series_list=[] 
    
    #b.appending monitoring window dataframe to lists
    xz_series_list.append(monwin)
    xy_series_list.append(monwin)

    for n in range(1,1+num_nodes):
        
        #c.creating node series        
        curxz=input_df.loc[input_df.id==n,['xz']]
        curxy=input_df.loc[input_df.id==n,['xy']]
        #d.resampling node series to 30-min exact intervals
        finite_data=len(np.where(np.isfinite(curxz.values.astype(np.float64)))[0])
        if finite_data>0:
            curxz=curxz.resample('30Min',how='mean',base=0)
            curxy=curxy.resample('30Min',how='mean',base=0)
        else:
            print colname, n, "ERROR missing node data"
            #zeroing tilt data if node data is missing
            curxz=pd.DataFrame(data=np.zeros(len(monwin)), index=monwin.index)
            curxy=pd.DataFrame(data=np.zeros(len(monwin)), index=monwin.index)
        #5e. appending node series to list
        xz_series_list.append(curxz)
        xy_series_list.append(curxy)

    return xz_series_list,xy_series_list

def create_fill_smooth_df(series_list,num_nodes,monwin, roll_window_numpts, to_fill, to_smooth):
    
    ##DESCRIPTION:
    ##returns rounded-off values within monitoring window

    ##INPUT:
    ##series_list
    ##num_dodes; integer; number of nodes
    ##monwin; monitoring window dataframe
    ##roll_window_numpts; integer; number of data points per rolling window
    ##to_fill; filling NAN values
    ##to_smooth; smoothing dataframes with moving average

    ##OUTPUT:
    ##np.round(df[(df.index>=monwin.index[0])&(df.index<=monwin.index[-1])],4)
    
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

def compute_col_pos(xz,xy,col_pos_end, col_pos_interval, col_pos_number):

    ##DESCRIPTION:
    ##returns rounded values of cumulative displacements

    ##INPUT:
    ##xz; dataframe; horizontal linear displacements along the planes defined by xa-za
    ##xy; dataframe; horizontal linear displacements along the planes defined by xa-ya
    ##col_pos_end; string; right bound for generating dates
    ##col_pos_interval; string ; interval between two adjacent column position dates
    ##col_pos_number; integer; number of column position dates to plot

    ##OUTPUT:
    ##np.round(cs_x,4), np.round(cs_xz,4), np.round(cs_xy,4)    
    
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

    ##DESCRIPTION:
    ##returns rounded-off values of velocity of xz and xy

    ##INPUT:
    ##xz; dataframe; horizontal linear displacements along the planes defined by xa-za
    ##xy; dataframe; horizontal linear displacements along the planes defined by xa-ya
    ##roll_window_numpts; integer; number of data points per rolling window

    ##OUTPUT:
    ##np.round(vel_xz,4), np.round(vel_xy,4)

#    trimming xz and xy for a more efficient run
    end_xz = xz.index[-1]
    end_xy = xy.index[-1]
    start_xz = end_xz - timedelta(days=1)    
    start_xy = end_xy - timedelta(days=1)
    xz = xz.loc[start_xz:end_xz]
    xy = xy.loc[start_xy:end_xy]    
    
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
              proc_file_path,
              CSVFormat):

    ##DESCRIPTION:
    ##writes to csv and returns:
    ##horizontal linear displacements along the planes defined by xa-za, and xa-ya;
    ##zeroed and offset dataframes of xz and xy;
    ##velocities of xz and xy;
    ##zeroed and offset dataframes of velocities of xz and xy;
    ## resized dataframes of cumulative displacements;
    ##zeroed and offset dataframes of cumulative displacements

    ##INPUT:dfm = dfm.sort('ts')
    ##colname; string; name of site   
    ##xz; dataframe; horizontal linear displacements along the planes defined by xa-za
    ##xy; dataframe; horizontal linear displacements along the planes defined by xa-ya
    ##xz_vel; dataframe; velocity along the planes defined by xa-za
    ##xy_vel; dataframe; velocity along the planes defined by xa-ya
    ##cs_x; dataframe; cumulative vertical displacement
    ##cs_xz; dataframe; cumulative vertical displacement horizontal linear displacements along the planes defined by xa-za
    ##cs_xy; dataframe; cumulative vertical displacement horizontal linear displacements along the planes defined by xa-ya
    ##proc_file_path; file path
    ##CSVFormat; file type

    ##OUTPUT:
    ##xz,xy,   xz_0off,xy_0off,   vel_xz,vel_xy, vel_xz_0off, vel_xy_0off, cs_x,cs_xz,cs_xy,   cs_xz_0,cs_xy_0


    #resizing dataframes
    xz=xz[(xz.index>=vel_xz.index[0])&(xz.index<=vel_xz.index[-1])]
    xy=xy[(xy.index>=vel_xz.index[0])&(xy.index<=vel_xz.index[-1])]
    cs_x=cs_x[(cs_x.index>=vel_xz.index[0])&(cs_x.index<=vel_xz.index[-1])]
    cs_xz=cs_xz[(cs_xz.index>=vel_xz.index[0])&(cs_xz.index<=vel_xz.index[-1])]
    cs_xy=cs_xy[(cs_xy.index>=vel_xz.index[0])&(cs_xy.index<=vel_xz.index[-1])]


    #creating\ zeroed and offset dataframes
    xz_0off=df_add_offset_col(df_zero_initial_row(xz),0.15)
    xy_0off=df_add_offset_col(df_zero_initial_row(xy),0.15)
    vel_xz_0off=df_add_offset_col(df_zero_initial_row(vel_xz),0.015)
    vel_xy_0off=df_add_offset_col(df_zero_initial_row(vel_xy),0.015)
    cs_xz_0=df_zero_initial_row(cs_xz)
    cs_xy_0=df_zero_initial_row(cs_xy)

    #writing to csv
    if PrintProc:
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
            if not os.path.exists(proc_file_path+colname+"/"):
                os.makedirs(proc_file_path+colname+"/")
            df.to_csv(proc_file_path+colname+"/"+colname+" "+fname+CSVFormat,
                      sep=',', header=False,mode='w')

    return xz,xy,   xz_0off,xy_0off,   vel_xz,vel_xy, vel_xz_0off, vel_xy_0off, cs_x,cs_xz,cs_xy,   cs_xz_0,cs_xy_0

def alert_generation(colname,xz,xy,vel_xz,vel_xy,num_nodes, T_disp, T_velL2, T_velL3, k_ac_ax,
                     num_nodes_to_check,end,proc_file_path,CSVFormat):

    ##DESCRIPTION:
    ##returns node level alerts

    ##INPUT:
    ##colname; string; name of site    
    ##xz; dataframe; horizontal linear displacements along the planes defined by xa-za
    ##xy; dataframe; horizontal linear displacements along the planes defined by xa-ya
    ##xz_vel; dataframe; velocity along the planes defined by xa-za
    ##xy_vel; dataframe; velocity along the planes defined by xa-ya
    ##num_nodes; float; number of nodes
    ##T_disp; float; threshold values for displacement
    ##T_velL2; float; threshold velocities correspoding to alert level L2
    ##T_velL3; float; threshold velocities correspoding to alert level L3
    ##k_ac_ax; float; minimum value of (minimum velocity / maximum velocity) required to consider movement as valid
    ##num_nodes_to_check; integer; number of adjacent nodes to check for validating current node alert
    ##end; 
    ##proc_file_path; file path
    ##CSVFormat; file type

    ##OUTPUT:
    ##alert_out
 
    #processing node-level alerts
    alert_out=alert.node_alert(colname,xz,xy,vel_xz,vel_xy,num_nodes, T_disp, T_velL2, T_velL3, k_ac_ax)
    
    #processing column-level alerts
    alert_out=alert.column_alert(alert_out, num_nodes_to_check, k_ac_ax)

    #trending_col=alert.trending_col(alert_out,colname)

    #adding 'ts' 
    alert_out['ts']=end
    
    #setting ts and node_ID as indices
    alert_out=alert_out.set_index(['ts','id'])
    

    #checks if file exist, append latest alert; else, write new file
    if PrintProc:
        try:
            if os.path.exists(proc_file_path+colname+"/"+colname+" "+"alert"+CSVFormat) and os.stat(proc_file_path+colname+"/"+colname+" "+"alert"+CSVFormat).st_size != 0:
                alert_monthly=pd.read_csv(proc_file_path+colname+"/"+colname+" "+"alert"+CSVFormat,names=alert_headers,parse_dates='ts',index_col='ts')
                alert_monthly=alert_monthly[(alert_monthly.index>=end-timedelta(days=alert_file_length))]
                alert_monthly=alert_monthly.reset_index()
                alert_monthly=alert_monthly.set_index(['ts','id'])
                alert_monthly=alert_monthly.append(alert_out)
                alert_monthly=alert_monthly[alertgen_headers]
                alert_monthly.to_csv(proc_file_path+colname+"/"+colname+" "+"alert"+CSVFormat,
                                     sep=',', header=False,mode='w')
            else:
                if not os.path.exists(proc_file_path+colname+"/"):
                    os.makedirs(proc_file_path+colname+"/")
                alert_out.to_csv(proc_file_path+colname+"/"+colname+" "+"alert"+CSVFormat,
                                 sep=',', header=False,mode='w')
        except:
            print "Error in Printing Proc"

    
    return alert_out

def alert_summary(alert_out,alert_list):

    ##DESCRIPTION:
    ##creates list of sites per alert level

    ##INPUT:
    ##alert_out; array
    ##alert_list; array


    
    nd_check=alert_out.loc[(alert_out['node_alert']=='nd')|(alert_out['col_alert']=='nd')]
    if len(nd_check)>(num_nodes/2):
        nd_alert.append(colname)
        
    else:
        l3_check=alert_out.loc[(alert_out['node_alert']=='l3')|(alert_out['col_alert']=='l3')]
        l2_check=alert_out.loc[(alert_out['node_alert']=='l2')|(alert_out['col_alert']=='l2')]
        l0_check=alert_out.loc[(alert_out['node_alert']=='l0')]
        checklist=[l3_check,l2_check,l0_check]
        
        for c in range(len(checklist)):
            if len(checklist[c])!=0:
                checklist[c]=checklist[c].reset_index()
                alert_list[c].append(colname + str(checklist[c]['id'].values[0]))
                if c==2: continue
                print checklist[c].set_index(['ts','id']).drop(['disp_alert','min_vel','max_vel','vel_alert'], axis=1)
                break
                
def nonrepeat_colors(ax,NUM_COLORS,color='gist_rainbow'):
    cm = plt.get_cmap(color)
    ax.set_color_cycle([cm(1.*(NUM_COLORS-i-1)/NUM_COLORS) for i in range(NUM_COLORS)])
    return ax
    
    
def plot_column_positions(colname,x,xz,xy):

    ##DESCRIPTION
    ##returns plot of xz and xy absolute displacements of each node

    ##INPUT
    ##colname; array; list of sites
    ##x; dataframe; vertical displacements
    ##xz; dataframe; horizontal linear displacements along the planes defined by xa-za
    ##xy; dataframe; horizontal linear displacements along the planes defined by xa-ya

    try:
        fig=plt.figure(1)
        plt.clf()
        plt.suptitle(colname+" absolute position", fontsize = 12)
        ax_xz=fig.add_subplot(121)
        ax_xy=fig.add_subplot(122,sharex=ax_xz,sharey=ax_xz)
        
        ax_xz=nonrepeat_colors(ax_xz,len(cs_x))
        ax_xy=nonrepeat_colors(ax_xy,len(cs_x))        
        
        print 'plot_column_positions 1'
        for i in cs_x.index:
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
    
        for tick in ax_xz.xaxis.get_minor_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(10)
    
        for tick in ax_xy.xaxis.get_minor_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(10)
    
        for tick in ax_xz.xaxis.get_major_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(10)
       
        for tick in ax_xy.xaxis.get_major_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(10)
    
        fig.tight_layout()
        plt.legend(fontsize='x-small')        

    except:        
        print colname, "ERROR in plotting column position"
    return
    
def plot_disp_vel(colname, xz,xy,xz_vel,xy_vel):

    ##DESCRIPTION:
    ##returns plot of xz and xy displacements per node, xz and xy velocities per node

    ##INPUT:
    ##xz; array of floats; horizontal linear displacements along the planes defined by xa-za
    ##xy; array of floats; horizontal linear displacements along the planes defined by xa-ya
    ##xz_vel; array of floats; velocity along the planes defined by xa-za
    ##xy_vel; array of floats; velocity along the planes defined by xa-ya

    try:
        fig=plt.figure(2)
        plt.clf()
        ax_xzd=fig.add_subplot(141)
        ax_xyd=fig.add_subplot(142,sharex=ax_xzd,sharey=ax_xzd)
    
        ax_xzv=fig.add_subplot(143)
        ax_xyv=fig.add_subplot(144,sharex=ax_xzv,sharey=ax_xzv)
        
        ax_xzd=nonrepeat_colors(ax_xzd,len(xz.columns))
        ax_xyd=nonrepeat_colors(ax_xyd,len(xz.columns))
        ax_xzv=nonrepeat_colors(ax_xzv,len(xz.columns))
        ax_xyv=nonrepeat_colors(ax_xyv,len(xz.columns))
    
        curax=ax_xzd
        plt.sca(curax)
        xz.plot(ax=curax,legend=False)
        curax.set_title(colname+' XZ')
        curax.set_ylabel('disp, m', fontsize='small')
        
        curax=ax_xyd
        plt.sca(curax)
        xy.plot(ax=curax,legend=False)
        curax.set_title(colname+' XY')
        
        curax=ax_xzv
        plt.sca(curax)
        xz_vel.plot(ax=curax,legend=False)
        curax.set_ylabel('vel, m/day', fontsize='small')
        
        curax=ax_xyv
        plt.sca(curax)
        xy_vel.plot(ax=curax,legend=False)
        
        # rotating xlabel
        
        for tick in ax_xzd.xaxis.get_minor_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(8)
            
        for tick in ax_xyd.xaxis.get_minor_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(8)
    
        for tick in ax_xzv.xaxis.get_minor_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(8)
    
        for tick in ax_xyv.xaxis.get_minor_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(8)
    
        for tick in ax_xzd.xaxis.get_major_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(8)
            
        for tick in ax_xyd.xaxis.get_major_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(8)
    
        for tick in ax_xzv.xaxis.get_major_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(8)
    
        for tick in ax_xyv.xaxis.get_major_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(8)
                
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

        
start_time=datetime.now()

#Function for directory manipulations
def up_one(p):
    out = os.path.abspath(os.path.join(p, '..'))
    return out

cfg = ConfigParser.ConfigParser()
cfg.read(up_one(os.path.dirname(__file__))+'/server-config.txt')

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

output_path = up_one(up_one(up_one(os.path.dirname(__file__))))

nd_path = output_path + cfg.get('I/O', 'NDFilePath')
output_file_path = output_path + cfg.get('I/O','OutputFilePath')
proc_file_path = output_path + cfg.get('I/O','ProcFilePath')
ColAlerts_file_path = output_path + cfg.get('I/O','ColAlertsFilePath')
TrendAlerts_file_path = output_path + cfg.get('I/O','TrendAlertsFilePath')

#Create filepaths if it does not exists
def create_dir(p):
    if not os.path.exists(p):
        os.makedirs(p)

directories = [nd_path,output_file_path,proc_file_path,ColAlerts_file_path,TrendAlerts_file_path]
for p in directories:
    create_dir(p)

#file names
#columnproperties_file = cfg.get('I/O','ColumnProperties')
CSVFormat = cfg.get('I/O','CSVFormat')
webtrends = cfg.get('I/O','webtrends')
textalert = cfg.get('I/O','textalert')
textalert2 = cfg.get('I/O','textalert2')
rainfall_alert = cfg.get('I/O','rainfallalert')
all_alerts = cfg.get('I/O','allalerts')
gsm_alert = cfg.get('I/O','gsmalert')
eq_summary = cfg.get('I/O','eqsummary')
eq_summaryGSM = cfg.get('I/O','eqsummaryGSM')
timer = cfg.get('I/O','timer')
NDlog = cfg.get('I/O','NDlog')
ND7x = cfg.get('I/O','ND7x')

#Create webtrends.csv if it does not exists

files = [webtrends,textalert,textalert2,rainfall_alert,all_alerts,gsm_alert,eq_summary,eq_summaryGSM,timer,NDlog,ND7x]

def create_file(f):
    if not os.path.isfile(f):
        with open(f,'w') as t:
            pass

for f in files:
    create_file(output_file_path + f)


#file headers
proc_monitoring_file_headers = cfg.get('I/O','proc_monitoring_file_headers').split(',')
alert_headers = cfg.get('I/O','alert_headers').split(',')
alertgen_headers = cfg.get('I/O','alertgen_headers').split(',')

#ALERT CONSTANTS
T_disp = cfg.getfloat('I/O','T_disp')  #m
T_velL2 = cfg.getfloat('I/O','T_velL2') #m/day
T_velL3 = cfg.getfloat('I/O','T_velL3')  #m/day
k_ac_ax = cfg.getfloat('I/O','k_ac_ax')
num_nodes_to_check = cfg.getint('I/O','num_nodes_to_check')
alert_file_length=cfg.getint('I/O','alert_time_int') # in days


#To Output File or not
PrintProc = cfg.getboolean('I/O','PrintProc')
PrintColPos = cfg.getboolean('I/O','PlotColPos')
PrintDispVel = cfg.getboolean('I/O','PlotDispVel')
PrintTrendAlerts = cfg.getboolean('I/O','PrintTrendAlerts')
PrintTAlert = cfg.getboolean('I/O','PrintTAlert')
PrintTAlert2 = cfg.getboolean('I/O','PrintTAlert2')
PrintWAlert = cfg.getboolean('I/O','PrintWAlert')
PrintND = cfg.getboolean('I/O','PrintND')
PrintTimer = cfg.getboolean('I/O','PrintTimer')
PrintAAlert = cfg.getboolean('I/O','PrintAAlert')
PrintGSMAlert = cfg.getboolean('I/O', 'PrintGSMAlert')

#if PrintColPos or PrintTrendAlerts:
#    import matplotlib.pyplot as plt
#    plt.ioff()


#MAIN

#Set as true if printing by JSON would be done
set_json = False


# setting monitoring window
roll_window_numpts, end, start, offsetstart, monwin = set_monitoring_window(roll_window_length,data_dt,rt_window_length,num_roll_window_ops)

# creating summary of alerts
nd_alert=[]
l0_alert=[]
l2_alert=[]
l3_alert=[]
alert_df = []
alert_list=[l3_alert,l2_alert,l0_alert,nd_alert]
alert_names=['l3: ','l2: ','l0: ','ND: ']

# creating dictionary of working nodes per site
wn = open('working_nodes.txt', 'r')
working_nodes = {}
for line in wn:
    lst = line.split(',')
    site = lst[0]
    for i in range(1,len(lst)):
        lst[i] = int(lst[i])
    nodes = lst[1:len(lst)]
    working_nodes[site] = nodes

print "Generating plots and alerts for:"

names = ['ts','col_a']
fmt = '%Y-%m-%d %H:%M'
hr = end - timedelta(hours=3)

with open(output_file_path+webtrends, 'ab') as w, open (output_file_path+textalert, 'wb') as t:
    t.write('As of ' + end.strftime(fmt) + ':\n')
    w.write(end.strftime(fmt) + ';')


CreateColAlertsTable('col_alerts', Namedb)

# getting list of sensors
sensorlist = GetSensorList()

for s in sensorlist:

    last_col=sensorlist[-1:]
    last_col=last_col[0]
    last_col=last_col.name
    
    # getting current column properties
    colname,num_nodes,seg_len= s.name,s.nos,s.seglen
    print colname, num_nodes, seg_len

    # importing proc_monitoring csv file of current column to dataframe
    try:
        proc_monitoring=genproc.generate_proc(colname)
        print proc_monitoring
        print "\n", colname
    except:
        print "     ",colname, "ERROR...missing/empty proc monitoring csv"
        continue

    # creating series lists per node
    xz_series_list,xy_series_list = create_series_list(proc_monitoring,monwin,colname,num_nodes)

    # create, fill and smooth dataframes from series lists
    xz=create_fill_smooth_df(xz_series_list,num_nodes,monwin, roll_window_numpts,1,1)
    xy=create_fill_smooth_df(xy_series_list,num_nodes,monwin, roll_window_numpts,1,1)
    
    # computing instantaneous velocity
    vel_xz, vel_xy = compute_node_inst_vel(xz,xy,roll_window_numpts)
    
    # computing cumulative displacements
    cs_x, cs_xz, cs_xy=compute_col_pos(xz,xy,monwin.index[-1], col_pos_interval, col_pos_num)

    # processing dataframes for output
    xz,xy,xz_0off,xy_0off,vel_xz,vel_xy, vel_xz_0off, vel_xy_0off,cs_x,cs_xz,cs_xy,cs_xz_0,cs_xy_0 = df_to_out(colname,xz,xy,
                                                                                                               vel_xz,vel_xy,
                                                                                                               cs_x,cs_xz,cs_xy,
                                                                                                               proc_file_path,
                                                                                                               CSVFormat)
                                                                                                                          
    # Alert generation
    xz=xz[(xz.index>=end-timedelta(days=3))]
    xy=xy[(xy.index>=end-timedelta(days=3))]
    vel_xz=vel_xz[(vel_xz.index>=end-timedelta(days=3))]
    vel_xy=vel_xy[(vel_xy.index>=end-timedelta(days=3))]
    alert_out=alert_generation(colname,xz,xy,vel_xz,vel_xy,num_nodes, T_disp, T_velL2, T_velL3, k_ac_ax,
                               num_nodes_to_check,end,proc_file_path,CSVFormat)
    print alert_out

########################################################################

    #connecting to localdb
    db = MySQLdb.connect(host = Hostdb, user = Userdb, passwd = Passdb)
    cur = db.cursor()
    cur.execute("USE %s"%Namedb)


    #writes col_alert to csv    
    for s in range(len(pd.Series.tolist(alert_out.col_alert))):
        query = """INSERT IGNORE INTO col_alerts (sitecode, timestamp, id, alerts) VALUES """
        query = query + str((str(colname), str(end), str(s+1), str(pd.Series.tolist(alert_out.col_alert)[s])))
        cur.execute(query)
        db.commit()

    #deletes col_alerts older than 3 hrs
    query = """DELETE FROM col_alerts WHERE timestamp < TIMESTAMP('%s')""" % hr
    cur.execute(query)
    db.commit()  
    
    #selects and otputs to dataframe col_alerts from the last 3hrs
    query = "select sitecode, timestamp, id, alerts from senslopedb.col_alerts where timestamp >= timestamp('%s')" % hr
    query = query + " and timestamp <= timestamp('%s')" % end
    query = query + " and id >= 1 and id <= %s" % num_nodes
    query = query + " and sitecode = '%s' ;" % colname
    df =  GetDBDataFrame(query)   
    df.columns = ['sitecode', 'timestamp', 'id', 'alerts']
    df.timestamp = pd.to_datetime(df.timestamp)
    df = df[['timestamp', 'id', 'alerts']]
    print df
    
    db.close()
    
###############################################################################
    
    # trending node alert for all nodes
    trending_node_alerts = []
    for n in range(1,1+num_nodes): 
        calert = df.loc[df['id'] == n]        
        node_trend = pd.Series.tolist(calert.alerts)
        counter = Counter(node_trend)
        max_count = max(counter.values())
        mode = [k for k,v in counter.items() if v == max_count]
        if 'l3' in mode:
            mode = ['l3']
        elif 'l2' in mode:
            mode = ['l2']
        elif 'nd' in mode:
            mode = ['nd']   
        elif 'l0' in mode:
            mode = ['l0']
        else:
            print "No node data for node " + str(n) + " in" + colname
        trending_node_alerts.extend(mode)

    # trending node alert for working nodes
    working_node_alerts = []

    try:
        for n in working_nodes.get(colname):
            working_node_alerts += [trending_node_alerts[n-1]]
    except TypeError:
        continue
        
    #adding trending node alerts to alert output table 
    alert_out['trending_alert']=trending_node_alerts
    print alert_out

    if PrintTrendAlerts:    
        with open(TrendAlerts_file_path+colname+CSVFormat, "ab") as c:
            trending_node_alerts.insert(0, end.strftime(fmt))
            wr = csv.writer(c, quoting=False)
            wr.writerows([trending_node_alerts])   
        
        seen = set() # set for fast O(1) amortized lookup
        for line in fileinput.FileInput(TrendAlerts_file_path+colname+CSVFormat, inplace=1):
         if line in seen: continue # skip duplicate
    
         seen.add(line)
         print line, # standard output is now redirected to the file
    
    # writes sensor name and sensor alerts alphabetically, one sensor per row, in textalert
    if working_node_alerts.count('l3') != 0:
        if PrintTAlert:
            with open (output_file_path+textalert, 'ab') as t:
                t.write (colname + ":" + 'l3' + '\n')
        l3_alert.append(colname)
        alert_df.append((end,colname,'l3'))                
    elif working_node_alerts.count('l2') != 0:
        if PrintTAlert:
            with open (output_file_path+textalert, 'ab') as t:
                t.write (colname + ":" + 'l2' + '\n')
        l2_alert.append(colname)
        alert_df.append((end,colname,'l2'))
    elif (colname == 'sinb') or (colname == 'blcb'):
        if working_node_alerts.count('l0') > 0:
            if PrintTAlert:
                with open (output_file_path+textalert, 'ab') as t:
                    t.write (colname + ":" + 'l0' + '\n')
            l0_alert.append(colname)
            alert_df.append((end,colname,'l0'))
        else:
            if PrintTAlert:
                with open (output_file_path+textalert, 'ab') as t:
                    t.write (colname + ":" + 'nd' + '\n')
            nd_alert.append(colname)
            alert_df.append((end,colname,'nd'))
    else:
        working_node_alerts_count = Counter(working_node_alerts)  
        if PrintTAlert:
            with open (output_file_path+textalert, 'ab') as t:
                t.write (colname + ":" + (working_node_alerts_count.most_common(1)[0][0]) + '\n')
        if (working_node_alerts_count.most_common(1)[0][0] == 'l0'):
            l0_alert.append(colname)
            alert_df.append((end,colname,'l0'))
        else:
            nd_alert.append(colname)
            alert_df.append((end,colname,'nd'))
#        
        if len(calert.index)<7:
            print 'Trending alert note: less than 6 data points for ' + colname
    
    # writes sensor alerts in one row in webtrends
    if PrintWAlert:
        with open(output_file_path+webtrends, 'ab') as w:
                if working_node_alerts.count('l3') != 0:
                    w.write ('l3' + ',')
                elif working_node_alerts.count('l2') != 0:
                    w.write ('l2' + ',')
                elif (colname == 'sinb') or (colname == 'blcb'):
                    if working_node_alerts.count('l0') > 0:
                        w.write ('l0' + ',')
                    else:
                        w.write ('nd' + ',')       
                else:
                    working_node_alerts = Counter(working_node_alerts)  
                    w.write ((working_node_alerts.most_common(1)[0][0]) + ',')
        #        
                if len(calert.index)<7:
                    print 'Trending alert note: less than 6 data points for ' + colname
                
                if colname == last_col:
                           w.seek(-1, os.SEEK_END)
                           w.truncate()
                           w.write('\n')
    
    print alert_out
  
#    prints to csv: node alert, column alert and trending alert of sites with nd alert
    if PrintND:
        for colname in nd_alert:
            if os.path.exists(nd_path + colname + CSVFormat):
                alert_out[['node_alert', 'col_alert', 'trending_alert']].to_csv(nd_path + colname + CSVFormat, sep=',', header=False, mode='a')
            else:
                alert_out[['node_alert', 'col_alert', 'trending_alert']].to_csv(nd_path + colname + CSVFormat, sep=',', header=True, mode='w')

#    #11. Plotting column positions
    if PrintColPos:
        plot_column_positions(colname,cs_x,cs_xz_0,cs_xy_0)
        plot_column_positions(colname,cs_x,cs_xz,cs_xy)
        plt.savefig(output_file_path+colname+' colpos ',
                    dpi=160, facecolor='w', edgecolor='w',orientation='landscape',mode='w')
#
    #12. Plotting displacement and velocity
    if PrintDispVel:
        plot_disp_vel(colname, xz_0off,xy_0off, vel_xz_0off, vel_xy_0off)
        plt.savefig(output_file_path+colname+' disp_vel ',
                    dpi=160, facecolor='w', edgecolor='w',orientation='landscape',mode='w')

    if PrintColPos or PrintDispVel:
        plt.close()

# writes list of site per alert level in textalert2
if PrintTAlert2:
    with open (output_file_path+textalert2, 'wb') as t:
        t.write('As of ' + end.strftime(fmt) + ':\n')
        t.write ('l0: ' + ','.join(sorted(l0_alert)) + '\n')
        t.write ('nd: ' + ','.join(sorted(nd_alert)) + '\n')
        t.write ('l2: ' + ','.join(sorted(l2_alert)) + '\n')
        t.write ('l3: ' + ','.join(sorted(l3_alert)) + '\n')


#Prints rainfall alerts, text alert and eq summary in one file
if PrintAAlert:
    with open(output_file_path+all_alerts, 'wb') as allalerts:
        allalerts.write('As of ' + end.strftime(fmt) + ':\n')
        allalerts.write('l3: ' + ','.join(sorted(l3_alert)) + '\n')
        allalerts.write('l2: ' + ','.join(sorted(l2_alert)) + '\n')
        allalerts.write('\n')
        with open(output_file_path+rainfall_alert) as rainfallalert:
            n = 0
            for line in rainfallalert:
                if n == 0 or n == 3 or n == 4:
                    allalerts.write(line)
                n += 1
            allalerts.write('\n')
        with open(output_file_path+eq_summary) as eqsummary:
            for line in eqsummary:
                allalerts.write(line)

if PrintGSMAlert:
    with open(output_file_path+gsm_alert, 'wb') as gsmalert:
        if len(l3_alert) != 0:
            gsmalert.write('l3: ' + ','.join(sorted(l3_alert)) + '\n')
        if len(l2_alert) != 0:
            gsmalert.write('l2: ' + ','.join(sorted(l2_alert)) + '\n')
        with open(output_file_path+rainfall_alert) as rainfallalert:
            n = 0
            for line in rainfallalert:
                if n == 3 or n == 4:
                    if len(line) > 6:
                        gsmalert.write(line)
                n += 1
        with open(output_file_path+eq_summaryGSM) as eqsummary:
            n = 0            
            for line in eqsummary:
                if n == 0:
                    eqalert = line[6:25]
                    if end - pd.to_datetime(eqalert) > timedelta(hours = 0.5):
                        break
                else:
                    gsmalert.write(line)
                n += 1

if PrintGSMAlert:                        
    f = open(output_file_path+gsm_alert)
    text = f.read()
    f.close()
    if os.stat(output_file_path+gsm_alert).st_size != 0:
        f = open(output_file_path+gsm_alert, 'w')
        f.write('As of ' + end.strftime(fmt) + ':\n')
        f.write(text)
        f.close()

name = []
nos = []
for col in sensorlist:
    name += [col.name]
    nos += [col.nos]
sensors = pd.DataFrame(data=None)
sensors['name']=name
sensors['nos']=nos
sensors=sensors.set_index('name')

# gets list of working sites
working_sites = []
with open('working_sites.txt', 'r') as SQLsites:
    for line in SQLsites:
        working_sites += [line.split('\n')[0]]
        

## creates list of sites with no data and classifies whether its raw or filtered
#if PrintND:
#    with open(output_file_path+NDlog, 'ab') as ND:
#        if len(l0_alert) == 0 and len(l2_alert) == 0 and len(l3_alert) == 0:
#            ND.write(end.strftime(fmt) + ',D,')
#            ND.write("ND on all sites,")
#            ND.write(',\n')
#    if len(l0_alert) != 0 or len(l2_alert) != 0 or len(l3_alert) != 0:
#        with open(output_file_path+NDlog, 'ab') as ND:
#            try:
#                ND.write(end.strftime(fmt) + ',D,')
#                for colname in nd_alert:
#                    filtered = pd.read_csv(proc_file_path+colname+"/"+colname+" "+"alert"+CSVFormat, names=alert_headers,parse_dates='ts',index_col='ts')
#                    filtered = filtered[(filtered.index>=end)]
#                    print 'filtered'            
#                    print filtered
#                    raw = GetFilledAccelData(colname, end - timedelta(hours=0.5))
#                    raw = raw.set_index('ts')
#                    raw = raw[(raw.index>=end)]
#                    print 'raw'            
#                    print raw
#                    filteredND = []
#                    rawND = []
#                    for i in filtered.loc[filtered['node_alert']=='nd', ['id']].values:
#                        if i[0] in raw['id'].values:
#                            filteredND += [str(i[0])]
#                        else:
#                            rawND += [str(i[0])]
#                    print 'filtered nodes'
#                    print filteredND
#                    print 'raw nodes'            
#                    print rawND
#                    num_nodes = str(sensors.loc[sensors.index==colname, ['nos']].values[0][0])
#                    print num_nodes
#                    if len(filteredND) != 0 and colname in working_sites:
#                        ND.write(colname + '(f-' + str(len(filteredND)) + '/' + num_nodes + ');')
#                    if len(rawND) != 0 and colname in working_sites:
#                        ND.write(colname + '(r-' + str(len(rawND)) + '/' + num_nodes + ');')
#                ND.write(',\n')
#            except:
#                pass
#
## creates list of site with no data for 7 consecutive times
#    with open(output_file_path + ND7x, 'ab') as ND7x:
#        try:
#            NDlog = pd.read_csv(output_file_path + NDlog, names = ['ts', 'R or A or D', 'description', 'responder'], parse_dates = 'ts', index_col = 'ts')
#            NDlog = NDlog[(NDlog.index>=end-timedelta(hours=3))]
#            if len(NDlog.loc[NDlog['R or A or D']=='R']) != 0 and len(NDlog.loc[NDlog['R or A or D']=='D']) < 7:
#                ND7x.write('')
#            else:    
#                NDlog = NDlog.loc[NDlog['R or A or D']=='D']
#                NDcolumns = NDlog['description'].values
#                for s in range(len(NDcolumns)):
#                    NDcolumns[s] = NDcolumns[s].split(';')
#                    NDs = []
#                    for n in NDcolumns[s]:
#                        ND = ''
#                        for i in n:
#                            if i != '(':
#                                ND += i
#                            else:
#                                NDs += [ND]
#                    NDcolumns[s] = NDs
#                NDlog['description'] = NDcolumns
#            ND7 = []
#            for n in NDlog['description'].values[-1]:
#                if n in NDlog['description'].values[0] and NDlog['description'].values[1] and \
#                NDlog['description'].values[2] and NDlog['description'].values[3] and NDlog['description'].values[4] \
#                and NDlog['description'].values[5]:
#                    ND7 += [n]
#            if len(ND7) != 0:
#                ND7x.write(end.strftime(fmt) + ',')
#                ND7x.write(';'.join(ND7))
#                ND7x.write('\n')
#        except:
#            pass

# records the number of minutes the code runs
if PrintTimer:
    end_time = datetime.now() - start_time
    with open (output_file_path+timer, 'ab') as p:
        p.write (start_time.strftime(fmt) + ": " + str(end_time) + '\n')
        print 'run time =', end_time
    
#Printing of JSON format:
if set_json:
#create data frame for easy JSON format printing
    dfa = pd.DataFrame(alert_df,columns = ['timestamp','site','s alert'])

#convert data frame to JSON format
    dfajson = dfa.to_json(orient="records",date_format='iso')
#ensuring proper datetime format
    i = 0
    while i <= len(dfajson):
        if dfajson[i:i+9] == 'timestamp':
            dfajson = dfajson[:i] + dfajson[i:i+36].replace("T"," ").replace("Z","").replace(".000","") + dfajson[i+36:]
            i += 1
        else:
            i += 1
    print dfajson


