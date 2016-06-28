# -*- coding: utf-8 -*-
"""
Created on Wed Jun 15 13:35:02 2016

@author: kennex
"""

import querySenslopeDb as qdb
import filterSensorData as fsd
import pandas as pd
from datetime import datetime, date, time, timedelta
import cfgfileio as cfg
import numpy as np
import filter_daily_flucts as ffd
import os
from pandas.stats.api import ols
#import statsmodels.api as sm
def get_rt_window(rt_window_length,roll_window_size,num_roll_window_ops,end=''):
    
    end = pd.to_datetime(end) 
    end_Year=end.year
    end_month=end.month
    end_day=end.day
    end_hour=end.hour
    end_minute=end.minute
    if end_minute<30:end_minute=0
    else:end_minute=30
    end=datetime.combine(date(end_Year,end_month,end_day),time(end_hour,end_minute,0))

    #starting point of the interval
    start=end-timedelta(days=rt_window_length)
    
    #starting point of interval with offset to account for moving window operations 
    offsetstart=end-timedelta(days=rt_window_length+((num_roll_window_ops*roll_window_size-1)/48.))

    monwin_time=pd.date_range(start=offsetstart, end=end, freq='30Min',name='ts', closed=None)
    monwin=pd.DataFrame(data=np.nan*np.ones(len(monwin_time)), index=monwin_time)

    return end, start, offsetstart,monwin
    
def accel_to_lin_xz_xy(seg_len,xa,ya,za):

    x=seg_len/np.sqrt(1+(np.tan(np.arctan(za/(np.sqrt(xa**2+ya**2))))**2+(np.tan(np.arctan(ya/(np.sqrt(xa**2+za**2))))**2)))
    xz=x*(za/(np.sqrt(xa**2+ya**2)))
    xy=x*(ya/(np.sqrt(xa**2+za**2)))
    
    return np.round(xz,4),np.round(xy,4)
    
def create_series_list(input_df,monwin,colname,num_nodes):
    
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
#            curxz=curxz.resample('30Min',how='mean',base=0)
            curxz=curxz.resample('30Min',base=0).mean()
#            curxy=curxy.resample('30Min',how='mean',base=0)
            curxy=curxy.resample('30Min',base=0).mean()
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
 
    #dropping rows outside monitoring window
    df=df[(df.index>=monwin.index[0])&(df.index<=monwin.index[-1])]

    if to_smooth:
        #smoothing dataframes with moving average
#        df=pd.rolling_mean(df,window=roll_window_numpts)[roll_window_numpts-1:]
        df = df.rolling(window=7,center=False).mean()[roll_window_numpts-1:]
    #returning rounded-off values within monitoring window
    return np.round(df[(df.index>=monwin.index[0])&(df.index<=monwin.index[-1])],4)
    
def GetNodesWithNoInitialData(df,num_nodes,offsetstart):
    allnodes=np.arange(1,num_nodes+1)*1.
    with_init_val=df[df.ts<offsetstart+timedelta(hours=0.5)]['id'].values
    no_init_val=allnodes[np.in1d(allnodes, with_init_val, invert=True)]
    return no_init_val
    
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

        lr_xz=ols(y=xz[n],x=td,window=roll_window_numpts,intercept=True)
        lr_xy=ols(y=xy[n],x=td,window=roll_window_numpts,intercept=True)

        vel_xz[n]=np.round(lr_xz.beta.x.values,4)
        vel_xy[n]=np.round(lr_xy.beta.x.values,4)


    #returning rounded-off values
    return np.round(vel_xz,4), np.round(vel_xy,4)
    
def compute_col_pos(xz,xy,col_pos_end, col_pos_interval, col_pos_number):

    
    #computing x from xz and xy
    x=pd.DataFrame(data=None,index=xz.index)
    num_nodes=len(xz.columns.tolist())
    for n in np.arange(1,1+num_nodes):
        x[n]=x_from_xzxy(seg_len, xz.loc[:,n].values, xy.loc[:,n].values)

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
    
def df_to_out(colname,xz,xy,
              vel_xz,vel_xy,
              cs_x,cs_xz,cs_xy,
              proc_file_path,
              CSVFormat):


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


    return xz,xy,   xz_0off,xy_0off,   vel_xz,vel_xy, vel_xz_0off, vel_xy_0off, cs_x,cs_xz,cs_xy,   cs_xz_0,cs_xy_0

def x_from_xzxy(seg_len, xz, xy):
  
    cond=(xz==0)*(xy==0)
    diagbase=np.sqrt(np.power(xz,2)+np.power(xy,2))
    return np.round(np.where(cond,
                             seg_len*np.ones(len(xz)),
                             np.sqrt(seg_len**2-np.power(diagbase,2))),2)

def df_add_offset_col(df,offset):
    #adding offset value based on column value (node ID);
    #topmost node (node 1) has largest offset
    for n in range(1,1+len(df.columns)):
        df[n]=df[n] + (len(df.columns)-n)*offset
    return np.round(df,4)

def df_zero_initial_row(df):
    #zeroing time series to initial value;
    #essentially, this subtracts the value of the first row
    #from all the rows of the dataframe
    return np.round(df-df.loc[(df.index==df.index[0])].values.squeeze(),4)
    
def node_alert(colname, xz_tilt, xy_tilt, xz_vel, xy_vel, num_nodes, T_disp, T_velL2, T_velL3, k_ac_ax,end):

   #initializing DataFrame object, alert
    alert=pd.DataFrame(data=None)

    #adding node IDs
    alert['id']=[n for n in range(1,1+num_nodes)]
    alert=alert.set_index('id')

    #checking for nodes with no data
    LastGoodData= qdb.GetLastGoodDataFromDb(colname)
    LastGoodData=LastGoodData[:num_nodes]
    cond = np.asarray((LastGoodData.ts< end - timedelta(hours=3)))
    if len(LastGoodData)<num_nodes:
        x=np.ones(num_nodes-len(LastGoodData),dtype=bool)
        cond=np.append(cond,x)
    alert['ND']=np.where(cond,
                         
                         #No data within valid date 
                         np.nan,
                         
                         #Data present within valid date
                         np.ones(len(alert)))
    
    #evaluating net displacements within real-time window
    alert['xz_disp']=np.round(xz_tilt.values[-1]-xz_tilt.values[0], 3)
    alert['xy_disp']=np.round(xy_tilt.values[-1]-xy_tilt.values[0], 3)

    #determining minimum and maximum displacement
    cond = np.asarray(np.abs(alert['xz_disp'].values)<np.abs(alert['xy_disp'].values))
    min_disp=np.round(np.where(cond,
                               np.abs(alert['xz_disp'].values),
                               np.abs(alert['xy_disp'].values)), 4)
    cond = np.asarray(np.abs(alert['xz_disp'].values)>=np.abs(alert['xy_disp'].values))
    max_disp=np.round(np.where(cond,
                               np.abs(alert['xz_disp'].values),
                               np.abs(alert['xy_disp'].values)), 4)

    #checking if displacement threshold is exceeded in either axis    
    cond = np.asarray((np.abs(alert['xz_disp'].values)>T_disp, np.abs(alert['xy_disp'].values)>T_disp))
    alert['disp_alert']=np.where(np.any(cond, axis=0),

                                 #disp alert=2
                                 np.where(min_disp/max_disp<k_ac_ax,
                                          np.zeros(len(alert)),
                                          np.ones(len(alert))),

                                 #disp alert=0
                                 np.zeros(len(alert)))
    
    #getting minimum axis velocity value
    alert['min_vel']=np.round(np.where(np.abs(xz_vel.values[-1])<np.abs(xy_vel.values[-1]),
                                       np.abs(xz_vel.values[-1]),
                                       np.abs(xy_vel.values[-1])), 4)

    #getting maximum axis velocity value
    alert['max_vel']=np.round(np.where(np.abs(xz_vel.values[-1])>=np.abs(xy_vel.values[-1]),
                                       np.abs(xz_vel.values[-1]),
                                       np.abs(xy_vel.values[-1])), 4)
                                       
    #checking if proportional velocity is present across node
    alert['vel_alert']=np.where(alert['min_vel'].values/alert['max_vel'].values<k_ac_ax,   

                                #vel alert=0
                                np.zeros(len(alert)),    

                                #checking if max node velocity exceeds threshold velocity for alert 1
                                np.where(alert['max_vel'].values<=T_velL2,                  

                                         #vel alert=0
                                         np.zeros(len(alert)),

                                         #checking if max node velocity exceeds threshold velocity for alert 2
                                         np.where(alert['max_vel'].values<=T_velL3,         

                                                  #vel alert=1
                                                  np.ones(len(alert)),

                                                  #vel alert=2
                                                  np.ones(len(alert))*2)))
    
    alert['node_alert']=np.where(alert['vel_alert'].values >= alert['disp_alert'].values,

                                 #node alert takes the higher perceive risk between vel alert and disp alert
                                 alert['vel_alert'].values,                                

                                 alert['disp_alert'].values)


    alert['disp_alert']=alert['ND']*alert['disp_alert']
    alert['vel_alert']=alert['ND']*alert['vel_alert']
    alert['node_alert']=alert['ND']*alert['node_alert']
    alert['ND']=alert['ND'].map({0:1,1:1})
    alert['ND']=alert['ND'].fillna(value=0)
    alert['disp_alert']=alert['disp_alert'].fillna(value=-1)
    alert['vel_alert']=alert['vel_alert'].fillna(value=-1)
    alert['node_alert']=alert['node_alert'].fillna(value=-1)

    #rearrange columns
    alert=alert.reset_index()
    cols=colarrange
    alert = alert[cols]
 
    return alert

def column_alert(alert, num_nodes_to_check, k_ac_ax):

#    print alert
    col_alert=[]
    col_node=[]
    #looping through each node
    for i in range(1,len(alert)+1):

        if alert['ND'].values[i-1]==0:
            col_node.append(i-1)
            col_alert.append(-1)
    
        #checking if current node alert is 2 or 3
        elif alert['node_alert'].values[i-1]!=0:
            
            #defining indices of adjacent nodes
            adj_node_ind=[]
            for s in range(1,int(num_nodes_to_check+1)):
                if i-s>0: adj_node_ind.append(i-s)
                if i+s<=len(alert): adj_node_ind.append(i+s)

            #looping through adjacent nodes to validate current node alert
            validity_check(adj_node_ind, alert, i, col_node, col_alert, k_ac_ax)
               
        else:
            col_node.append(i-1)
            col_alert.append(alert['node_alert'].values[i-1])
            
    alert['col_alert']=np.asarray(col_alert)

    alert['node_alert']=alert['node_alert'].map({-1:'ND',0:'L0',1:'L2',2:'L3'})
    alert['col_alert']=alert['col_alert'].map({-1:'ND',0:'L0',1:'L2',2:'L3'})

    return alert
    
def validity_check(adj_node_ind, alert, i, col_node, col_alert, k_ac_ax):                       

    adj_node_alert=[]
    for j in adj_node_ind:
        if alert['ND'].values[j-1]==0:
            adj_node_alert.append(-1)
        else:
            if alert['vel_alert'].values[i-1]!=0:
                #comparing current adjacent node velocity with current node velocity
                if abs(alert['max_vel'].values[j-1])>=abs(alert['max_vel'].values[i-1])*1/(2.**abs(i-j)):
                    #current adjacent node alert assumes value of current node alert
                    col_node.append(i-1)
                    col_alert.append(alert['node_alert'].values[i-1])
                    break
                    
                else:
                    adj_node_alert.append(0)
                    col_alert.append(max(getmode(adj_node_alert)))
                    break
                
            else:
                check_pl_cur=abs(alert['xz_disp'].values[i-1])>=abs(alert['xy_disp'].values[i-1])

                if check_pl_cur==True:
                    max_disp_cur=abs(alert['xz_disp'].values[i-1])
                    max_disp_adj=abs(alert['xz_disp'].values[j-1])
                else:
                    max_disp_cur=abs(alert['xy_disp'].values[i-1])
                    max_disp_adj=abs(alert['xy_disp'].values[j-1])        

                if max_disp_adj>=max_disp_cur*1/(2.**abs(i-j)):
                    #current adjacent node alert assumes value of current node alert
                    col_node.append(i-1)
                    col_alert.append(alert['node_alert'].values[i-1])
                    break
                    
                else:
                    adj_node_alert.append(0)
                    col_alert.append(max(getmode(adj_node_alert)))
                    break
                
        if j==adj_node_ind[-1]:
            col_alert.append(max(getmode(adj_node_alert)))
        
    return col_alert, col_node
    
def getmode(li):
    li.sort()
    numbers = {}
    for x in li:
        num = li.count(x)
        numbers[x] = num
    highest = max(numbers.values())
    n = []
    for m in numbers.keys():
        if numbers[m] == highest:
            n.append(m)
    return n    
    
def alert_generation(colname,xz,xy,vel_xz,vel_xy,num_nodes, T_disp, T_velL2, T_velL3, k_ac_ax,
                     num_nodes_to_check,end,proc_file_path,CSVFormat):
 
    #processing node-level alerts
    alert_out=node_alert(colname,xz,xy,vel_xz,vel_xy,num_nodes, T_disp, T_velL2, T_velL3, k_ac_ax,end)

#    print alert_out
    #processing column-level alerts
    alert_out=column_alert(alert_out, num_nodes_to_check, k_ac_ax)

    #adding 'ts' 
    alert_out['ts']=end
    
    #setting ts and node_ID as indices
    alert_out=alert_out.set_index(['ts','id'])

    
    return alert_out
                             
def generate_proc(colname, num_nodes, seg_len, custom_end,f=False,for_plots=False):
    
    #1. setting date boundaries for real-time monitoring window
#    roll_window_numpts=int(1+roll_window_length/data_dt)
    roll_window_numpts=int(1+roll_window_length/data_dt)
    end, start, offsetstart,monwin=get_rt_window(rt_window_length,roll_window_numpts,num_roll_window_ops,custom_end)

    # generating proc monitoring data for each site
    print "Generating PROC monitoring data for:-->> %s - %s <<--" %(str(colname),str(num_nodes))


    #3. getting accelerometer data for site 'colname'
    monitoring=qdb.GetRawAccelData(colname,offsetstart)
    if f:
        if for_plots:
            monitoring = ffd.filt(monitoring,keep_orig=True)
            return monitoring
        else:
            monitoring = ffd.filt(monitoring)

    else:
        monitoring = monitoring.loc[(monitoring.ts >= offsetstart) & (monitoring.ts <= end)]
     
    #3.1 identify the node ids with no data at start of monitoring window
    NodesNoInitVal=GetNodesWithNoInitialData(monitoring,num_nodes,offsetstart)
#    print NodesNoInitVal
    #4: get last good data prior to the monitoring window (LGDPM)
    lgdpm = pd.DataFrame()
    for node in NodesNoInitVal:
        temp = qdb.GetSingleLGDPM(colname, node, offsetstart.strftime("%Y-%m-%d %H:%M"))
        temp = fsd.applyFilters(temp)
        temp = temp.sort_index(ascending = False)[0:1]        
        lgdpm = lgdpm.append(temp,ignore_index=True)
 
    #5 TODO: Resample the dataframe together with the LGDOM
    monitoring=monitoring.append(lgdpm)

    #6. evaluating which data needs to be filtered
#    try:
    monitoring=fsd.applyFilters(monitoring)		
    LastGoodData=qdb.GetLastGoodData(monitoring,num_nodes)		
    qdb.PushLastGoodData(LastGoodData,colname)		
    LastGoodData = qdb.GetLastGoodDataFromDb(colname)		
    print 'Done'		
	
		
    if len(LastGoodData)<num_nodes: print colname, " Missing nodes in LastGoodData"		
		
    #5. extracting last data outside monitoring window		
    LastGoodData=LastGoodData[(LastGoodData.ts<offsetstart)]		
		
    #6. appending LastGoodData to monitoring		
    monitoring=monitoring.append(LastGoodData)    

    
    #7. replacing date of data outside monitoring window with first date of monitoring window
    monitoring.loc[monitoring.ts < offsetstart, ['ts']] = offsetstart

    #8. computing corresponding horizontal linear displacements (xz,xy), and appending as columns to dataframe
    monitoring['xz'],monitoring['xy']=accel_to_lin_xz_xy(seg_len,monitoring.x.values,monitoring.y.values,monitoring.z.values)
    
    #9. removing unnecessary columns x,y,z
    monitoring=monitoring.drop(['x','y','z'],axis=1)
    monitoring = monitoring.drop_duplicates(['ts', 'id'])

    #10. setting ts as index
    monitoring=monitoring.set_index('ts')

    #11. reordering columns
    monitoring=monitoring[['id','xz','xy']]
    
    return monitoring,monwin

def time_site(target,df_sa):
    if (target < len(df_sa)):
        site = df_sa['site'].iloc[target]
        t_time = df_sa.index[target]
        return site,t_time
    else:
        print "Error. Target > len(df_sa)"
        

io = cfg.config()
num_roll_window_ops = io.io.num_roll_window_ops
roll_window_length = io.io.roll_window_length
data_dt = io.io.data_dt
rt_window_length = io.io.rt_window_length

roll_window_numpts=int(1+roll_window_length/data_dt)

col_pos_interval = io.io.col_pos_interval
col_pos_num = io.io.num_col_pos
to_fill = io.io.to_fill
to_smooth = io.io.to_smooth
output_path = (__file__)
output_file_path = (__file__)
proc_file_path = (__file__)
CSVFormat = '.csv'
PrintProc = io.io.printproc

T_disp = io.io.t_disp
T_velL2 = io.io.t_vell2 
T_velL3 = io.io.t_vell3
k_ac_ax = io.io.k_ac_ax
num_nodes_to_check = io.io.num_nodes_to_check
colarrange = io.io.alerteval_colarrange.split(',')
summary = pd.DataFrame()
node_status = qdb.GetNodeStatus(1)

last_target = 5
for i in range(0,last_target):
    try:
        sites,custom_end = ffd.aim(i)
        sensorlist = qdb.GetSensorList(sites)
        for s in sensorlist:
        
            last_col=sensorlist[-1:]
            last_col=last_col[0]
            last_col=last_col.name
            
            # getting current column properties
            colname,num_nodes,seg_len= s.name,s.nos,s.seglen
        
            # list of working nodes     
            node_list = range(1, num_nodes + 1)
            not_working = node_status.loc[(node_status.site == colname) & (node_status.node <= num_nodes)]
            not_working_nodes = not_working['node'].values  
            for i in not_working_nodes:
                node_list.remove(i)
        
            proc_monitoring,monwin=generate_proc(colname, num_nodes, seg_len, custom_end)    
            
            xz_series_list,xy_series_list = create_series_list(proc_monitoring,monwin,colname,num_nodes)
#            print "create_series_list tapos na"
            # create, fill and smooth dataframes from series lists
            xz=create_fill_smooth_df(xz_series_list,num_nodes,monwin, roll_window_numpts,to_fill,to_smooth)
            xy=create_fill_smooth_df(xy_series_list,num_nodes,monwin, roll_window_numpts,to_fill,to_smooth)
            
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
            alert_out=alert_generation(colname,xz,xy,vel_xz,vel_xy,num_nodes, T_disp, T_velL2, T_velL3, k_ac_ax,
                                       num_nodes_to_check,custom_end,proc_file_path,CSVFormat)

        alert_out = alert_out.reset_index(level = ['id'])
        alert_out = alert_out[['id','disp_alert','vel_alert','node_alert','col_alert']]
        alert_out = alert_out[(alert_out['vel_alert'] > 0 ) | (alert_out.node_alert == 'l2')]
        alert_out = alert_out[alert_out.id == 1]
        alert_out['site'] = sites
        summary = pd.concat((summary,alert_out),axis = 0)
    except:
        print "Error."
        continue
print "--------------------Filtering chenes----------------------"
print "--------------------Store yung mga nafilter----------------------"
s_f = pd.DataFrame()
s_a = pd.DataFrame()
for j in range(0,len(summary)):
    try:
        sites,custom_end = time_site(j,summary)
        sensorlist = qdb.GetSensorList(sites)
        for s in sensorlist:
        
            last_col=sensorlist[-1:]
            last_col=last_col[0]
            last_col=last_col.name
            
            # getting current column properties
            colname,num_nodes,seg_len= s.name,s.nos,s.seglen
        
            # list of working nodes     
            node_list = range(1, num_nodes + 1)
            not_working = node_status.loc[(node_status.site == colname) & (node_status.node <= num_nodes)]
            not_working_nodes = not_working['node'].values  
            for i in not_working_nodes:
                node_list.remove(i)
        
            proc_monitoring,monwin=generate_proc(colname, num_nodes, seg_len, custom_end,f=True)
            
            
            xz_series_list,xy_series_list = create_series_list(proc_monitoring,monwin,colname,num_nodes)
    
            xz=create_fill_smooth_df(xz_series_list,num_nodes,monwin, roll_window_numpts,to_fill,to_smooth)
            xy=create_fill_smooth_df(xy_series_list,num_nodes,monwin, roll_window_numpts,to_fill,to_smooth)
            
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
            alert_out=alert_generation(colname,xz,xy,vel_xz,vel_xy,num_nodes, T_disp, T_velL2, T_velL3, k_ac_ax,
                                       num_nodes_to_check,custom_end,proc_file_path,CSVFormat)
        #    print alert_out
            
        
        alert_out = alert_out.reset_index(level = ['id'])
        a_out = alert_out.copy()
        
        a_out = a_out[['id','disp_alert','vel_alert','node_alert','col_alert']]
        a_out = a_out[(a_out['vel_alert'] < 1.0 ) | (a_out.node_alert == 'l0')]
        a_out = a_out[a_out.id == 1]
        a_out['site'] = sites
        s_f = pd.concat((s_f,a_out),axis = 0)
        
        b_out = alert_out.copy()
        b_out = b_out[['id','disp_alert','vel_alert','node_alert','col_alert']]
        b_out = b_out[(b_out['vel_alert'] > 0.0 ) | (b_out.node_alert == 'l2')]
        b_out = b_out[b_out.id == 1]
        b_out['site'] = sites
        s_a = pd.concat((s_a,b_out),axis = 0)
    except:
        print "Error."
        continue

print "################# Drawing! Dahil drawing ka! ##################"
print "################# Idrawing lahat ng nafilter! ##################"

for k in range(0,len(s_f)):
    try:
        sites,custom_end = time_site(k,s_f)
        ce =  custom_end.strftime("%y_%m_%d__%H_%M")
        fname = "FILTERED_" +str(sites) + "_" + ce + "_015_035"
        sensorlist = qdb.GetSensorList(sites)
        
        for s in sensorlist:
            last_col=sensorlist[-1:]
            last_col=last_col[0]
            last_col=last_col.name
            
            # getting current column properties
            colname,num_nodes,seg_len= s.name,s.nos,s.seglen
        
            # list of working nodes     
            node_list = range(1, num_nodes + 1)
            not_working = node_status.loc[(node_status.site == colname) & (node_status.node <= num_nodes)]
            not_working_nodes = not_working['node'].values  
            for i in not_working_nodes:
                node_list.remove(i)
        
            # importing proc_monitoring file of current column to dataframe
        #    try:
        #            print "proc_monitoring here: "
            proc_monitoring=generate_proc(colname, num_nodes, seg_len, custom_end,f=True,for_plots=True)
        #    print proc_monitoring
            proc_monitoring = proc_monitoring[proc_monitoring.id == 1]
            ffd.plotter(proc_monitoring,fname=fname)
    except:
        print "Error plotting Filtered."
    
for k in range(0,len(s_a)):
    try:
        sites,custom_end = time_site(k,s_a)
        ce =  custom_end.strftime("%y_%m_%d__%H_%M")
        fname = "ALARMS_" +str(sites) + "_" + ce + "_015_035"
        
        sensorlist = qdb.GetSensorList(sites)
        for s in sensorlist:
            
            last_col=sensorlist[-1:]
            last_col=last_col[0]
            last_col=last_col.name
            
            # getting current column properties
            colname,num_nodes,seg_len= s.name,s.nos,s.seglen
        
            # list of working nodes     
            node_list = range(1, num_nodes + 1)
            not_working = node_status.loc[(node_status.site == colname) & (node_status.node <= num_nodes)]
            not_working_nodes = not_working['node'].values  
            for i in not_working_nodes:
                node_list.remove(i)
        
            # importing proc_monitoring file of current column to dataframe
        #    try:
        #            print "proc_monitoring here: "
            proc_monitoring=generate_proc(colname, num_nodes, seg_len, custom_end,f=True,for_plots=True)
        #    print proc_monitoring
            proc_monitoring = proc_monitoring[proc_monitoring.id == 1]
            ffd.plotter(proc_monitoring,fname=fname)
    except:
        print "Error plotting Alarms."      