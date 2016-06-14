##### IMPORTANT matplotlib declarations must always be FIRST to make sure that matplotlib works with cron-based automation
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as md
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
import time

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

import errorAnalysis as err

#Generate Last Good Data Table if it doesn't exist yet
lgdExistence = DoesTableExist("lastgooddata")
if lgdExistence == False:
    print "Generate Last Good Data Table"
    GenerateLastGoodData()

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
    
def fill_smooth_df(proc_monitoring, offsetstart, end, roll_window_numpts, to_smooth):

    ##DESCRIPTION:
    ##returns filled and smoothened xz and xy within monitoring window

    ##INPUT:
    ##proc_monitoring; dataframe; index: ts, columns: [id, xz, xy]
    ##num_dodes; integer; number of nodes
    ##monwin; monitoring window dataframe
    ##roll_window_numpts; integer; number of data points per rolling window
    ##to_fill; filling NAN values
    ##to_smooth; smoothing dataframes with moving average

    ##OUTPUT:
    ##proc_monitoring; dataframe; index: ts, columns: [id, filled and smoothened (fs) xz, fs xy]

    #filling NAN values
    NodesWithVal = list(set(proc_monitoring.dropna().id.values))
    blank_df = pd.DataFrame({'ts': [offsetstart]*len(NodesWithVal)+[end]*len(NodesWithVal), 'id': NodesWithVal*2}).set_index('ts')
    proc_monitoring = proc_monitoring.append(blank_df)
    proc_monitoring = proc_monitoring.reset_index().drop_duplicates(['ts','id'], keep = 'first').set_index('ts')
    proc_monitoring.index = pd.to_datetime(proc_monitoring.index)
    proc_monitoring = proc_monitoring.resample('30Min', base=0, how='pad')
    proc_monitoring = proc_monitoring.fillna(method='pad')
    proc_monitoring = proc_monitoring.fillna(method='bfill')
 
    #dropping rows outside monitoring window
    proc_monitoring = proc_monitoring[(proc_monitoring.index>=offsetstart)&(proc_monitoring.index<=end)]
    
    if to_smooth:
        #smoothing dataframes with moving average
        proc_monitoring=pd.rolling_mean(proc_monitoring,window=roll_window_numpts)[roll_window_numpts-1:]

    return np.round(proc_monitoring, 4)

def node_inst_vel(filled_smoothened, roll_window_numpts, start):

    try:          
        lr_xz=ols(y=filled_smoothened.xz,x=filled_smoothened.td,window=roll_window_numpts,intercept=True)
        lr_xy=ols(y=filled_smoothened.xy,x=filled_smoothened.td,window=roll_window_numpts,intercept=True)
                
        filled_smoothened = filled_smoothened.loc[filled_smoothened.ts >= (start-timedelta(hours=0.5))]
                   
        filled_smoothened['vel_xz'] = np.round(lr_xz.beta.x.values,4)
        filled_smoothened['vel_xy'] = np.round(lr_xy.beta.x.values,4)
    
    except:
        print " ERROR in computing velocity"
        filled_smoothened['vel_xz'] = np.zeros(len(filled_smoothened))
        filled_smoothened['vel_xy'] = np.zeros(len(filled_smoothened))
    
    return filled_smoothened    

def col_pos(colpos_dfts, col_pos_end, col_pos_interval, col_pos_number, num_nodes):
    
    cumsum_df = colpos_dfts.loc[colpos_dfts.ts == colpos_dfts.ts.values[0]][['xz','xy']].cumsum()
    colpos_dfts['cs_xz'] = cumsum_df.xz.values
    colpos_dfts['cs_xy'] = cumsum_df.xy.values
    
    return np.round(colpos_dfts, 4)

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

def alert_generation(disp_vel,colname,num_nodes,T_disp, T_velL2, T_velL3, k_ac_ax,
                      num_nodes_to_check,end,proc_file_path,CSVFormat):

    # displacement and velocity at start and end of interval
    disp_vel = disp_vel.loc[(disp_vel.ts == start) | (disp_vel.ts == end)]
    
    LastGoodData= GetLastGoodDataFromDb(colname)
    
    #NEED TO GROUPBY ID THEN PROCESS NODE ALERT
    nodal_disp_vel = disp_vel.groupby('id')
    
    alert_out = nodal_disp_vel.apply(alert.node_alert2, colname=colname, num_nodes=num_nodes, T_disp=T_disp, T_velL2=T_velL2, T_velL3=T_velL3, k_ac_ax=k_ac_ax, lastgooddata=LastGoodData)
    
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

def alert_summary(alert_out,alert_list, colname, num_nodes):

    ##DESCRIPTION:
    ##creates list of sites per alert level

    ##INPUT:
    ##alert_out; array
    ##alert_list; array
    
    ND_check=alert_out.loc[(alert_out['node_alert']=='ND')|(alert_out['col_alert']=='ND')]
    if len(ND_check)>(num_nodes/2):
        ND_alert.append(colname)
        
    else:
        L3_check=alert_out.loc[(alert_out['node_alert']=='L3')|(alert_out['col_alert']=='L3')]
        L2_check=alert_out.loc[(alert_out['node_alert']=='L2')|(alert_out['col_alert']=='L2')]
        L0_check=alert_out.loc[(alert_out['node_alert']=='L0')]
        checklist=[L3_check,L2_check,L0_check]
        
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
    
def subplot_column_positions(cumsum_dfts, ax_xz, ax_xy):
    #current column position x
    curcolpos_x = cumsum_dfts.x.values
    
    #current column position xz
    curax = ax_xz
    curcolpos_xz = cumsum_dfts.cs_xz.values
    curax.plot(curcolpos_xz,curcolpos_x,'.-')
    curax.set_xlabel('xz')
    curax.set_ylabel('x')
    
    #current column position xy
    curax = ax_xy
    curcolpos_xy = cumsum_dfts.cs_xy.values
    curax.plot(curcolpos_xy,curcolpos_x,'.-', label=str(pd.to_datetime(cumsum_dfts.ts.values[0])))
    curax.set_xlabel('xy')
    return
    
def plot_column_positions(cumsum_df,colname,end):
    try:
        fig=plt.figure()
        ax_xz=fig.add_subplot(121)
        ax_xy=fig.add_subplot(122,sharex=ax_xz,sharey=ax_xz)

        ax_xz=nonrepeat_colors(ax_xz,len(set(cumsum_df.ts.values)))
        ax_xy=nonrepeat_colors(ax_xy,len(set(cumsum_df.ts.values)))

        cumsum_dfts = cumsum_df.groupby('ts')
        cumsum_dfts.apply(subplot_column_positions, ax_xz=ax_xz, ax_xy=ax_xy)
    
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
        fig.subplots_adjust(top=0.9)        
        fig.suptitle(colname+" as of "+str(end),fontsize='medium')

        plt.legend(fontsize='x-small')        

    except:        
        print colname, "ERROR in plotting column position"
    return
        
def plot_disp_vel(disp_vel_0off, colname, end):

    ##DESCRIPTION:
    ##returns plot of xz and xy displacements per node, xz and xy velocities per node

    ##INPUT:
    ##xz; array of floats; horizontal linear displacements along the planes defined by xa-za
    ##xy; array of floats; horizontal linear displacements along the planes defined by xa-ya
    ##xz_vel; array of floats; velocity along the planes defined by xa-za
    ##xy_vel; array of floats; velocity along the planes defined by xa-ya

    try:
        fig=plt.figure()

        ax_xzd=fig.add_subplot(141)
        ax_xyd=fig.add_subplot(142,sharex=ax_xzd,sharey=ax_xzd)
    
        ax_xzv=fig.add_subplot(143)
        ax_xyv=fig.add_subplot(144,sharex=ax_xzv,sharey=ax_xzv)
        
        ax_xzd=nonrepeat_colors(ax_xzd,len(set(disp_vel_0off.id.values)))
        ax_xyd=nonrepeat_colors(ax_xyd,len(set(disp_vel_0off.id.values)))
        ax_xzv=nonrepeat_colors(ax_xzv,len(set(disp_vel_0off.id.values)))
        ax_xyv=nonrepeat_colors(ax_xyv,len(set(disp_vel_0off.id.values)))
        
        disp_vel_0off = disp_vel_0off.sort('ts', ascending = True).set_index('ts')
        nodal_disp_vel = disp_vel_0off.groupby('id')
    
        curax=ax_xzd
        plt.sca(curax)
        nodal_disp_vel['xz_0off'].apply(plt.plot)
        curax.set_title('3-day disp\n XZ axis',fontsize='x-small')
        curax.set_ylabel('displacement scale, m', fontsize='x-small')
        i = disp_vel_0off.loc[disp_vel_0off.index == disp_vel_0off.index[0]].id.values
        y = disp_vel_0off.loc[disp_vel_0off.index == disp_vel_0off.index[0]].xz_0off.values
        for a,b in zip(i,y):
            curax.annotate(str(a), xy=(disp_vel_0off.index[0],b), xytext = (5,-2.5), textcoords='offset points',size = 'x-small')
        
        curax=ax_xyd
        plt.sca(curax)
        nodal_disp_vel['xy_0off'].apply(plt.plot)
        curax.set_title('3-day disp\n XY axis',fontsize='x-small')
        y = disp_vel_0off.loc[disp_vel_0off.index == disp_vel_0off.index[0]].xy_0off.values
        for a,b in zip(i,y):
            curax.annotate(str(a), xy=(disp_vel_0off.index[0],b), xytext = (5,-2.5), textcoords='offset points',size = 'x-small')

        
        disp_vel_0off = disp_vel_0off.loc[disp_vel_0off.index >= end - timedelta(hours = 3)]
        nodal_disp_vel = disp_vel_0off.groupby('id')
        
        curax=ax_xzv
        plt.sca(curax)
        nodal_disp_vel['vel_xz_0off'].apply(plt.plot)
        curax.set_title('3-hr vel alerts\n XZ axis',fontsize='x-small')
        i = disp_vel_0off.loc[disp_vel_0off.index == disp_vel_0off.index[0]].id.values
        y = disp_vel_0off.loc[disp_vel_0off.index == disp_vel_0off.index[0]].vel_xz_0off.values
        for a,b in zip(i,y):
            curax.annotate(str(a), xy=(disp_vel_0off.index[0],b), xytext = (5,-2.5), textcoords='offset points',size = 'x-small')

        
        curax=ax_xyv
        plt.sca(curax)
        nodal_disp_vel['vel_xy_0off'].apply(plt.plot)
        curax.set_title('3-hr vel alerts\n XY axis',fontsize='x-small')
        y = disp_vel_0off.loc[disp_vel_0off.index == disp_vel_0off.index[0]].vel_xy_0off.values
        for a,b in zip(i,y):
            curax.annotate(str(a), xy=(disp_vel_0off.index[0],b), xytext = (5,-2.5), textcoords='offset points',size = 'x-small')

        fig.subplots_adjust(hspace=0)
        plt.setp([a.get_yticklabels() for a in fig.axes[1:4]], visible=False)
        
        # rotating xlabel
        
        for tick in ax_xzd.xaxis.get_major_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(6)
            
        for tick in ax_xyd.xaxis.get_major_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(6)
    
        for tick in ax_xzv.xaxis.get_major_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(8)
    
        for tick in ax_xyv.xaxis.get_major_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(8)
                
        for item in ([ax_xzd.xaxis.label, ax_xzv.yaxis.label, ax_xyd.xaxis.label, ax_xyv.yaxis.label]):
            item.set_fontsize(8)
            
        dfmt = md.DateFormatter('%m-%d\n%H:%M')
        ax_xzd.xaxis.set_major_formatter(dfmt)
        ax_xyd.xaxis.set_major_formatter(dfmt)
        vfmt = md.DateFormatter('%H:%M')
        ax_xzv.xaxis.set_major_formatter(vfmt)
        ax_xyv.xaxis.set_major_formatter(vfmt)
                
        fig.tight_layout()

        fig.subplots_adjust(top=0.85)
        fig.suptitle(colname+" as of "+str(end),fontsize='small')
        
    except:      
        print colname, "ERROR in plotting displacements and velocities"
    return   

def zero_initial_cumsum(input_df):
    input_df = input_df.sort('ts', ascending = True)
    input_df['cs_xz_0'] = input_df['cs_xz'].apply(lambda x: x - input_df.cs_xz.values[0])
    input_df['cs_xy_0'] = input_df['cs_xy'].apply(lambda x: x - input_df.cs_xy.values[0])
    return np.round(input_df, 4)
    
def offsetzero_initial_dispvel(input_df, offset, num_nodes):
    input_df = input_df.sort('ts', ascending = True)
    input_df['xz_0'] = input_df['xz'].apply(lambda x: x - input_df.xz.values[0])
    input_df['xy_0'] = input_df['xy'].apply(lambda x: x - input_df.xy.values[0])
    input_df['vel_xz_0'] = input_df['vel_xz'].apply(lambda x: x - input_df.vel_xz.values[0])
    input_df['vel_xy_0'] = input_df['vel_xy'].apply(lambda x: x - input_df.vel_xy.values[0])
    node_id = input_df.id.values[0]
    input_df['xz_0off'] = input_df['xz_0'].apply(lambda x: x + ((num_nodes - node_id) * offset) )
    input_df['xy_0off'] = input_df['xy_0'].apply(lambda x: x + ((num_nodes - node_id) * offset) )
    input_df['vel_xz_0off'] = input_df['vel_xz_0'].apply(lambda x: x + ((num_nodes - node_id) * offset) )
    input_df['vel_xy_0off'] = input_df['vel_xy_0'].apply(lambda x: x + ((num_nodes - node_id) * offset) )
    return np.round(input_df, 4)

def IntegratedAlert(site_col_props):
    
    # getting current column properties
    colname = site_col_props.name
    num_nodes = int(site_col_props.nos.values[0])
    seg_len = float(site_col_props.seglen.values[0])
    
    print "colname, num_nodes, seg_len", colname, num_nodes, seg_len
    
    # list of working nodes     
    node_list = range(1, num_nodes + 1)
    not_working = node_status.loc[(node_status.site == colname) & (node_status.node <= num_nodes)]
    not_working_nodes = not_working['node'].values
    for i in not_working_nodes:
        node_list.remove(i)

    # importing proc_monitoring file of current column to dataframe
    try:
        proc_monitoring=genproc.generate_proc(colname, num_nodes, seg_len)
        proc_monitoring.sort_index(ascending = True, inplace = True)
        print proc_monitoring
    except:
        print "     ",colname, "ERROR...missing/empty proc monitoring"
        proc_monitoring = pd.DataFrame({'ts': [offsetstart]*num_nodes, 'id': range(1, num_nodes + 1),
                                        'xz': [0.0]*num_nodes, 'xy': [0.0]*num_nodes}).set_index('ts')

    nodes_with_val = set(proc_monitoring.dropna().id.values)
    all_nodes = set(range(1, num_nodes+1))
    no_val_nodes = list(all_nodes - nodes_with_val)
    
    node_fill = pd.DataFrame({'ts': [offsetstart]*len(no_val_nodes), 'id': no_val_nodes, 
                              'xz': [0]*len(no_val_nodes), 'xy': [0]*len(no_val_nodes)}).set_index('ts')

    proc_monitoring = proc_monitoring.append(node_fill)
    
    nodal_proc_monitoring = proc_monitoring.groupby('id')
    
    # fill and smoothen displacement
    filled_smoothened = nodal_proc_monitoring.apply(fill_smooth_df, offsetstart=offsetstart, end=end, roll_window_numpts=roll_window_numpts, to_smooth=to_smooth)
    filled_smoothened = filled_smoothened[['xz', 'xy']].reset_index()
    filled_smoothened['td'] = filled_smoothened.ts.values - filled_smoothened.ts.values[0]
    filled_smoothened['td'] = filled_smoothened['td'].apply(lambda x: x / np.timedelta64(1,'D'))

    nodal_filled_smoothened = filled_smoothened.groupby('id')    
    
    # xz and xy displacements, xz and xy velocities within monitoring window
    disp_vel = nodal_filled_smoothened.apply(node_inst_vel, roll_window_numpts=roll_window_numpts, start=start)
    disp_vel = disp_vel[['ts', 'xz', 'xy', 'vel_xz', 'vel_xy']].reset_index()
    disp_vel = disp_vel[['ts', 'id', 'xz', 'xy', 'vel_xz', 'vel_xy']]
    disp_vel = disp_vel.sort('ts', ascending=True)
    
    # absolute column position for col_pos_num with col_pos_interval
    colposdates = pd.date_range(end=end, freq=col_pos_interval,periods=col_pos_num, name='ts',closed=None)
    colpos_df = pd.DataFrame({'ts': colposdates, 'id': [num_nodes+1]*len(colposdates), 'xz': [0]*len(colposdates), 'xy': [0]*len(colposdates)})
    for colpos_ts in colposdates:
        colpos_df = colpos_df.append(disp_vel.loc[disp_vel.ts == colpos_ts, ['ts', 'id', 'xz', 'xy']])
    colpos_df['x'] = colpos_df['id'].apply(lambda x: (num_nodes + 1 - x) * seg_len)
    colpos_df = colpos_df.sort('id', ascending = False)

    colpos_dfts = colpos_df.groupby('ts')
    
    # relative column position
    cumsum_df = colpos_dfts.apply(col_pos, col_pos_end=end, col_pos_interval=col_pos_interval, col_pos_number=col_pos_num, num_nodes=num_nodes)
    
    # relative column position with zeroed initial
    nodal_cumsum_df = cumsum_df.groupby('id')
    cumsum_df_zeroed = nodal_cumsum_df.apply(zero_initial_cumsum)
    cumsum_df_zeroed = cumsum_df_zeroed[['ts','xz','xy','x','cs_xz','cs_xy','cs_xz_0','cs_xy_0']].reset_index()
    cumsum_df_zeroed = cumsum_df_zeroed[['ts','id','xz','xy','x','cs_xz','cs_xy','cs_xz_0','cs_xy_0']]
    
    # displacements and velocity with zeroed initial
    nodal_disp_vel = disp_vel.groupby('id')
    disp_vel_0off = nodal_disp_vel.apply(offsetzero_initial_dispvel, num_nodes=num_nodes, offset=0.15)
    disp_vel_0off = disp_vel_0off[['ts','xz','xy','vel_xz','vel_xy','xz_0','xy_0','xz_0off','xy_0off','vel_xz_0','vel_xy_0','vel_xz_0off','vel_xy_0off']].reset_index()
    disp_vel_0off = disp_vel_0off[['ts','id','xz','xy','vel_xz','vel_xy','xz_0','xy_0','xz_0off','xy_0off','vel_xz_0','vel_xy_0','vel_xz_0off','vel_xy_0off']]
    
    # Alert generation
    alert_out = alert_generation(disp_vel,colname,num_nodes, T_disp, T_velL2, T_velL3, k_ac_ax,
                               num_nodes_to_check,end,proc_file_path,CSVFormat)        
                                                                                                                   
    print alert_out
    
    # without trending_node_alert (col_alerts to trending column)
    trending_col_alerts = []
    node_list = list(set(node_list) - (set(node_list) - set(filled_smoothened.id.values)))
    
    for n in node_list:
        trending_col_alerts += [pd.Series.tolist(alert_out.col_alert)[n-1]]
    
    # TRENDING COLUMN ALERT ONLY
    if trending_col_alerts.count('L3') != 0:
        if PrintTAlert:
            with open (output_file_path+textalert, 'ab') as t:
                t.write (colname + ":" + 'L3' + '\n')
        L3_alert.append(colname)
        alert_df.append((end,colname,'L3'))                
    elif trending_col_alerts.count('L2') != 0:
        if PrintTAlert:
            with open (output_file_path+textalert, 'ab') as t:
                t.write (colname + ":" + 'L2' + '\n')
        L2_alert.append(colname)
        alert_df.append((end,colname,'L2'))
    else:
        trending_col_alerts_count = Counter(trending_col_alerts)  
        if PrintTAlert:
            with open (output_file_path+textalert, 'ab') as t:
                t.write (colname + ":" + (trending_col_alerts_count.most_common(1)[0][0]) + '\n')
        if (trending_col_alerts_count.most_common(1)[0][0] == 'L0'):
            L0_alert.append(colname)
            alert_df.append((end,colname,'L0'))
        else:
            ND_alert.append(colname)
            alert_df.append((end,colname,'ND'))
    
    # writes sensor alerts in one row in webtrends
    if PrintWAlert:
        with open(output_file_path+webtrends, 'ab') as w:
                if trending_col_alerts.count('L3') != 0:
                    w.write ('L3' + ',')
                elif trending_col_alerts.count('L2') != 0:
                    w.write ('L2' + ',')
                elif (colname == 'sinb') or (colname == 'blcb'):
                    if trending_col_alerts.count('L0') > 0:
                        w.write ('L0' + ',')
                    else:
                        w.write ('ND' + ',')       
                else:
                    trending_col_alerts = Counter(trending_col_alerts)  
                    w.write ((trending_col_alerts.most_common(1)[0][0]) + ',')
                                
                if colname == last_col:
                           w.seek(-1, os.SEEK_END)
                           w.truncate()
                           w.write('\n')
    
    print alert_out
  
#    prints to csv: node alert, column alert and trending alert of sites with nd alert
    if PrintND:
        for colname in ND_alert:
            if os.path.exists(ND_path + colname + CSVFormat):
                alert_out[['node_alert', 'col_alert', 'trending_alert']].to_csv(ND_path + colname + CSVFormat, sep=',', header=False, mode='a')
            else:
                alert_out[['node_alert', 'col_alert', 'trending_alert']].to_csv(ND_path + colname + CSVFormat, sep=',', header=True, mode='w')

    #11. Plotting column positions
    if PrintColPos:
        plot_column_positions(cumsum_df,colname,end)
        plt.savefig(output_file_path+colname+'ColPos',
                    dpi=160, facecolor='w', edgecolor='w',orientation='landscape',mode='w')

    #12. Plotting displacement and velocity
    if PrintDispVel:
        plot_disp_vel(disp_vel_0off, colname, end)
        plt.savefig(output_file_path+colname+'Disp_vel',
                    dpi=160, facecolor='w', edgecolor='w',orientation='landscape',mode='w')
                    
    output = pd.DataFrame({'alert_df': [alert_df], 'ND_alert': [ND_alert], 'L0_alert': [L0_alert], 'L2_alert': [L2_alert], 'L3_alert': [L3_alert]})

    return output

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

ND_path = output_path + cfg.get('I/O', 'NDFilePath')
output_file_path = output_path + cfg.get('I/O','OutputFilePath')
proc_file_path = output_path + cfg.get('I/O','ProcFilePath')
ColAlerts_file_path = output_path + cfg.get('I/O','ColAlertsFilePath')
TrendAlerts_file_path = output_path + cfg.get('I/O','TrendAlertsFilePath')

#Create filepaths if it does not exists
def create_dir(p):
    if not os.path.exists(p):
        os.makedirs(p)

directories = [ND_path,output_file_path,proc_file_path,ColAlerts_file_path,TrendAlerts_file_path]
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

#Create webtrends.csv if it does not exists

files = [webtrends,textalert,textalert2,rainfall_alert,all_alerts,gsm_alert,eq_summary,eq_summaryGSM,timer]

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

to_fill = cfg.getint('I/O','to_fill')
to_smooth = cfg.getint('I/O','to_smooth')

with_TrendingNodeAlert = cfg.getboolean('I/O','with_TrendingNodeAlert')
test_specific_sites = cfg.getboolean('I/O','test_specific_sites')
test_sites = cfg.get('I/O','test_sites').split(',')



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


#MAIN

#Set as true if printing by JSON would be done
set_json = False


# setting monitoring window
roll_window_numpts, end, start, offsetstart, monwin = set_monitoring_window(roll_window_length,data_dt,rt_window_length,num_roll_window_ops)

# creating summary of alerts
ND_alert=[]
L0_alert=[]
L2_alert=[]
L3_alert=[]
alert_df = []
alert_list=[L3_alert,L2_alert,L0_alert,ND_alert]
alert_names=['L3: ','L2: ','L0: ','ND: ']

print "Generating plots and alerts for:"

names = ['ts','col_a']
fmt = '%Y-%m-%d %H:%M'
hr = end - timedelta(hours=3)

with open(output_file_path+webtrends, 'ab') as w, open (output_file_path+textalert, 'wb') as t:
    t.write('As of ' + end.strftime(fmt) + ':\n')
    w.write(end.strftime(fmt) + ';')

# getting list of sensors
sensorlist = GetSensorList()

# dataframe containing site column properties
col_props = pd.DataFrame(data=None)
name = []
nos = []
seglen = []
for s in sensorlist:
    name += [s.name]
    nos += [s.nos]
    seglen += [s.seglen]
col_props['name'] = name
col_props['nos'] = nos
col_props['seglen'] = seglen

col_props = col_props
site_col_props = col_props.groupby('name')

last_col = col_props.name.values[-1]

# list of non-working nodes
node_status = GetNodeStatus(1)

site_col_prop = col_props.groupby('name')

output = site_col_props.apply(IntegratedAlert)

alert_df = output.alert_df.values[0]
ND_alert = output.ND_alert.values[0]
L0_alert = output.L0_alert.values[0]
L2_alert = output.L2_alert.values[0]
L3_alert = output.L3_alert.values[0]

# writes list of site per alert level in textalert2
if PrintTAlert2:
    with open (output_file_path+textalert2, 'wb') as t:
        t.write('As of ' + end.strftime(fmt) + ':\n')
        t.write ('L0: ' + ','.join(sorted(L0_alert)) + '\n')
        t.write ('ND: ' + ','.join(sorted(ND_alert)) + '\n')
        t.write ('L2: ' + ','.join(sorted(L2_alert)) + '\n')
        t.write ('L3: ' + ','.join(sorted(L3_alert)) + '\n')


#Prints rainfall alerts, text alert and eq summary in one file
if PrintAAlert:
    with open(output_file_path+all_alerts, 'wb') as allalerts:
        allalerts.write('As of ' + end.strftime(fmt) + ':\n')
        allalerts.write('L3: ' + ','.join(sorted(L3_alert)) + '\n')
        allalerts.write('L2: ' + ','.join(sorted(L2_alert)) + '\n')
        allalerts.write('\n')
        with open(output_file_path+rainfall_alert) as rainfallalert:
            n = 0
            for line in rainfallalert:
                if n == 0 or n == 3 or n == 4:
                    allalerts.write(line)
                n += 1
            allalerts.write('\n')
    
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

# records the number of minutes the code runs
end_time = datetime.now() - start_time
if PrintTimer:
    with open (output_file_path+timer, 'ab') as p:
        p.write (start_time.strftime(fmt) + ": " + str(end_time) + '\n')
print 'run time =', str(end_time)