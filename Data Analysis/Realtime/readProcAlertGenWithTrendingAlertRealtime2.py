##### IMPORTANT matplotlib declarations must always be FIRST to make sure that matplotlib works with cron-based automation
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
plt.ion()

import os
from datetime import datetime, timedelta
import pandas as pd
from pandas.stats.api import ols
import numpy as np
import ConfigParser
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

import errorAnalysis as err

#Generate Last Good Data Table if it doesn't exist yet
lgdExistence = DoesTableExist("lastgooddata")
if lgdExistence == False:
#    print "Generate Last Good Data Table"
    GenerateLastGoodData()

def set_monitoring_window(roll_window_length,data_dt,rt_window_length,num_roll_window_ops,end):
    
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
    end, start, offsetstart=gf.get_rt_window(rt_window_length,roll_window_numpts,num_roll_window_ops,end)
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
  #==============================================================================
#     DESCRIPTION:
#     returns rounded-off values within monitoring window
# 
#     INPUT:
#     series_list
#     num_dodes; integer; number of nodes
#     monwin; monitoring window dataframe
#     roll_window_numpts; integer; number of data points per rolling window
#     to_fill; filling NAN values
#     to_smooth; smoothing dataframes with moving average
# 
#     OUTPUT:
#     np.round(df[(df.index>=monwin.index[0])&(df.index<=monwin.index[-1])],4)
#==============================================================================
    
    #concatenating series list into dataframe
    df=pd.concat(series_list, axis=1, join='outer', names=None)
    
    #renaming columns
    df.columns=[a for a in np.arange(0,1+num_nodes)]

    #dropping column "monwin" from df
    df=df.drop(0,1)
    
    hasRawValue=df.notnull()
    
   
    
    if to_fill:
        #filling NAN values
        df=df.fillna(method='pad')
        
        #Checking, resolving and reporting fill process    
        print 'Post-filling report:'
        if df.isnull().values.any():
            for n in df.columns:
                if df[n].isnull().values.all():
                    print '     ',n, 'NaN all values'
                    df[n]=0
                elif np.isnan(df[n].values[0]):
                    print '     ',n, 'NaN 1st value'
                    df[n]=df[n].fillna(method='bfill')
                else:
                    continue
#                    print '     ',n, 'NaN some values'
        else: 
            print '     All numerical values.'
 
    #dropping rows outside monitoring window
    df=df[(df.index>=monwin.index[0])&(df.index<=monwin.index[-1])]

    if to_smooth:
        #smoothing dataframes with moving average
        df=df.rolling(window=roll_window_numpts).mean()[roll_window_numpts-1:]

    
    
    #returning rounded-off values within monitoring window and the nan_df
    return np.round(df[(df.index>=monwin.index[0])&(df.index<=monwin.index[-1])],6), hasRawValue

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

    # getting slice for latest 3-hours + 1 rolling window operation    
    xz=xz.iloc[-(2*roll_window_numpts-1):]
    xy=xy.iloc[-(2*roll_window_numpts-1):]
    
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
            print "\n"

    
    return alert_out

def alert_summary(alert_out,alert_list):

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
#                print checklist[c].set_index(['ts','id']).drop(['disp_alert','min_vel','max_vel','vel_alert'], axis=1)
                break
                
def nonrepeat_colors(ax,NUM_COLORS,color='gist_rainbow'):
    cm = plt.get_cmap(color)
    ax.set_color_cycle([cm(1.*(NUM_COLORS-i-1)/NUM_COLORS) for i in range(NUM_COLORS)])
    return ax
    
    
def plot_column_positions(colname,x,xz,xy):
#==============================================================================
# 
#     DESCRIPTION
#     returns plot of xz and xy absolute displacements of each node
# 
#     INPUT
#     colname; array; list of sites
#     x; dataframe; vertical displacements
#     xz; dataframe; horizontal linear displacements along the planes defined by xa-za
#     xy; dataframe; horizontal linear displacements along the planes defined by xa-ya
#==============================================================================

    try:
        fig=plt.figure()
        ax_xz=fig.add_subplot(121)
        ax_xy=fig.add_subplot(122,sharex=ax_xz,sharey=ax_xz)
        
        ax_xz=nonrepeat_colors(ax_xz,len(cs_x))
        ax_xy=nonrepeat_colors(ax_xy,len(cs_x))
        
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
        fig.subplots_adjust(top=0.9)        
        fig.suptitle(colname+" as of "+str(x.index[-1]),fontsize='medium')
        
        plt.legend(fontsize='x-small')        
    
    except:        
        print colname, "ERROR in plotting column position"
    return ax_xz,ax_xy
    
def vel_classify(vel):
    velplot=pd.DataFrame(index=vel.index)
    for n in vel.columns:
#        velplot[n]=np.ones(len(vel))*(len(vel.columns)-n)
        velplot[n]=n
    try:        
        L2mask=(vel.abs()>T_velL2)&(vel.abs()<=T_velL3)
        L3mask=(vel.abs()>T_velL3) 
        
        L2=velplot[L2mask]
        L3=velplot[L3mask]
                
        return velplot,L2,L3
    except:
        print "ERROR computing velocity classification ###################################"
        return 
    
def plot_disp_vel(colname, xz,xy,xz_vel,xy_vel,excludenodelist,
                  xz_mx=0,xz_mn=0,xy_mx=0,xy_mn=0, 
                  disp_offset='mean',disp_zero=True):
#==============================================================================
# 
#     DESCRIPTION:
#     returns plot of xz and xy displacements per node, xz and xy velocities per node
# 
#     INPUT:
#     xz; array of floats; horizontal linear displacements along the planes defined by xa-za
#     xy; array of floats; horizontal linear displacements along the planes defined by xa-ya
#     xz_vel; array of floats; velocity along the planes defined by xa-za
#     xy_vel; array of floats; velocity along the planes defined by xa-ya
#==============================================================================

   
  
    #setting up zeroing and offseting parameters
    
    if disp_offset=='max':
        xzd_plotoffset=(xz.max()-xz.min()).max()
        disp_zero=True
    elif disp_offset=='mean':
        xzd_plotoffset=(xz.max()-xz.min()).mean()
        disp_zero=True
    elif disp_offset=='min':
        xzd_plotoffset=(xz.max()-xz.min()).min()
        disp_zero=True
    else:
        xzd_plotoffset=0
        
    # defining cumulative (surface) displacement    
    cs_xz=xz.sum(axis=1)   
    cs_xy=xy.sum(axis=1)
    cs_xz=cs_xz-cs_xz.values[0]+xzd_plotoffset*(len(xz.columns))
    cs_xy=cs_xy-cs_xy.values[0]+xzd_plotoffset*(len(xz.columns))
    
     
    #creating noise envelope
#    check if xz_mx, xz_mn, xy_mx, xy_mn are all arrays
    try:
        len(xz_mx)
        len(xz_mn)
        len(xy_mx)
        len(xy_mn)
    except:
        xz_mx=np.ones(len(xz.columns)+1)*np.nan
        xz_mn=np.ones(len(xz.columns)+1)*np.nan
        xy_mx=np.ones(len(xz.columns)+1)*np.nan
        xy_mn=np.ones(len(xz.columns)+1)*np.nan
        
    try:
        print np.where(xz_mx==xz_mn).all()
        xz_mx=np.ones(len(xz.columns)+1)*np.nan
        xz_mn=np.ones(len(xz.columns)+1)*np.nan
        print xz_mx
        
    except:
        if disp_zero:
            xz_first_row=xz.loc[(xz.index==xz.index[0])].values.squeeze()
            xz_mx0=np.subtract(xz_mx[:-1],xz_first_row)
            xz_mn0=np.subtract(xz_mn[:-1],xz_first_row)
        else:
            xz_mx0=xz_mx[:-1]
            xz_mn0=xz_mn[:-1]

    try:
        print np.where(xy_mx==xy_mn).all()
        xy_mx=np.ones(len(xz.columns)+1)*np.nan
        xy_mn=np.ones(len(xz.columns)+1)*np.nan
        print xy_mx
        
    except:
        if disp_zero:
            xy_first_row=xy.loc[(xy.index==xy.index[0])].values.squeeze()
            xy_mx0=np.subtract(xy_mx[:-1],xy_first_row)
            xy_mn0=np.subtract(xy_mn[:-1],xy_first_row)
        else:
            xy_mx0=xy_mx[:-1]
            xy_mn0=xy_mn[:-1]
    
    xz_u=pd.DataFrame(index=xz.index)
    xz_l=pd.DataFrame(index=xz.index)
    xy_u=pd.DataFrame(index=xy.index)
    xy_l=pd.DataFrame(index=xy.index)

    for h in xz.columns:
        if xz_mx0[h-1]==xz_mn0[h-1]:
            xz_u[h]=np.nan################   
            xz_l[h]=np.nan
        else:
            xz_u[h]=xz_mx0[h-1]
            xz_l[h]=xz_mn0[h-1]
        if xy_mx0[h-1]==xy_mn0[h-1]:
            xy_u[h]=np.nan
            xy_l[h]=np.nan
        else:
            xy_u[h]=xy_mx0[h-1]
            xy_l[h]=xy_mn0[h-1]
        
#        xz_u[h]=xz_mx0[h-1]
#        xz_l[h]=xz_mn0[h-1]
#        xy_u[h]=xy_mx0[h-1]
#        xy_l[h]=xy_mn0[h-1]
    
    xz_u=df_add_offset_col(xz_u,xzd_plotoffset)
    xz_l=df_add_offset_col(xz_l,xzd_plotoffset)
    xy_u=df_add_offset_col(xy_u,xzd_plotoffset)
    xy_l=df_add_offset_col(xy_l,xzd_plotoffset)
    
    #zeroing and offseting xz,xy
    if disp_zero:
        xz=df_add_offset_col(df_zero_initial_row(xz),xzd_plotoffset)
        xy=df_add_offset_col(df_zero_initial_row(xy),xzd_plotoffset)
    else:
        xz=df_add_offset_col(xz,xzd_plotoffset)
        xy=df_add_offset_col(xy,xzd_plotoffset)
    
    try:
        fig=plt.figure()
       
        
        #creating subplots        
        ax_xzd=fig.add_subplot(141)
        ax_xyd=fig.add_subplot(142,sharex=ax_xzd,sharey=ax_xzd)
        ax_xzv=fig.add_subplot(143)
        ax_xzv.invert_yaxis()
        ax_xyv=fig.add_subplot(144,sharex=ax_xzv,sharey=ax_xzv)

        #plotting cumulative (surface) displacments
        cs_xz.plot(ax=ax_xzd,color='0.4',linewidth=0.5,legend=False)
        cs_xy.plot(ax=ax_xyd,color='0.4',linewidth=0.5,legend=False)
        ax_xzd.fill_between(cs_xz.index,cs_xz.values,xzd_plotoffset*(len(xz.columns)),color='0.8')
        ax_xyd.fill_between(cs_xy.index,cs_xy.values,xzd_plotoffset*(len(xz.columns)),color='0.8')
       
       
        #assigning non-repeating colors to subplots axis
        ax_xzd=nonrepeat_colors(ax_xzd,len(xz.columns))
        ax_xyd=nonrepeat_colors(ax_xyd,len(xz.columns))
        ax_xzv=nonrepeat_colors(ax_xzv,len(xz.columns))
        ax_xyv=nonrepeat_colors(ax_xyv,len(xz.columns))
    
        
        #plotting displacement for xz
        curax=ax_xzd
        plt.sca(curax)
        xz.plot(ax=curax,legend=False)
        xz_u.plot(ax=curax,ls=':',legend=False)
        xz_l.plot(ax=curax,ls=':',legend=False)
        curax.set_title('3-day displacement\n XZ axis',fontsize='small')
        curax.set_ylabel('displacement scale, m', fontsize='small')
        y = xz.iloc[0].values
        x = xz.index[0]
        z = xz.columns
        for i,j in zip(y,z):
           curax.annotate(str(j),xy=(x,i),xytext = (5,-2.5), textcoords='offset points',size = 'x-small')
        
        #plotting displacement for xy
        curax=ax_xyd
        plt.sca(curax)
        xy.plot(ax=curax,legend=False)
        xy_u.plot(ax=curax,ls=':',legend=False)
        xy_l.plot(ax=curax,ls=':',legend=False)
        curax.set_title('3-day displacement\n XY axis',fontsize='small')
        y = xy.iloc[0].values
        x = xy.index[0]
        z = xy.columns
        for i,j in zip(y,z):
           curax.annotate(str(j),xy=(x,i),xytext = (5,-2.5), textcoords='offset points',size = 'x-small')
        
        #plotting velocity for xz
        curax=ax_xzv
        plt.sca(curax)
        try:        
            velplot,L2,L3=vel_classify(xz_vel)            
            velplot.plot(ax=curax,marker='.',legend=False)
            L2.plot(ax=curax,marker='^',ms=8,mfc='y',lw=0,legend=False)
            L3.plot(ax=curax,marker='^',ms=10,mfc='r',lw=0,legend=False)
            
            y = velplot.iloc[0].values
            x = velplot.index[0]
            z = velplot.columns
            for i,j in zip(y,z):
                curax.annotate(str(j),xy=(x,i),xytext = (5,-2.5), textcoords='offset points',size = 'x-small')            
            curax.set_ylabel('node ID', fontsize='small')
            curax.set_title('3-hr velocity alerts\n XZ axis',fontsize='small')
        except: 
            print "ERROR plotting xz velocity class"    
            
        
        #plotting velocity for xz        
        curax=ax_xyv
        plt.sca(curax)
        try:        
            velplot,L2,L3=vel_classify(xy_vel)            
            velplot.plot(ax=curax,marker='.',legend=False)
            L2.plot(ax=curax,marker='^',ms=8,mfc='y',lw=0,legend=False)
            L3.plot(ax=curax,marker='^',ms=10,mfc='r',lw=0,legend=False)
            
            y = velplot.iloc[0].values
            x = velplot.index[0]
            z = velplot.columns
            for i,j in zip(y,z):
                curax.annotate(str(j),xy=(x,i),xytext = (5,-2.5), textcoords='offset points',size = 'x-small')            
            curax.set_title('3-hr velocity alerts\n XY axis',fontsize='small')            
        except:
            print "ERROR plotting xz velocity class"
            
            
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
#    
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
        
        fig.subplots_adjust(top=0.85)        
        fig.suptitle(colname+" as of "+str(velplot.index[-1]),fontsize='medium')
        line=mpl.lines.Line2D((0.5,0.5),(0.1,0.8))
        fig.lines=line,
        
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

#ND_path = output_path + cfg.get('I/O', 'NDFilePath')
output_file_path = output_path + cfg.get('I/O','OutputFilePath')
proc_file_path = output_path + cfg.get('I/O','ProcFilePath')
#ColAlerts_file_path = output_path + cfg.get('I/O','ColAlertsFilePath')
#TrendAlerts_file_path = output_path + cfg.get('I/O','TrendAlertsFilePath')

RTfilepath = output_file_path + 'realtime/'
#Create filepaths if it does not exists
def create_dir(p):
    if not os.path.exists(p):
        os.makedirs(p)

directories = [output_file_path,RTfilepath]
for p in directories:
    create_dir(p)

#file names
#columnproperties_file = cfg.get('I/O','ColumnProperties')
CSVFormat = cfg.get('I/O','CSVFormat')
timer = cfg.get('I/O','timer')
NDlog = cfg.get('I/O','NDlog')
ND7x = cfg.get('I/O','ND7x')

#Create webtrends.csv if it does not exists

#files = [webtrends,textalert,textalert2,rainfall_alert,all_alerts,gsm_alert,eq_summary,eq_summaryGSM,timer,NDlog,ND7x]
#
#def create_file(f):
#    if not os.path.isfile(f):
#        with open(f,'w') as t:
#            pass
#
#for f in files:
#    create_file(output_file_path + f)


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

to_fill = cfg.getint('I/O','RT_to_fill')
to_smooth = cfg.getint('I/O','RT_to_smooth')

with_TrendingNodeAlert = cfg.getboolean('I/O','with_TrendingNodeAlert')
realtime_test_specific_sites = cfg.getboolean('I/O','realtime_test_specific_sites')
realtime_specific_sites = cfg.get('I/O','realtime_specific_sites').split(',')



#To Output File or not
PrintProc = cfg.getboolean('I/O','PrintProc')
PrintColPos = cfg.getboolean('I/O','PlotColPos')
PrintDispVel = cfg.getboolean('I/O','PlotDispVel')
#PrintTrendAlerts = cfg.getboolean('I/O','PrintTrendAlerts')
#PrintTAlert = cfg.getboolean('I/O','PrintTAlert')
#PrintTAlert2 = cfg.getboolean('I/O','PrintTAlert2')
#PrintWAlert = cfg.getboolean('I/O','PrintWAlert')
#PrintND = cfg.getboolean('I/O','PrintND')
PrintTimer = cfg.getboolean('I/O','PrintTimer')
#PrintAAlert = cfg.getboolean('I/O','PrintAAlert')
#PrintGSMAlert = cfg.getboolean('I/O', 'PrintGSMAlert')

TestSpecificTime = cfg.getboolean('I/O', 'test_specific_time')
#if PrintColPos or PrintTrendAlerts:
#    import matplotlib.pyplot as plt
#    plt.ioff()


#MAIN

#sys.stdout = open(RTfilepath+'runresult.txt', 'w')

if TestSpecificTime:
    end = pd.to_datetime(cfg.get('I/O','use_specific_time'))

else:
    end = datetime.now()
#Set as true if printing by JSON would be done
set_json = False


# setting monitoring window
roll_window_numpts, end, start, offsetstart, monwin = set_monitoring_window(roll_window_length,data_dt,rt_window_length,num_roll_window_ops,end=end)

# creating summary of alerts
ND_alert=[]
L0_alert=[]
L2_alert=[]
L3_alert=[]
alert_df = []
alert_list=[L3_alert,L2_alert,L0_alert,ND_alert]
alert_names=['L3: ','L2: ','L0: ','ND: ']


names = ['ts','col_a']
fmt = '%Y-%m-%d %H:%M'
hr = end - timedelta(hours=3)


# getting list of sensors
sensorlist = GetSensorList()

node_status = GetNodeStatus(1)

print datetime.now().strftime('%Y-%m-%d %H-%M')
for s in sensorlist:

    if realtime_test_specific_sites:
        if s.name not in realtime_specific_sites:
            continue

    last_col=sensorlist[-1:]
    last_col=last_col[0]
    last_col=last_col.name
    
    # getting current column properties
    colname,num_nodes,seg_len= s.name,s.nos,s.seglen
    print colname, end

    # list of working nodes     
    node_list = range(1, num_nodes + 1)
    not_working = node_status.loc[(node_status.site == colname) & (node_status.node <= num_nodes)]
    not_working_nodes = not_working['node'].values        
    for i in not_working_nodes:
        node_list.remove(i)

    # importing proc_monitoring file of current column to dataframe
    try:
        proc_monitoring=genproc.generate_proc(colname,end)
    except KeyboardInterrupt:
        continue

    # creating series lists per node
    xz_series_list,xy_series_list = create_series_list(proc_monitoring,monwin,colname,num_nodes)
    
    # create xz,xy dataframe for error analysis, and analyze noise of unsmoothed data and the effect on column position    
    xz=create_fill_smooth_df(xz_series_list,num_nodes,monwin, roll_window_numpts,0,0)[0]
    xy=create_fill_smooth_df(xy_series_list,num_nodes,monwin, roll_window_numpts,0,0)[0] 
    xz_mx,xz_mn,xy_mx,xy_mn, xz_mxc, xz_mnc, xy_mxc,xy_mnc=err.cml_noise_profiling(xz,xy)

    # create, fill and smooth dataframes from series lists
    xz,hasRawValue=create_fill_smooth_df(xz_series_list,num_nodes,monwin, roll_window_numpts,to_fill,to_smooth)
    xy=create_fill_smooth_df(xy_series_list,num_nodes,monwin, roll_window_numpts,to_fill,to_smooth)[0]
    
    #user-defined list of nodes to exclude (reset to vertical) in the plots
    excludenodelist=[1]    
    
    # resetting excluded nodes to vertical position   
    col_mask=((xz == xz) | xz.isnull()) & ([a in excludenodelist for a in xz.columns])
    xz=xz.mask(col_mask).fillna(0.)
    xy=xy.mask(col_mask).fillna(0.)
    


    # computing instantaneous velocity
    vel_xz, vel_xy = compute_node_inst_vel(xz,xy,roll_window_numpts)

    
    # computing cumulative displacements
    cs_x, cs_xz, cs_xy=compute_col_pos(xz,xy,monwin.index[-1], col_pos_interval, col_pos_num)
    
    
                                                                                                                          
    # Alert generation
    alert_out=alert_generation(colname,xz,xy,vel_xz,vel_xy,num_nodes, T_disp, T_velL2, T_velL3, k_ac_ax,
                               num_nodes_to_check,end,proc_file_path,CSVFormat)
    
    print '\n',alert_out,'\n'
    
#    #11. Plotting column positions
    if PrintColPos:
        ax=plot_column_positions(colname,cs_x,cs_xz,cs_xy)
        
        #plotting error band for column positions        
        try:
            xl=cs_x.mean().values               
            ax[0].fill_betweenx(xl[::-1], xz_mxc, xz_mnc, where=xz_mxc >= xz_mnc, facecolor='0.7',linewidth=0)
            ax[1].fill_betweenx(xl[::-1], xy_mxc, xy_mnc, where=xy_mxc >= xy_mnc, facecolor='0.7',linewidth=0)
        except KeyboardInterrupt:
            print 'no column position error band'
        plt.savefig(RTfilepath+colname+' colpos '+str(end.strftime('%Y-%m-%d %H-%M')),
                    dpi=160, facecolor='w', edgecolor='w',orientation='landscape',mode='w')


#
    #12. Plotting displacement and velocity
    if PrintDispVel:
        plot_disp_vel(colname, xz,xy, vel_xz, vel_xy,xz_mx,xz_mn,xy_mx,xy_mn)
        plt.savefig(RTfilepath+colname+' disp_vel '+str(end.strftime('%Y-%m-%d %H-%M')),
                    dpi=160, facecolor='w', edgecolor='w',orientation='landscape',mode='w')





# records the number of minutes the code runs
if PrintTimer:
    end_time = datetime.now() - start_time
    with open (output_file_path+timer, 'ab') as p:
        p.write (start_time.strftime(fmt) + ": " + str(end_time) + '\n')
        print 'run time =', end_time
        

    

