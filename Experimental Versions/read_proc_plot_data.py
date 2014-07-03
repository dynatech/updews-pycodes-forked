from dateutil import parser
import pandas as pd
from pandas.stats.api import ols
import numpy as np
from numpy import nan
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os
import csv
from datetime import datetime, date, time, timedelta
import gc
import math
from matplotlib.ticker import MaxNLocator, AutoMinorLocator
from matplotlib import font_manager as font
import ConfigParser
import alert_evaluation as al

##################
#THRESHOLD VALUES#
##################
thresholdtilt=0.05 #degrees
offsettilt=0
thresholdvel=0.005 #degrees/day
offsetvel=0

Tvela1=0.005 #m/day
Tvela2=0.5 #m/day
Ttilt=0.05 #m
op_axis_k=0.1
adj_node_k=0.5


###################
#LEGEND PARAMETERS#
###################
legend_font_props = font.FontProperties()
legend_font_props.set_size('x-small')
ncol=1
bbtoa=(1,0.5)
loc="center left"
prop=legend_font_props

###################
# GRID PARAMETERS #
###################
g_which='both'
g_ax='both'
g_ls='-'
g_c='0.6'

tan=math.tan
sin=math.sin
asin=math.asin
cos=math.cos
pow=math.pow
sqrt=math.sqrt
atan=math.atan
deg=math.degrees
rad=math.radians

def Input_Loc_Col(loc_col_list,num_nodes_loc_col,col_seg_len_list,IWS):
    colname=loc_col_list[IWS]
    num_nodes=num_nodes_loc_col[IWS]
    seg_len=col_seg_len_list[IWS]
    return colname,num_nodes,seg_len

def x_from_xzxy(node_len, xz, xy):
    #xz and xy here are already linear units
    cond=(xz==0)*(xy==0)
    diagbase=np.sqrt(np.power(xz,2)+np.power(xy,2))
    return np.round(np.where(cond,
                               node_len*np.ones(len(xz)),
                               np.sqrt(node_len**2-np.power(diagbase,2))),2)

def df_from_csv(csvfilepath,colname,col):
    df=pd.read_csv(csvfilepath+colname+"_proc.csv",names=col,parse_dates=[col[0]],index_col=col[0])
    xz,xy = df['xz'].values,df['xy'].values
    x = x_from_xzxy(seg_len, xz, xy)
        
    #appending linear displacements series to data frame
    df['xlin']=pd.Series(data=x,index=df.index)
    df['xzlin']=pd.Series(data=xz,index=df.index)
    df['xylin']=pd.Series(data=xy,index=df.index)
    df.drop(['xz','xy'],inplace=True,axis=1)
    gc.collect()
    
    #creating dataframes
    xzdf, xydf, xdf = create_dataframes(df, num_nodes, seg_len)

    return xzdf, xydf, xdf


def create_dataframes(df, num_nodes, seg_len):
    #creating series list
    xzlist=[]
    xylist=[]
    xlist=[]
    for curnodeID in range(num_nodes):
        #extracting data from current node
        df_curnode=df[(df.Node_ID==curnodeID+1)]
        dates=df_curnode.index

        #handling "no data"
        if len(dates)<1:
            print 'Filling node:',curnodeID+1
            xzlist.append(pd.Series(data=[0],index=[df.index[-1]], name=curnodeID+1))
            xylist.append(pd.Series(data=[0],index=[df.index[-1]], name=curnodeID+1))
            xlist.append(pd.Series(data=[seg_len],index=[df.index[-1]], name=curnodeID+1))
            continue

        #extracting component displacements as series
        xz=pd.Series(data=df_curnode['xzlin'], index=df_curnode.index, name=curnodeID+1)
        xy=pd.Series(data=df_curnode['xylin'], index=df_curnode.index, name=curnodeID+1)
        x=pd.Series(data=df_curnode['xlin'], index=df_curnode.index, name=curnodeID+1)

        #resampling series to 30-minute intervals
        xz=xz.resample('30Min',how='mean',base=0)
        xy=xy.resample('30Min',how='mean',base=0)
        x=x.resample('30Min',how='mean',base=0)
        
        #appending resampled series to list
        xzlist.append(xz)
        xylist.append(xy)
        xlist.append(x)

    #creating unfilled XZ, XY and X dataframes
    xzdf=pd.concat([xzlist[num_nodes-a-1] for a in range(num_nodes)] ,axis=1,join='outer', names=[num_nodes-b for b in range(num_nodes)])
    xydf=pd.concat([xylist[num_nodes-a-1] for a in range(num_nodes)] ,axis=1,join='outer', names=[num_nodes-b for b in range(num_nodes)])
    xdf=pd.concat([xlist[num_nodes-a-1] for a in range(num_nodes)] ,axis=1,join='outer', names=[num_nodes-b for b in range(num_nodes)])    
    return xzdf, xydf, xdf

def resamp_fill_df(resampind, xzdf,xydf,xdf):
    #resampling XZ, XY and X dataframes to desired interval
    r_xzdf=xzdf.resample(resampind,how='mean',base=0)
    r_xydf=xydf.resample(resampind,how='mean',base=0)
    r_xdf=xdf.resample(resampind,how='mean',base=0)
    #filling XZ,XY and X dataframes
    fr_xzdf=r_xzdf.fillna(method='pad')
    fr_xydf=r_xydf.fillna(method='pad')
    fr_xdf=r_xdf.fillna(method='pad')
    fr_xzdf=fr_xzdf.fillna(method='bfill')
    fr_xydf=fr_xydf.fillna(method='bfill')
    fr_xdf=fr_xdf.fillna(method='bfill')
    return fr_xzdf, fr_xydf, fr_xdf


def plot_node_disp(colname,num_nodes, xz, xy, x, voff, title):
    fig,ax=plt.subplots(nrows=3,ncols=1, sharex=True,sharey=True, figsize=fig_size )
    plt.suptitle(colname+"\n"+title)
    cm = plt.get_cmap('gist_rainbow')
    ax[0].set_color_cycle([cm(1.*(num_nodes-i-1)/(num_nodes)) for i in range((num_nodes))])
    ax[1].set_color_cycle([cm(1.*(num_nodes-i-1)/(num_nodes)) for i in range((num_nodes))])
    ax[2].set_color_cycle([cm(1.*(num_nodes-i-1)/(num_nodes)) for i in range((num_nodes))])
    for curnodeID in range(num_nodes):
        plt.sca(ax[0])
        xplot=voff*(curnodeID)+(x[num_nodes-curnodeID]-x[num_nodes-curnodeID][0])
        xplot.plot()
        plt.sca(ax[1])
        xzplot=voff*(curnodeID)+(xz[num_nodes-curnodeID]-xz[num_nodes-curnodeID][0])
        xzplot.plot()
        plt.sca(ax[2])
        xyplot=voff*(curnodeID)+(xy[num_nodes-curnodeID]-xy[num_nodes-curnodeID][0])
        xyplot.plot()
    ax[0].set_xlabel([], visible=False)
    ax[1].set_xlabel([], visible=False)
    ax[0].set_ylabel("X, m", fontsize='small')
    ax[1].set_ylabel("XZ, m", fontsize='small')
    ax[2].set_ylabel("XY, m", fontsize='small')
    plt.tight_layout()
    plt.subplots_adjust(left=None, bottom=None, right=None, top=0.9,wspace=None, hspace=None)
    return fig, ax


def plot_col_pos(cs_xzdf, cs_xydf, cs_xdf, colposperiod, colpostype):
    dat=pd.date_range(start=cs_xzdf.index[0],end=cs_xzdf.index[-1], freq=colposperiod)
    
    fig,ax=plt.subplots(nrows=1,ncols=2, sharex=True, sharey=True,figsize=fig_size)
    plt.suptitle(colname+"\n"+colpostype+" col pos")
    cm = plt.get_cmap('gist_rainbow')
    ax[0].set_color_cycle([cm(1.*(len(dat)-i-1)/len(dat)) for i in range(len(dat))])
    ax[1].set_color_cycle([cm(1.*(len(dat)-i-1)/len(dat)) for i in range(len(dat))])

    for d in range(len(dat)):
        curxz=cs_xzdf[(cs_xzdf.index==dat[d])]
        curxy=cs_xydf[(cs_xydf.index==dat[d])]
        curx=cs_xdf[(cs_xdf.index==dat[d])]

        if colpostype=='rel':
            curxz=curxz.sub(cs_xzdf.iloc[0,:],axis=1)    
            curxy=curxy.sub(cs_xydf.iloc[0,:],axis=1)    

        plt.sca(ax[0])
        plt.axis('equal')
        plt.plot([[0]]+curxz.values.T.tolist(),[[0.0]]+curx.values.T.tolist(), '.-')

        plt.sca(ax[1])
        plt.axis('equal')
        plt.plot([[0.0]]+curxy.values.T.tolist(),[[0.0]]+curx.values.T.tolist(), '.-',label=datetime.strftime(dat[d],'%Y-%m-%d'))
        

    ax[0].set_xlabel("XZ disp, m \n (+) downslope", fontsize='small',horizontalalignment='center')
    ax[0].set_ylabel("X disp, m \n (+) towards surface", fontsize='small', rotation='vertical',horizontalalignment='center')
    ax[1].set_xlabel("XY disp, m \n (+) to the right, facing downslope", fontsize='small', horizontalalignment='center')
    plt.legend(loc='lower right', fontsize='small')
    
    plt.tight_layout()
    plt.subplots_adjust(left=None, bottom=None, right=None, top=0.9,wspace=None, hspace=None)
    return fig,ax

def time_exact_interval(end):

    end_Year=end.year
    end_month=end.month
    end_day=end.day

    end_minute=end.minute
    if end_minute==30:
        end_hour=end.hour
        end_minute=30
        end_second=0
    if end_minute>30:
        end_hour=end.hour+1
        end_minute=0
        end_second=0
    if end_minute<30:
        end_hour=end.hour
        end_minute=0
        end_second=0

    end=datetime.combine(date(end_Year,end_month,end_day),time(end_hour,end_minute,end_second))

    return end

def compute_colpos_time(end,days,numcolpos):
    colposdays=np.linspace(-days,0,numcolpos)
    colposdates=day_to_date(colposdays, end)

    return colposdates

def day_to_date(days, end_date):
    date=[end_date+timedelta(days=x) for x in days]

    return date


#CONSTANTS and SETTINGS
loc_col_list=("eeet","sinb","sint","sinu","lipb","lipt","bolb","pugb","pugt","mamb","mamt","oslb","labt", "labb", "gamt","gamb", "humt","humb", "plat","plab","blct","blcb")
num_nodes_loc_col=(14,29,19,29,28,31,30,14,10,29,24,23,39,25,18,22,21,26,39,40,24,19)
col_seg_len_list=(0.5,1,1,1,0.5,0.5,0.5,1.2,1.2,1.0,1.0,1.,1.,1.,1.,1.,1.,1,0.5,0.5,1,1)
col = ['Time','Node_ID', 'xz', 'xy', 'moi']
fig_size=(9.5,6.5)


cfg = ConfigParser.ConfigParser()
cfg.read('IO-config.txt')

#set file path for input *.proc files
csvfilepath=cfg.get('I/O','OutputFilePath')

#set file path for saving figures and CSVs
OutputFigurePath = cfg.get('I/O','OutputFigurePath')
figsavefilepath="C:\Users\\acer\Desktop\%s" %''
CSVOutputFile = cfg.get('I/O','CSVOutputFilePath') + cfg.get('I/O','CSVOutputFile')

#set this to 1 to plot figures
plotfigs=1

#set this to 1 to save figures, 0 to just display the output
savefigs=0

#set this to desired node date sampling interval: D=daily, 3H=3-hourly, M=monthly, Q=quarterly, etc...
resampind='30Min'

#set this to desired interval between column positions: D=daily, 3H=3-hourly, M=monthly, Q=quarterly, etc...
colposperiod='D'
    

#MAIN

#manually input date to slice dataframe to desired date interval
Y=2012
m=9
d=28
H=0
M=0
S=0
end=datetime.combine(date(Y,m,d),time(H,M,S))
end=datetime.now()
end=time_exact_interval(end)
start=end-timedelta(days=4)
start_alert=end-timedelta(days=3)

windowlength=7
INPUT_fit_interval=3
INPUT_number_colpos=INPUT_fit_interval+1
dates_to_plot=compute_colpos_time(end,INPUT_fit_interval,INPUT_number_colpos)
csvout=[]

for INPUT_which_sensor in range(1,len(loc_col_list)):
    if INPUT_which_sensor!=5:continue
    colname,num_nodes,seg_len=Input_Loc_Col(loc_col_list,num_nodes_loc_col,col_seg_len_list, INPUT_which_sensor)
    all_nodes_data=range(num_nodes)
    all_vel_data=range(num_nodes)

    print "\nDATA for ",colname," as of ", datetime.now().strftime("%Y-%m-%d %H:%M")    
    
    #reading from csv file and writing to dataframe
    xzdf, xydf, xdf = df_from_csv(csvfilepath,colname,col)
    
    #resampling and filling XZ, XY and X dataframes
    fr_xzdf, fr_xydf, fr_xdf = resamp_fill_df(resampind, xzdf,xydf,xdf)

    #slicing dataframe for rolling mean analysis
    fr_xzdf = fr_xzdf[start:end]
    fr_xydf = fr_xydf[start:end]
    fr_xdf = fr_xdf[start:end]
    if len(fr_xzdf)<1: continue
    
    #computing cumulative node displacements
    cs_xzdf=fr_xzdf.cumsum(axis=1)
    cs_xydf=fr_xydf.cumsum(axis=1)
    cs_xdf=fr_xdf.cumsum(axis=1)

    #plots absolute column position
#    fig_cp, ax_cp=plot_col_pos(cs_xzdf, cs_xydf, cs_xdf, colposperiod, "abs")

    #plots relative column position
#    fig_cp, ax_cp=plot_col_pos(cs_xzdf, cs_xydf, cs_xdf, colposperiod, "rel")

    #rolling mean in 3 hour-window and 3 minimum data points
    rm_xzdf=pd.rolling_mean(fr_xzdf,window=windowlength)
    rm_xydf=pd.rolling_mean(fr_xydf,window=windowlength)
    #linear regression in 3 hour-window and 3 minimum data points
    td_rm_xzdf=rm_xzdf.index.values-rm_xzdf.index.values[0]
    td_rm_xydf=rm_xydf.index.values-rm_xydf.index.values[0]
    tdelta=pd.Series(td_rm_xzdf/np.timedelta64(1,'D'),index=rm_xzdf.index)
    tdelta=pd.Series(td_rm_xydf/np.timedelta64(1,'D'),index=rm_xydf.index)

    #setting up dataframe for velocity values
    d_vel_xzdf=pd.DataFrame(data=None, index=rm_xzdf.index)
    d_vel_xydf=pd.DataFrame(data=None, index=rm_xydf.index)
        
    for cur_node_ID in range(num_nodes):
        
        lr_xzdf=ols(y=rm_xzdf[num_nodes-cur_node_ID],x=tdelta,window=windowlength,intercept=True)
        lr_xydf=ols(y=rm_xydf[num_nodes-cur_node_ID],x=tdelta,window=windowlength,intercept=True)
<<<<<<< .mine
        vel_xzdf[str(num_nodes-cur_node_ID)]=np.concatenate((np.nan*np.ones((windowlength-1)*2),lr_xzdf.beta.x.values))
        vel_xydf[str(num_nodes-cur_node_ID)]=np.concatenate((np.nan*np.ones((windowlength-1)*2),lr_xydf.beta.x.values))
        #all_vel_data[cur_node_ID]=(vel_xzdf,vel_xydf)
=======
        d_vel_xzdf[str(num_nodes-cur_node_ID)]=np.concatenate((np.nan*np.ones(12),lr_xzdf.beta.x.values))
        d_vel_xydf[str(num_nodes-cur_node_ID)]=np.concatenate((np.nan*np.ones(12),lr_xydf.beta.x.values))
        vel_xzdf=(lr_xzdf.beta.index,lr_xzdf.beta.x.values)
        vel_xydf=(lr_xydf.beta.index,lr_xydf.beta.x.values)
        all_vel_data[cur_node_ID]=(vel_xzdf,vel_xydf)
>>>>>>> .r69
    
        #instantaneous velocity
        #temp_lr_xzdf = lr_xzdf.beta.x
        #temp_lr_xydf = lr_xydf.beta.x
        #tilt_xz = round((temp_lr_xzdf.tail(1).values[0]-temp_lr_xzdf.head(1).values[0]),4)
        #tilt_xy = round((temp_lr_xydf.tail(1).values[0]-temp_lr_xydf.head(1).values[0]),4)
        #inst_vel_xz = round(temp_lr_xzdf[-1],4)
        #inst_vel_xy = round(temp_lr_xydf[-1],4)

        #cur_node_data=(cur_node_ID+1,tilt_xz,inst_vel_xz,tilt_xy,inst_vel_xy)
        #all_nodes_data[cur_node_ID]=cur_node_data

    #slicing to 3-day window
    rm_xzdf=rm_xzdf[start_alert:end]
    rm_xydf=rm_xydf[start_alert:end]
    d_vel_xzdf=d_vel_xzdf[start_alert:end]
    d_vel_xydf=d_vel_xydf[start_alert:end]
    if len(rm_xzdf)<1: continue

    #Displays node alert of columns
    ac_ax=al.node_alert(colname, rm_xzdf, rm_xydf, d_vel_xzdf, d_vel_xydf, num_nodes, 0.05, 0.005, 0.5, 0.1)
    col_al=al.column_alert(ac_ax,5)
    #print col_al

    #creating one dataframe for all column alerts
    csvout.append(col_al)
    #print csvout

    ##plots time series (tilt, velocity) within date range##
    if 1==0:
        for INPUT_which_axis in [0,1]:
                
            tiltvelfig=plt.figure(10+INPUT_which_axis)
            plt.clf()
            axtilt=tiltvelfig.add_subplot(121)
            axvel=tiltvelfig.add_subplot(122, sharex=axtilt)
                    
            if INPUT_which_axis==0:
                tiltvelfig.suptitle(loc_col_list[INPUT_which_sensor]+" XZ as of "+str(end.strftime("%Y-%m-%d %H:%M")))

            else:
                tiltvelfig.suptitle(loc_col_list[INPUT_which_sensor]+" XY as of "+str(end.strftime("%Y-%m-%d %H:%M")))

            for INPUT_which_node in range(num_nodes):

                if INPUT_which_axis==0:
                    cur_tilt_data=fr_xzdf[INPUT_which_node+1].values

                if INPUT_which_axis==1:
                    cur_tilt_data=fr_xydf[INPUT_which_node+1].values
                
                vel_data=all_vel_data[INPUT_which_node]                    
                cur_vel_data=vel_data[INPUT_which_axis]
                cur_vel_date=cur_vel_data[0]
                cur_vel_values=cur_vel_data[1]
                cur_vel_values=np.asarray(cur_vel_values)
                                        
                axtilt.axhspan(offsettilt*thresholdtilt*(num_nodes-(INPUT_which_node))-thresholdtilt,offsettilt*thresholdtilt*(num_nodes-(INPUT_which_node))+thresholdtilt,color='0.9')
                axtilt.axhline(y=(offsettilt*thresholdtilt*(num_nodes-(INPUT_which_node))),color='0.6')
                axtilt.plot(fr_xzdf[INPUT_which_node+1].index,[offsetvel*thresholdvel*(num_nodes-(INPUT_which_node))+ q for q in cur_tilt_data], '-',linewidth=1,label="n"+str(INPUT_which_node+1))

                axvel=tiltvelfig.add_subplot(122, sharex=axtilt)
                        
                offsetvel=0
                axvel.axhspan(offsetvel*thresholdvel*(num_nodes-(INPUT_which_node))-thresholdvel,offsetvel*thresholdvel*(num_nodes-(INPUT_which_node))+thresholdvel,color='0.9')
                axvel.axhline(y=(offsetvel*thresholdvel*(num_nodes-(INPUT_which_node))),color='0.6')
                axvel.plot(cur_vel_date,[offsetvel*thresholdvel*(num_nodes-(INPUT_which_node))+ q for q in cur_vel_values],'-',linewidth=1,label="n"+str(INPUT_which_node+1))
                
                
            days_vlines=[end+timedelta(days=-q) for q in range(0,int(INPUT_fit_interval)+1)]
            for dvl in range(len(days_vlines)):
                if dvl!=0:lw=(0.2)

                else:lw=(1)
                plt.sca(axtilt)
                axtilt.axvline(x=days_vlines[dvl], color='r',lw=lw)
                plt.sca(axvel)
                axvel.axvline(x=days_vlines[dvl], color='r',lw=lw)

            for cpvl in range(len(dates_to_plot)):
                plt.sca(axtilt)
                axtilt.axvline(x=dates_to_plot[cpvl], color='b',lw=0.5)
                        
            plt.sca(axtilt)
            cax=plt.gca()
            cax.yaxis.set_major_locator(MaxNLocator(4))
            cax.yaxis.set_minor_locator(AutoMinorLocator(4))
            cax.fmt_xdata = mdates.DateFormatter('%Y-%m-%d %H:%M')
            plt.xlim(end+timedelta(days=-INPUT_fit_interval),end+timedelta(days=0))    
            plt.ylabel("displacement (m)")
            plt.xlabel("date,time")
                    
            plt.sca(axvel)
            cax=plt.gca()
            axvel.axhline(y=0.005,color='y', ls=':',lw=3,label="A1: Slow")
            axvel.axhline(y=.50,color='r', ls=':',lw=3,label="A2: Mod")
            axvel.axhline(y=-0.005,color='y', ls=':',lw=3,)
            axvel.axhline(y=-.50,color='r', ls=':',lw=3,)
                    
            cax.yaxis.set_major_locator(MaxNLocator(4))
            cax.yaxis.set_minor_locator(AutoMinorLocator(4))
            cax.fmt_xdata = mdates.DateFormatter('%Y-%m-%d %H:%M')
            plt.yscale('symlog', linthreshy=.01,linscaley=1)
            plt.ylim(-1,1)
            plt.xlabel("date,time")
            plt.ylabel("velocity (m/day)")
            plt.legend()
            cax.legend(ncol=1,loc="upper left", bbox_to_anchor=(1,1),prop=legend_font_props)

            tiltvelfig.autofmt_xdate()

            if INPUT_which_axis==0:
                fig_name=OutputFigurePath+loc_col_list[INPUT_which_sensor]+"_xz.png"
            else:
                fig_name=OutputFigurePath+loc_col_list[INPUT_which_sensor]+"_xy.png"

<<<<<<< .mine
    #plt.close()
=======
#writing column alerts into csv file
csvout=pd.concat(csvout[1:])
csvout=np.asarray(csvout)
with open(CSVOutputFile, "wb") as f:
    writer = csv.writer(f)
    writer.writerows(csvout)
    print "\nAlert file written"

    plt.close()
>>>>>>> .r69
    #plt.savefig(fig_name, dpi=100, facecolor='w', edgecolor='w',orientation='landscape')
