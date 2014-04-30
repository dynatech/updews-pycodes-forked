import pandas as pd
import numpy as np
from numpy import nan
import scipy.stats.stats as st
import matplotlib.pyplot as plt
import os
import csv
from datetime import datetime, date, time, timedelta
import gc


loc_col_list=("eeet","sinb","sint","sinu","lipb","lipt","bolb","pugb","pugt","mamb","mamt","oslt","oslb","labt", "labb", "gamt","gamb", "humt","humb", "plat","plab","blct","blcb")
num_nodes_loc_col=(14,29,19,29,28,31,30,14,10,29,24,21,23,39,25,18,22,21,26,39,40,24,19)
col_seg_len_list=(0.5,1,1,1,0.5,0.5,0.5,1.2,1.2,1.0,1.0,1.,1.,1.,1.,1.,1.,1.,1,0.5,0.5,1,1)
csvfilepath="/home/egl-sais/Dropbox/Senslope Data/Proc/csv/"
col = ['Time','Node_ID', 'x', 'y', 'z', 'good_tilt', 'xz', 'xy', 'phi', 'rho', 'moi', 'good_moi']

def Input_Loc_Col(loc_col_list,num_nodes_loc_col,col_seg_len_list,IWS):
    colname=loc_col_list[IWS]
    num_nodes=num_nodes_loc_col[IWS]
    seg_len=col_seg_len_list[IWS]
    return colname,num_nodes,seg_len

def xzxy_to_cart(node_len, xz, xy):
    #xz and xy here are in angular units (degrees)
    cond=(xz==0)*(xy==0)
    H=np.round(np.where(cond,
                        node_len*np.ones(len(xz)),
                        node_len/np.sqrt(1+(np.tan(np.radians(xz)))**2+(np.tan(np.radians(xy)))**2)),3)
    a=np.where(cond,
               np.zeros(len(xz)),
               (np.round(H*np.tan(np.radians(xz)),3)))
    b=np.where(cond,
               np.zeros(len(xz)),
               (np.round(H*np.tan(np.radians(xy)),3)))
    return H,a,b


def create_dataframes(df, num_nodes, seg_len):
    #creating series list
    xzlist=[]
    xylist=[]
    xlist=[]
    for curnodeID in range(num_nodes):
        #extracting data from current node, with good tilt filter
        df_curnode=df[(df.Node_ID==curnodeID+1) & (df.good_tilt==1)]# & (df.good_moi==1)]
        dates=df_curnode.index

        #handling "no data"
        if len(dates)<1:
            print curnodeID 
            xzlist.append(pd.Series(data=[0],index=[df.index[0]]))
            xylist.append(pd.Series(data=[0],index=[df.index[0]]))
            xlist.append(pd.Series(data=[seg_len],index=[df.index[0]]))
            continue

        #converting angular to linear units
        x,xz,xy=xzxy_to_cart(seg_len,df_curnode['xz'].values,df_curnode['xy'].values)

        #creating series
        xz=pd.Series(data=xz, index=dates)
        xy=pd.Series(data=xy, index=dates)
        x=pd.Series(data=x, index=dates)
        
        #resampling series to 30-minute intervals
        #xz=xz.asfreq('30Min')
        #xy=xy.asfreq('30Min')
        #x=x.asfreq('30Min')
        #resampling series to 30-minute intervals
        xz=xz.resample('30Min',how='mean',base=0)
        xy=xy.resample('30Min',how='mean',base=0)
        x=x.resample('30Min',how='mean',base=0)

        #appending resampled series to list
        xzlist.append(xz)
        xylist.append(xy)
        xlist.append(x)
    #creating unfilled XZ, XY and X dataframes
    xzdf=pd.concat([xzlist[a] for a in range(num_nodes)] ,axis=1,join='outer')
    xydf=pd.concat([xylist[a] for a in range(num_nodes)] ,axis=1,join='outer')
    xdf=pd.concat([xlist[a] for a in range(num_nodes)] ,axis=1,join='outer')  
    return xzdf, xydf, xdf

def resamp_fill_df(resampind, xzdf,xydf,xdf):
    #resampling series to 30-minute intervals
    xzdf=xzdf.resample('30Min',how='mean',base=0)
    xydf=xydf.resample('30Min',how='mean',base=0)
    xdf=xdf.resample('30Min',how='mean',base=0)
    #resampling XZ, XY and X dataframes
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


def compute_cumulative_node_disp(fr_xzdf,fr_xydf,fr_xdf):
    rcols=fr_xzdf.columns.tolist()[::-1]
    fr_xzdf=fr_xzdf[rcols]
    fr_xydf=fr_xydf[rcols]
    fr_xdf=fr_xdf[rcols]
    return fr_xzdf.cumsum(axis=1), fr_xydf.cumsum(axis=1),fr_xdf.cumsum(axis=1)





def plot_unfilled_individual_node_disp(num_nodes, xzdf, xydf, xdf, voff):
    
    fig,ax=plt.subplots(nrows=3,ncols=1, sharex=True,sharey=True, figsize=fig_size )
    plt.suptitle(colname+"\nunfilled individual node disp")
    cm = plt.get_cmap('gist_rainbow')
    ax[0].set_color_cycle([cm(1.*(num_nodes-i-1)/(num_nodes)) for i in range((num_nodes))])
    ax[1].set_color_cycle([cm(1.*(num_nodes-i-1)/(num_nodes)) for i in range((num_nodes))])
    ax[2].set_color_cycle([cm(1.*(num_nodes-i-1)/(num_nodes)) for i in range((num_nodes))])
    for curnodeID in range(num_nodes):
        plt.sca(ax[0])
        xplot=voff*(curnodeID)+(xdf[num_nodes-curnodeID-1]-xdf[num_nodes-curnodeID-1][0])
        xplot.plot()
        plt.sca(ax[1])
        xzplot=voff*(curnodeID)+(xzdf[num_nodes-curnodeID-1]-xzdf[num_nodes-curnodeID-1][0])
        xzplot.plot()
        plt.sca(ax[2])
        xyplot=voff*(curnodeID)+(xydf[num_nodes-curnodeID-1]-xydf[num_nodes-curnodeID-1][0])
        xyplot.plot()
    ax[0].set_xlabel([], visible=False)
    ax[1].set_xlabel([], visible=False)
    ax[0].set_ylabel("X, m", fontsize='small')
    ax[1].set_ylabel("XZ, m", fontsize='small')
    ax[2].set_ylabel("XY, m", fontsize='small')
    plt.tight_layout()
    plt.subplots_adjust(left=None, bottom=None, right=None, top=0.9,wspace=None, hspace=None)
    return fig, ax


def plot_individual_node_disp(num_nodes, fr_xzdf, fr_xydf, fr_xdf, voff):
    fig,ax=plt.subplots(nrows=3,ncols=1, sharex=True,sharey=True,figsize=fig_size)
    plt.suptitle(colname+"\nindividual node disp")
    cm = plt.get_cmap('gist_rainbow')
    ax[0].set_color_cycle([cm(1.*(num_nodes-i-1)/(num_nodes)) for i in range((num_nodes))])
    ax[1].set_color_cycle([cm(1.*(num_nodes-i-1)/(num_nodes)) for i in range((num_nodes))])
    ax[2].set_color_cycle([cm(1.*(num_nodes-i-1)/(num_nodes)) for i in range((num_nodes))])
    for curnodeID in range(num_nodes):
        plt.sca(ax[0])
        xplot=voff*(curnodeID)+(fr_xdf[num_nodes-curnodeID-1]-fr_xdf[num_nodes-curnodeID-1][0])
        xplot.plot()
        plt.sca(ax[1])
        xzplot=voff*(curnodeID)+(fr_xzdf[num_nodes-curnodeID-1]-fr_xzdf[num_nodes-curnodeID-1][0])
        xzplot.plot()
        plt.sca(ax[2])
        xyplot=voff*(curnodeID)+(fr_xydf[num_nodes-curnodeID-1]-fr_xydf[num_nodes-curnodeID-1][0])
        xyplot.plot()
    ax[0].set_xlabel([], visible=False)
    ax[1].set_xlabel([], visible=False)
    ax[0].set_ylabel("X, m", fontsize='small')
    ax[1].set_ylabel("XZ, m", fontsize='small')
    ax[2].set_ylabel("XY, m", fontsize='small')
    plt.tight_layout()
    plt.subplots_adjust(left=None, bottom=None, right=None, top=0.9,wspace=None, hspace=None)
    return fig, ax

def plot_cumulative_node_disp(num_nodes, cs_xzdf, cs_xydf, cs_xdf, voff):
    fig,ax=plt.subplots(nrows=3,ncols=1, sharex=True,sharey=True, figsize=fig_size)
    plt.suptitle(colname+"\ncumulative node disp")
    cm = plt.get_cmap('gist_rainbow')
    ax[0].set_color_cycle([cm(1.*(num_nodes-i-1)/(num_nodes)) for i in range((num_nodes))])
    ax[1].set_color_cycle([cm(1.*(num_nodes-i-1)/(num_nodes)) for i in range((num_nodes))])
    ax[2].set_color_cycle([cm(1.*(num_nodes-i-1)/(num_nodes)) for i in range((num_nodes))])
    for curnodeID in range(num_nodes):
        plt.sca(ax[0])
        xplot=voff*(curnodeID)+(cs_xdf[num_nodes-curnodeID-1]-cs_xdf[num_nodes-curnodeID-1][0])
        xplot.plot()
        plt.sca(ax[1])
        xzplot=voff*(curnodeID)+(cs_xzdf[num_nodes-curnodeID-1]-cs_xzdf[num_nodes-curnodeID-1][0])
        xzplot.plot()
        plt.sca(ax[2])
        xyplot=voff*(curnodeID)+(cs_xydf[num_nodes-curnodeID-1]-cs_xydf[num_nodes-curnodeID-1][0])
        xyplot.plot()
    ax[0].set_xlabel([], visible=False)
    ax[1].set_xlabel([], visible=False)
    ax[0].set_ylabel("X, m", fontsize='small')
    ax[1].set_ylabel("XZ, m", fontsize='small')
    ax[2].set_ylabel("XY, m", fontsize='small')
    plt.tight_layout()
    plt.subplots_adjust(left=None, bottom=None, right=None, top=0.9,wspace=None, hspace=None)
    return fig, ax





def plot_col_pos_abs(cs_xzdf, cs_xydf, cs_xdf, colposperiod):
    dat=pd.date_range(start=cs_xzdf.index[0],end=cs_xzdf.index[-1], freq=colposperiod)
    
    fig,ax=plt.subplots(nrows=1,ncols=2, sharex=True, sharey=True,figsize=fig_size)
    plt.suptitle(colname+"\nabs col pos")
    cm = plt.get_cmap('gist_rainbow')
    ax[0].set_color_cycle([cm(1.*(len(dat)-i-1)/len(dat)) for i in range(len(dat))])
    ax[1].set_color_cycle([cm(1.*(len(dat)-i-1)/len(dat)) for i in range(len(dat))])


    for d in range(len(dat)):    
        curxz=cs_xzdf[(cs_xzdf.index==dat[d])]
        curxy=cs_xydf[(cs_xydf.index==dat[d])]
        curx=cs_xdf[(cs_xdf.index==dat[d])]
        
        plt.sca(ax[0])
        plt.axis('equal')
        plt.plot([[0]]+curxz.values.T.tolist(),[[0]]+curx.values.T.tolist(), '.-',label=dat[d])
        #plt.legend()
        
        plt.sca(ax[1])
        plt.axis('equal')
        plt.plot([[0]]+curxy.values.T.tolist(),[[0]]+curx.values.T.tolist(), '.-',label=datetime.strftime(dat[d],'%Y-%m-%d'))
    ax[0].set_xlabel("XZ disp, m \n (+) downslope", fontsize='small',horizontalalignment='center')
    ax[0].set_ylabel("X disp, m \n (+) towards surface", fontsize='small', rotation='vertical',horizontalalignment='center')
    ax[1].set_xlabel("XY disp, m \n (+) to the right, facing downslope", fontsize='small', horizontalalignment='center')
    plt.legend(loc='lower right', fontsize='small')
    
    plt.tight_layout()
    plt.subplots_adjust(left=None, bottom=None, right=None, top=0.9,wspace=None, hspace=None)
    return fig,ax

def plot_col_pos_rel(cs_xzdf, cs_xydf, cs_xdf, colposperiod):
    dat=pd.date_range(start=cs_xzdf.index[0],end=cs_xzdf.index[-1], freq=colposperiod)
    cs_xzdf.index[0]
   

    fig,ax=plt.subplots(nrows=1,ncols=2, sharex=True, sharey=True,figsize=fig_size)
    plt.suptitle(colname+"\nrel col pos")
    cm = plt.get_cmap('gist_rainbow')
    ax[0].set_color_cycle([cm(1.*(len(dat)-i-1)/len(dat)) for i in range(len(dat))])
    ax[1].set_color_cycle([cm(1.*(len(dat)-i-1)/len(dat)) for i in range(len(dat))])

            
    for d in range(len(dat)):    
        curxz=cs_xzdf[(cs_xzdf.index==dat[d])]
        
        curxz=curxz.sub(cs_xzdf.iloc[0,:],axis=1)    

        curxy=cs_xydf[(cs_xydf.index==dat[d])]
        curxy=curxy.sub(cs_xydf.iloc[0,:],axis=1)    

        curx=cs_xdf[(cs_xdf.index==dat[d])]
        
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
















for INPUT_which_sensor in range(1,len(loc_col_list)):
    if INPUT_which_sensor!=1:continue
#    print loc_col_list[hj],":     ",hj
#INPUT_which_sensor=int(raw_input("Input which sensor to plot (number): "))
    colname,num_nodes,seg_len=Input_Loc_Col(loc_col_list,num_nodes_loc_col,col_seg_len_list, INPUT_which_sensor)

    print "\n",colname, num_nodes, seg_len
    fig_size=(9.5,6.5)
    figsavefilepath="/home/egl-sais/Desktop/sensorfigs/"

    start=datetime.now()
    

    #reading from csv file and writing to dataframe
    df=pd.read_csv(csvfilepath+colname+"_proc.csv",names=col,parse_dates=['Time'],index_col=0)
    gc.collect()

    #creating dataframes
    xzdf, xydf, xdf = create_dataframes(df, num_nodes, seg_len)

    #resampling and filling XZ, XY and X dataframes
    resampind='30Min'
    fr_xzdf, fr_xydf, fr_xdf = resamp_fill_df(resampind, xzdf,xydf,xdf)

    





    def movingLR(fr_xzdf, fr_xydf, fr_xdf, window_period):
        
        ts_xz=fr_xzdf.reset_index()
        ts_xy=fr_xydf.reset_index()
        ts_x=fr_xdf.reset_index()

        
        tdelta=ts_xz['Time']-ts_xz['Time'][0]
        tdelta=tdelta.astype('timedelta64[s]')/(60*60*24.)  #in days

        
    
   
        for col in range(0,len(ts_xz.columns)-1):
            print col
            model=pd.ols(y=ts_xz[col], x=tdelta,window_type='rolling', window=window_period, intercept=True)
            ts_xz[col]=np.round(model.beta.x,3)

            model=pd.ols(y=ts_xy[col], x=tdelta,window_type='rolling', window=window_period, intercept=True)
            ts_xy[col]=np.round(model.beta.x,3)

            model=pd.ols(y=ts_x[col], x=tdelta,window_type='rolling', window=window_period, intercept=True)
            ts_x[col]=np.round(model.beta.x,3)

        print ts_xz.tail
        print ts_xy.tail
        print ts_x.tail

    movingLR(fr_xzdf, fr_xydf, fr_xdf, 6)   

    

    
    plot=0
    if plot==1:

        #computing cumulative node displacements
        cs_xzdf, cs_xydf, cs_xdf = compute_cumulative_node_disp(fr_xzdf,fr_xydf,fr_xdf)

        #plotting individual node displacements
        voff=0.1 #in meters
        fig_id, ax_id=plot_unfilled_individual_node_disp(num_nodes, xzdf, xydf, xdf, voff)
        plt.savefig(figsavefilepath+colname+"nd_u.png", dpi=300,orientation='portrait', format="png")

        voff=0.1 #in meters
        fig_id, ax_id=plot_individual_node_disp(num_nodes, fr_xzdf, fr_xydf, fr_xdf, voff)
        plt.savefig(figsavefilepath+colname+"nd_f.png", dpi=300,orientation='portrait', format="png")

        #plotting cumulative node displacements
        voff=0.1 #in meters
        fig_cd, ax_cd=plot_cumulative_node_disp(num_nodes, cs_xzdf, cs_xydf, cs_xdf, voff)
        plt.savefig(figsavefilepath+colname+"nd_fc.png", dpi=300,orientation='portrait', format="png")


        #plotting column positions time series
        colposperiod='Q' #quarterly
        fig_cp, ax_cp=plot_col_pos_abs(cs_xzdf, cs_xydf, cs_xdf, colposperiod)
        plt.savefig(figsavefilepath+colname+"col_a.png", dpi=300,orientation='portrait', format="png")

        fig_cp, ax_cp=plot_col_pos_rel(cs_xzdf, cs_xydf, cs_xdf, colposperiod)
        plt.savefig(figsavefilepath+colname+"col_r.png", dpi=300,orientation='portrait', format="png")
        print (datetime.now()-start)
        #plt.show()

    plt.close('all')


add



