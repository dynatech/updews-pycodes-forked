import pandas as pd
import numpy as np
from numpy import nan
import scipy.stats.stats as st
import matplotlib.pyplot as plt
import os
import csv
from datetime import datetime, date, time, timedelta
import gc


import Col_Nod_Plots_with_fill_and_resamp as CNPfr 



def movingLR(fr_xzdf, fr_xydf, fr_xdf, window_period):
    ts_xz=fr_xzdf.reset_index()
    ts_xy=fr_xydf.reset_index()
    ts_x=fr_xdf.reset_index()
    
    tdelta=ts_xz['Time']-ts_xz['Time'][0]
    tdelta=tdelta.astype('timedelta64[s]')/(60*60*24.)  #in days

    for col in fr_xzdf.columns:
        print col
        #if col!=7:continue
        ts_xz[col].plot()
        model=pd.ols(y=ts_xz[col], x=tdelta,window_type='rolling', window=window_period, intercept=True)
        ts_xz[col]=np.round(model.beta.x,3)
        ts_xz[col].plot()

        model=pd.ols(y=ts_xy[col], x=tdelta,window_type='rolling', window=window_period, intercept=True)
        ts_xy[col]=np.round(model.beta.x,3)

        model=pd.ols(y=ts_x[col], x=tdelta,window_type='rolling', window=window_period, intercept=True)
        ts_x[col]=np.round(model.beta.x,3)

        plt.show()
        
    ts_xz.index=ts_xz['Time']
    ts_xy.index=ts_xz['Time']
    ts_x.index=ts_xz['Time']

    #ts_xz.plot()
    #fr_xzdf[7].plot()





#CONSTANTS and SETTINGS
loc_col_list=("eeet","sinb","sint","sinu","lipb","lipt","bolb","pugb","pugt","mamb","mamt","oslt","oslb","labt", "labb", "gamt","gamb", "humt","humb", "plat","plab","blct","blcb")
num_nodes_loc_col=(14,29,19,29,28,31,30,14,10,29,24,21,23,39,25,18,22,21,26,39,40,24,19)
col_seg_len_list=(0.5,1,1,1,0.5,0.5,0.5,1.2,1.2,1.0,1.0,1.,1.,1.,1.,1.,1.,1.,1,0.5,0.5,1,1)
col = ['Time','Node_ID', 'x', 'y', 'z', 'good_tilt', 'xz', 'xy', 'phi', 'rho', 'moi', 'good_moi']
usecol = ['Time','Node_ID','good_tilt', 'xz', 'xy']
fig_size=(9.5,6.5)


#set file path for input *.proc files
csvfilepath="/home/egl-sais/Dropbox/Senslope Data/Proc/csv/"

#set file path for saving figures
figsavefilepath="/home/egl-sais/Desktop/sensorfigs/"

#set this to 1 to plot figures
plotfigs=1

#set this to 1 to save figures, 0 to just display the output
savefigs=0

#set this to desired node date sampling interval: D=daily, 3H=3-hourly, M=monthly, Q=quarterly, etc...
resampind='D'

#set this to desired interval between column positions: D=daily, 3H=3-hourly, M=monthly, Q=quarterly, etc...
colposperiod='Q'
    



#MAIN

print "here"
for INPUT_which_sensor in range(1,len(loc_col_list)):
    if INPUT_which_sensor!=3:
        continue
    colname,num_nodes,seg_len=CNPfr.Input_Loc_Col(loc_col_list,num_nodes_loc_col,col_seg_len_list, INPUT_which_sensor)
    print loc_col_list
    print "\n",colname, num_nodes, seg_len
    start=datetime.now()
    
    #reading from csv file and writing to dataframe
    xzdf, xydf, xdf = CNPfr.df_from_csv(csvfilepath,colname,col,usecol,seg_len,num_nodes)


    
    #resampling and filling XZ, XY and X dataframes
    resampind='30Min'
    fr_xzdf, fr_xydf, fr_xdf = CNPfr.resamp_fill_df(resampind, xzdf,xydf,xdf)


    
    #fr_xzdf=pd.rolling_mean(fr_xzdf,window=48)
    #fr_xydf=pd.rolling_mean(fr_xydf,window=48)
    #fr_xdf=pd.rolling_mean(fr_xdf,window=48)

    #movingLR(fr_xzdf, fr_xydf, fr_xdf, 12)
    #plt.show()

    plt.close('all')






