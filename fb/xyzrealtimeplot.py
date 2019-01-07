# -*- coding: utf-8 -*-
"""
Created on Fri Nov 18 13:58:05 2016

@author: Brainerd D. Cruz
"""

import pandas as pd
#import numpy as np
from datetime import timedelta as td
#from datetime import datetime as dt
import querydb as qdb
import matplotlib.pyplot as plt
import filterdata as fsd

    
def xyzplot(df_node, tsm_id,nid,time, OutputFP='', percent=0):
    col=df_node.tsm_name.values[0]
    nid_up=nid-1
    nid_down=nid+1
    fromTime=time-td(days=6)
    toTime=time
    
    fig=plt.figure()        
    fig.suptitle("{}{} ({})".format(col,str(nid),time.strftime("%Y-%m-%d %H:%M")),fontsize=11)
    
    #accelerometer status
    query='''SELECT status,remarks FROM senslopedb.accelerometer_status as astat
            inner join 
            (select * from accelerometers where tsm_id={} and node_id={} 
            and in_use=1) as a
            on a.accel_id=astat.accel_id
            order by stat_id desc limit 1'''.format(tsm_id,nid) 
    dfs=qdb.get_db_dataframe(query)
    if not dfs.empty:
        stat_id=dfs.status[0]
        if stat_id==1:
            stat='Ok'
        elif stat_id==2:
            stat='Use with Caution'
        elif stat_id==3:
            stat='Special Case'
        elif stat_id==4:
            stat='Not Ok'

        com=dfs.remarks[0]
    else:
        stat='Ok'
        com=''
    
    fig.text(0.125, 0.95, 'Status: {}\nComment: {}'.format(stat,com),
         horizontalalignment='left',
         verticalalignment='top',
         fontsize=8,color='blue')
    # end of accelerometer status
    
    #filter/raw
    fig.text(0.900, 0.95, '%%filter/raw = %.2f%%'%percent,
         horizontalalignment='right',
         verticalalignment='top',
         fontsize=8,color='blue')    

    df0 = df_node[(df_node.node_id==nid_up) & (df_node.ts>=fromTime) & (df_node.ts<=toTime)]
    if not df0.empty:      
        df0 = df0.set_index('ts')
    
        ax1 = plt.subplot(3,3,1)
#        plt.fill_between([time-td(days=3),time],max(df0['x']), min(df0['x']), color='yellow', alpha=0.4)
        plt.axvspan(time-td(days=3),time,facecolor='yellow', alpha=0.4)
        df0['x'].plot(color='green')
        plt.ylabel(col+str(nid_up), color='green', fontsize=14)
        plt.title('x-axis', color='green',fontsize=8,verticalalignment='top')
        
        ax2 = plt.subplot(3,3,2, sharex = ax1)
#        plt.fill_between([time-td(days=3),time],max(df0['y']), min(df0['y']), color='yellow', alpha=0.4)
        plt.axvspan(time-td(days=3),time,facecolor='yellow', alpha=0.4)
        df0['y'].plot(color='green')
        plt.title('y-axis', color='green',fontsize=8,verticalalignment='top')
        
        ax3 = plt.subplot(3,3,3, sharex = ax1)
#        plt.fill_between([time-td(days=3),time],max(df0['z']), min(df0['z']), color='yellow', alpha=0.4)
        plt.axvspan(time-td(days=3),time,facecolor='yellow', alpha=0.4)
        df0['z'].plot(color='green')
        plt.title('z-axis', color='green',fontsize=8,verticalalignment='top')
        
        plt.xlim([fromTime,toTime])
        
#        for t in time:
#            ax1.axvline(t, color='k', linestyle='--')
#            ax2.axvline(t, color='k', linestyle='--')
#            ax3.axvline(t, color='k', linestyle='--')
    
    df = df_node[(df_node.node_id==nid) & (df_node.ts>=fromTime) & (df_node.ts<=toTime)]
    if not df.empty:      
        df = df.set_index('ts')
    
        ax4 = plt.subplot(3,3,4)
#        plt.fill_between([time-td(days=3),time],max(df['x']), min(df['x']), color='yellow', alpha=0.4)
        plt.axvspan(time-td(days=3),time,facecolor='yellow', alpha=0.4)
        df['x'].plot(color='blue')
        plt.ylabel(col+str(nid), color='blue', fontsize=14)        
        plt.title('x-axis', color='blue',fontsize=8,verticalalignment='top')
        
        ax5 = plt.subplot(3,3,5, sharex = ax4)
#        plt.fill_between([time-td(days=3),time],max(df['y']), min(df['y']), color='yellow', alpha=0.4)
        plt.axvspan(time-td(days=3),time,facecolor='yellow', alpha=0.4)
        df['y'].plot(color='blue')
        plt.title('y-axis', color='blue',fontsize=8,verticalalignment='top')
        
        ax6 = plt.subplot(3,3,6, sharex = ax4)
#        plt.fill_between([time-td(days=3),time],max(df['z']), min(df['z']), color='yellow', alpha=0.4)
        plt.axvspan(time-td(days=3),time,facecolor='yellow', alpha=0.4)
        df['z'].plot(color='blue')
        plt.title('z-axis', color='blue',fontsize=8,verticalalignment='top')
        
        plt.xlim([fromTime,toTime])
        
#        for t in time:
#            ax4.axvline(t, color='k', linestyle='--')
#            ax5.axvline(t, color='k', linestyle='--')
#            ax6.axvline(t, color='k', linestyle='--')
    
    df1 = df_node[(df_node.node_id==nid_down) & (df_node.ts>=fromTime) & (df_node.ts<=toTime)]
    if not df1.empty:      
        df1 = df1.set_index('ts')
 
        ax7 = plt.subplot(3,3,7)
        df1['x'].plot(color='red')
#        plt.fill_between([time-td(days=3),time],max(df1['x']), min(df1['x']), color='yellow', alpha=0.4)
        plt.axvspan(time-td(days=3),time,facecolor='yellow', alpha=0.4)
        plt.ylabel(col+str(nid_down), color='red', fontsize=14)
        plt.title('x-axis', color='red',fontsize=8,verticalalignment='top')
        
        ax8 = plt.subplot(3,3,8, sharex = ax7)
#        plt.fill_between([time-td(days=3),time],max(df1['y']), min(df1['y']), color='yellow', alpha=0.4)
        plt.axvspan(time-td(days=3),time,facecolor='yellow', alpha=0.4)
        df1['y'].plot(color='red')
        plt.title('y-axis', color='red',fontsize=8,verticalalignment='top')
        
        ax9 = plt.subplot(3,3,9, sharex = ax7)
#        plt.fill_between([time-td(days=3),time],max(df1['z']), min(df1['z']), color='yellow', alpha=0.4)
        plt.axvspan(time-td(days=3),time,facecolor='yellow', alpha=0.4)
        df1['z'].plot(color='red')
        plt.title('z-axis', color='red',fontsize=8,verticalalignment='top')
        
        plt.xlim([fromTime,toTime])
#    plt.show()

    plt.savefig(OutputFP+col+str(nid)+'('+time.strftime("%Y-%m-%d %H%M")+')', dpi=400)

#        for t in time:
#            ax7.axvline(t, color='k', linestyle='--')
#            ax8.axvline(t, color='k', linestyle='--')
#            ax9.axvline(t, color='k', linestyle='--')
        

#    

#    times=str(time[0])
#    times=times.replace(":", "")
#
#    print (col+nids+'('+times+')')
#    
##    plt.tight_layout()
#    
#    plt.savefig(OutputFP+col+nids+'('+times+')', dpi=400)
#    plt.close()
#    
#    
#time=pd.Series([])
##    
#
#
#col='dadta'
#tsm_id=25
#nid=6
#time='2018-01-10 03:00:00'
#time=pd.to_datetime(time)
#
#df_node=qdb.get_raw_accel_data(tsm_id=tsm_id, from_time=time-td(days=7), to_time=time,
#                               analysis=True)    
#dff=fsd.apply_filters(df_node, orthof=True, rangef=True, outlierf=True)    
#
#xyzplot(dff,tsm_id=tsm_id,nid=nid,time=time)    

