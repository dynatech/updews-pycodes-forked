# -*- coding: utf-8 -*-
"""
Created on Fri Nov 18 13:58:05 2016

@author: Brainerd D. Cruz
"""

import pandas as pd
#import numpy as np
from datetime import timedelta as td
from datetime import datetime as dt
import analysis.querydb as qdb
import matplotlib.pyplot as plt
import analysis.subsurface.filterdata as fsd
import memcache
import numpy as np

import dynadb.db as db
    
def xyzplot(tsm_id,nid,time, OutputFP=''):
    memc = memcache.Client(['127.0.0.1:11211'], debug=1)
    tsm_sensors = memc.get('DF_TSM_SENSORS')
    accelerometers = memc.get('DF_ACCELEROMETERS')
    accel = accelerometers[(accelerometers.tsm_id == tsm_id) & 
                           (accelerometers.in_use == 1)]
    
    
    tsm_name = tsm_sensors.tsm_name[tsm_sensors.tsm_id ==tsm_id].values[0]
    nid_up = nid-1
    nid_down = nid+1
    time = pd.to_datetime(time)
    from_time = time - td(days = 6)
    to_time = time
    
    
#dataframe
    df_node=qdb.get_raw_accel_data(tsm_id = tsm_id,
                                   from_time = time - td(weeks = 1),
                                   to_time = time, analysis = True, batt = True)
    dff=fsd.apply_filters(df_node)

#raw and filter counter    
    raw_count = float(df_node.ts[(df_node.node_id == nid) & 
                                 (df_node.ts >= time - td(days = 3))].count())
    filter_count = dff.ts[(dff.node_id == nid) & 
                          (dff.ts >= time - td(days = 3))].count()
    try:
        percent = filter_count / raw_count  * 100.0
    except ZeroDivisionError:
        percent = 0

#valid invalid
    query_na = ("SELECT COUNT(IF(na_status=1,1, NULL))/count(ts)*100.0 as "
                "'percent_valid' FROM node_alerts "
                "where tsm_id = {} and node_id = {} and na_status is not NULL "
                "group by tsm_id, node_id".format(tsm_id,nid))
    df_na = db.df_read(query_na, connection = "analysis")
    if df_na.empty:
        validity = np.nan
    else:
        validity = df_na.percent_valid.values[0]
    
    
    fig=plt.figure()        
    fig.suptitle("{}{} ({})".format(tsm_name,str(nid),time.strftime("%Y-%m-%d %H:%M")),fontsize=11)
    
#accelerometer status
    query='''SELECT status,remarks FROM accelerometer_status as astat
            inner join 
            (select * from accelerometers where tsm_id={} and node_id={} 
            and in_use=1) as a
            on a.accel_id=astat.accel_id
            order by stat_id desc limit 1'''.format(tsm_id,nid) 
    dfs=db.df_read(query, connection = "analysis")
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
    fig.text(0.900, 0.95, '%%filter/raw = %.2f%%\n%%validity = %.2f%%'%(percent,validity),
         horizontalalignment='right',
         verticalalignment='top',
         fontsize=8,color='blue')    

    df0 = dff[(dff.node_id==nid_up) & (dff.ts>=from_time) & (dff.ts<=to_time)]
    dfr0 = df_node[(df_node.node_id==nid_up) & (df_node.ts>=from_time) & (df_node.ts<=to_time)]
    
    if not df0.empty:      
        df0 = df0.set_index('ts')
        dfr0 = dfr0.set_index('ts')
        ax1 = plt.subplot(3,4,1)
        plt.axvspan(time-td(days=3),time,facecolor='yellow', alpha=0.4)
        df0['x'].plot(color='green')
        ax1.tick_params(axis='both',direction='in', labelsize=7)
        plt.ylabel(tsm_name+str(nid_up), color='green', fontsize=14)
        plt.title('x-axis', color='green',fontsize=8,verticalalignment='top')
        
        ax2 = plt.subplot(3,4,2, sharex = ax1)
        plt.axvspan(time-td(days=3),time,facecolor='yellow', alpha=0.4)
        df0['y'].plot(color='green')
        ax2.tick_params(axis='both',direction='in', labelsize=7)
        plt.title('y-axis', color='green',fontsize=8,verticalalignment='top')
        
        ax3 = plt.subplot(3,4,3, sharex = ax1)
        plt.axvspan(time-td(days=3),time,facecolor='yellow', alpha=0.4)
        df0['z'].plot(color='green')
        ax3.tick_params(axis='both',direction='in', labelsize=7)
        plt.title('z-axis', color='green',fontsize=8,verticalalignment='top')
        
        try:
            axb1 = plt.subplot(3,4,4, sharex = ax1)
            plt.axvspan(time-td(days=3),time,facecolor='yellow', alpha=0.4)
            dfr0['batt'].plot(color='green')
            axb1.tick_params(axis='both',direction='in', labelsize=7)
            axb1.axhline(accel.voltage_max[accel.node_id == nid_up].values[0],color='black', linestyle='--', linewidth=1)
            axb1.axhline(accel.voltage_min[accel.node_id == nid_up].values[0],color='black', linestyle='--', linewidth=1)
            plt.title('batt', color='green',fontsize=8,verticalalignment='top')
        except:
            print ("v1")
        
        plt.xlim([from_time,to_time])
        
#        for t in time:
#        ax1.axvline(time, color='gray', linestyle='--', linewidth=0.7)
#        ax2.axvline(time, color='gray', linestyle='--', linewidth=0.7)
#        ax3.axvline(time, color='gray', linestyle='--', linewidth=0.7)
    
    df = dff[(dff.node_id==nid) & (dff.ts>=from_time) & (dff.ts<=to_time)]
    dfr = df_node[(df_node.node_id==nid) & (df_node.ts>=from_time) & (df_node.ts<=to_time)]
    if not df.empty:      
        df = df.set_index('ts')
        dfr = dfr.set_index('ts')
        ax4 = plt.subplot(3,4,5)
        plt.axvspan(time-td(days=3),time,facecolor='yellow', alpha=0.4)
        df['x'].plot(color='blue')
        ax4.tick_params(axis='both',direction='in', labelsize=7)
        plt.ylabel(tsm_name+str(nid), color='blue', fontsize=14)        
        plt.title('x-axis', color='blue',fontsize=8,verticalalignment='top')
        
        ax5 = plt.subplot(3,4,6, sharex = ax4)
        plt.axvspan(time-td(days=3),time,facecolor='yellow', alpha=0.4)
        df['y'].plot(color='blue')
        ax5.tick_params(axis='both',direction='in', labelsize=7)
        plt.title('y-axis', color='blue',fontsize=8,verticalalignment='top')
        
        ax6 = plt.subplot(3,4,7, sharex = ax4)
        plt.axvspan(time-td(days=3),time,facecolor='yellow', alpha=0.4)
        df['z'].plot(color='blue')
        ax6.tick_params(axis='both',direction='in', labelsize=7)
        plt.title('z-axis', color='blue',fontsize=8,verticalalignment='top')
        
        try:
            axb2 = plt.subplot(3,4,8, sharex = ax4)
            plt.axvspan(time-td(days=3),time,facecolor='yellow', alpha=0.4)
            dfr['batt'].plot(color='blue')
            axb2.tick_params(axis='both',direction='in', labelsize=7)
            axb2.axhline(accel.voltage_max[accel.node_id == nid].values[0],color='black', linestyle='--', linewidth=1)
            axb2.axhline(accel.voltage_min[accel.node_id == nid].values[0],color='black', linestyle='--', linewidth=1)
            plt.title('batt', color='blue',fontsize=8,verticalalignment='top')
        except:
            print ("v1")
        
        plt.xlim([from_time,to_time])
        
#        for t in time:
#        ax4.axvline(time, color='gray', linestyle='--', linewidth=0.7)
#        ax5.axvline(time, color='gray', linestyle='--', linewidth=0.7)
#        ax6.axvline(time, color='gray', linestyle='--', linewidth=0.7)
    
    df1 = dff[(dff.node_id==nid_down) & (dff.ts>=from_time) & (dff.ts<=to_time)]
    dfr1 = df_node[(df_node.node_id==nid_down) & (df_node.ts>=from_time) & (df_node.ts<=to_time)]
    if not df1.empty:      
        df1 = df1.set_index('ts')
        dfr1 = dfr1.set_index('ts')
        
        ax7 = plt.subplot(3,4,9)
        df1['x'].plot(color='red')
        ax7.tick_params(axis='both',direction='in', labelsize=7)
        plt.axvspan(time-td(days=3),time,facecolor='yellow', alpha=0.4)
        plt.ylabel(tsm_name+str(nid_down), color='red', fontsize=14)
        plt.title('x-axis', color='red',fontsize=8,verticalalignment='top')
        
        ax8 = plt.subplot(3,4,10, sharex = ax7)
        plt.axvspan(time-td(days=3),time,facecolor='yellow', alpha=0.4)
        df1['y'].plot(color='red')
        ax8.tick_params(axis='both',direction='in', labelsize=7)
        plt.title('y-axis', color='red',fontsize=8,verticalalignment='top')
        
        ax9 = plt.subplot(3,4,11, sharex = ax7)
        plt.axvspan(time-td(days=3),time,facecolor='yellow', alpha=0.4)
        df1['z'].plot(color='red')
        ax9.tick_params(axis='both',direction='in', labelsize=7)
        plt.title('z-axis', color='red',fontsize=8,verticalalignment='top')
        
        try:
            axb3 = plt.subplot(3,4,12, sharex = ax7)
            plt.axvspan(time-td(days=3),time,facecolor='yellow', alpha=0.4)
            dfr1['batt'].plot(color='red')
            axb3.tick_params(axis='both',direction='in', labelsize=7)
            axb3.axhline(accel.voltage_max[accel.node_id == nid_down].values[0],color='black', linestyle='--', linewidth=1)
            axb3.axhline(accel.voltage_min[accel.node_id == nid_down].values[0],color='black', linestyle='--', linewidth=1)
            plt.title('batt', color='red',fontsize=8,verticalalignment='top')
        except:
            print ("v1")
        plt.xlim([from_time,to_time])
        
#        for t in time:
#        ax7.axvline(time, color='gray', linestyle='--', linewidth=0.7)
#        ax8.axvline(time, color='gray', linestyle='--', linewidth=0.7)
#        ax9.axvline(time, color='gray', linestyle='--', linewidth=0.7)
#    plt.show()

    plt.savefig(OutputFP+tsm_name+str(nid)+'('+time.strftime("%Y-%m-%d %H%M")+')', dpi=400)


        

#    

#    times=str(time[0])
#    times=times.replace(":", "")
#
#    print (tsm_name+nids+'('+times+')')
#    
##    plt.tight_layout()
#    
#    plt.savefig(OutputFP+tsm_name+nids+'('+times+')', dpi=400)
#    plt.close()
#    
#    
#time=pd.Series([])
##    
#
#
#tsm_name='dadta'
#tsm_id=25
#nid=6
#time='2018-01-10 03:00:00'
#time=pd.to_datetime(time)
#
#df_node=qdb.get_raw_accel_data(tsm_id=tsm_id, from_time=time-td(days=7), to_time=time,
#                               analysis=True)    
#dff=fsd.apply_filters(df_node, orthof=True, rangef=True, outlierf=True)    
#
#xyzplot(tsm_id = 1,nid = 2,time= "2019-02-27 11:00:00")    

