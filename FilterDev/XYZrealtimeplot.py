# -*- coding: utf-8 -*-
"""
Created on Fri Nov 18 13:58:05 2016

@author: Brainerd D. Cruz
"""

import pandas as pd
#import numpy as np
from datetime import timedelta as td
#from datetime import datetime as dt
import querySenslopeDb as qdb
import matplotlib.pyplot as plt
import filterSensorData as fsd

    
def xyzplot(df_node, col,nid,time):
    nid_up=nid-1
    nid_down=nid+1
    fromTime=time-td(days=6)
    toTime=time
    
    
    df0 = df_node[(df_node.id==nid_up) & (df_node.ts>=fromTime) & (df_node.ts<=toTime)]
    if not df0.empty:      
        df0 = df0.set_index('ts')
    
        ax1 = plt.subplot(3,3,1)
        df0['x'].plot(color='green')
        plt.ylabel(col+str(nid_up), color='green', fontsize=14)
        plt.title('x-axis', color='green')
        
        ax2 = plt.subplot(3,3,2, sharex = ax1)
        df0['y'].plot(color='green')
        plt.title('y-axis', color='green')
        
        ax3 = plt.subplot(3,3,3, sharex = ax1)
        df0['z'].plot(color='green')
        plt.title('z-axis', color='green')
        
        plt.xlim([fromTime,toTime])
        
#        for t in time:
#            ax1.axvline(t, color='k', linestyle='--')
#            ax2.axvline(t, color='k', linestyle='--')
#            ax3.axvline(t, color='k', linestyle='--')
    
    df = df_node[(df_node.id==nid) & (df_node.ts>=fromTime) & (df_node.ts<=toTime)]
    if not df.empty:      
        df = df.set_index('ts')
    
        ax4 = plt.subplot(3,3,4)
        df['x'].plot(color='blue')
        plt.ylabel(col+str(nid), color='blue', fontsize=14)        
        plt.title('x-axis', color='blue')
        
        ax5 = plt.subplot(3,3,5, sharex = ax4)
        df['y'].plot(color='blue')
        plt.title('y-axis', color='blue')
        
        ax6 = plt.subplot(3,3,6, sharex = ax4)
        df['z'].plot(color='blue')
        plt.title('z-axis', color='blue')
        
        plt.xlim([fromTime,toTime])
        
#        for t in time:
#            ax4.axvline(t, color='k', linestyle='--')
#            ax5.axvline(t, color='k', linestyle='--')
#            ax6.axvline(t, color='k', linestyle='--')
    
    df1 = df_node[(df_node.id==nid_down) & (df_node.ts>=fromTime) & (df_node.ts<=toTime)]
    if not df1.empty:      
        df1 = df1.set_index('ts')
 
        ax7 = plt.subplot(3,3,7)
        df1['x'].plot(color='red')
        plt.ylabel(col+str(nid_down), color='red', fontsize=14)
        plt.title('x-axis', color='red')
        
        ax8 = plt.subplot(3,3,8, sharex = ax7)
        df1['y'].plot(color='red')
        plt.title('y-axis', color='red')
        
        ax9 = plt.subplot(3,3,9, sharex = ax7)
        df1['z'].plot(color='red')
        plt.title('z-axis', color='red')
        
        plt.xlim([fromTime,toTime])
        
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
#    

#
#col='sinb'
#nid=3
#time='2017-03-24 07:00:00'
#time=pd.to_datetime(time)
#
#df_node=qdb.GetRawAccelData(siteid=col, fromTime=time-td(days=7), toTime='2017-03-24 11:30')    
#dff=fsd.applyFilters(df_node, orthof=True, rangef=True, outlierf=True)    
#
#xyzplot(dff,col=col,nid=nid,time=time)    