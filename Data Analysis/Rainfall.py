import pandas as pd
import numpy as np
import ConfigParser
import os.path

import rainDownload
import generic_functions as gf


cfg = ConfigParser.ConfigParser()
cfg.read('server-config.txt')    
#
##set/get values from config file
#
##time interval between data points, in hours
data_dt = cfg.getfloat('I/O','data_dt')
#
##length of real-time monitoring window, in days
rt_window_length = cfg.getfloat('I/O','rt_window_length')
#
##length of rolling/moving window operations in hours
roll_window_length = cfg.getfloat('I/O','roll_window_length')
#
##number of rolling window operations in the whole monitoring analysis
num_roll_window_ops = cfg.getfloat('I/O','num_roll_window_ops')

#file paths
proc_monitoring_path = cfg.get('I/O','OutputFilePathMonitoring2')
rainfall_path1 = cfg.get('I/O','RainfallFilePath_Dyna')
rainfall_path2 = cfg.get('I/O','RainfallFilePath_NOAH')
#file format
rainfall_file = cfg.get('I/O','CSVFormat')


def set_monitoring_window(roll_window_length,data_dt,rt_window_length,num_roll_window_ops):

    roll_window_numpts=int(1+roll_window_length/data_dt)
    end, start, offsetstart=gf.get_rt_window(rt_window_length,roll_window_numpts,num_roll_window_ops)
    monwin_time=pd.date_range(start=start, end=end, freq='15Min',name='ts', closed=None)
    monwin=pd.DataFrame(data=np.nan*np.ones(len(monwin_time)), index=monwin_time)

    return roll_window_numpts, end, start, offsetstart, monwin
    
    
    
#Setting monitoring window
roll_window_numpts, end, start, offsetstart, monwin = set_monitoring_window(roll_window_length,data_dt,rt_window_length,num_roll_window_ops)

#Extracting 2-yearm maximum rainfall value of all sites
properties=pd.read_csv('C:\Users\Piere\Desktop\updewscodes\trunk\Data Analysis\dynaslope_sites.csv',
                       header=False,usecols=[8,16],index_col=[0])
#Setting threshold value on all sites
properties['threshold']=properties.iloc[:,0]*1/2


def get_NOAH(r):
    #Acquiring data from NOAH weather station if no weather station from Dynaslope
    if os.path.exists(rainfall_path2+r+rainfall_file):

        if os.stat(rainfall_path2+r+rainfall_file).st_size != 0:
            rainfall=pd.read_csv(rainfall_path2+r+rainfall_file,index_col=[0],
                                 usecols=[0,5],parse_dates=[0])  
            rainfall.columns=[r+'2'] 
        else:

            rainfall=monwin
            rainfall.columns=[r+'3']
        
    else:

        rainfall=monwin
        rainfall.columns=[r+'3']
        
    return rainfall

def get_rain(monwin,properties,end,start):
    monwin=monwin.rename(columns={0:'skel'})
    rain_list=[monwin]
    for r in list(properties.index):

        #Gathering rainfall data for the past 'rt_window_length' days
        #Acquiring data from Dynaslope weather station if it exist
        if os.path.exists(rainfall_path1+r+'w'+rainfall_file):
            rain=pd.read_csv(rainfall_path1+r+'w'+rainfall_file,index_col=[0],
                             usecols=[0,5],parse_dates=[0])                
            if len(rain[(rain.index>=start)&(rain.index<=end)]) != 0:
                rain=rain          
                rain.columns=[r+'1']           
            
            #Acquiring data from NOAH weather sation if no data from Dynaslope weather station
            else:
                rain=get_NOAH(r)
                
        #Acquiring data from NOAH weather sation if Dynaslope weather station does not exist
        else:
            rain=get_NOAH(r)

        
        #merging raifall data from all sites
        rain=pd.concat([rain,monwin],axis=1,join='outer',names=[rain.columns,monwin.columns])
        
        #limiting data within monitoring window      
        rain=rain[(rain.index>=start)&(rain.index<=end)]
        
        #dropping skeleton dataframe
        rain=rain.drop('skel',1)
        
        #resampling data into 15min interval
        rain=rain.resample('15min',how='mean')
        
        #computing 24h cumulative rainfall
        rain=pd.rolling_sum(rain,96,min_periods=1)
        
        #normalizing rainfall data with respect to the threshold value
        rain=rain/properties.loc[r,'threshold']
        rain_list.append(rain)
        
    #creating normalized dataframe for graphing
    rain_df=pd.concat(rain_list, axis=1, join='outer', names=None)
    
    #dropping skeleton dataframe
    rain_df=rain_df.drop('skel',1)

    return rain_df
     
        
rain_df=get_rain(monwin,properties,end,start)   
##rain_df.to_csv('C:/DB Mount/Dropbox/Trials/rain-graph.csv',header=True,mode='w')

    
