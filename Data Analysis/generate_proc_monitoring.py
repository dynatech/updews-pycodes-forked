from datetime import datetime, date, time, timedelta
import pandas as pd
from pandas.stats.api import ols
import numpy as np
import matplotlib.pyplot as plt
import ConfigParser

import generic_functions as gf


cfg = ConfigParser.ConfigParser()
cfg.read('server-config.txt')



    

##set/get values from config file

#time interval between data points, in hours
data_dt = cfg.getfloat('I/O','data_dt')

#length of real-time monitoring window, in days
rt_window_length = cfg.getfloat('I/O','rt_window_length')

#length of rolling/moving window operations in hours
roll_window_length = cfg.getfloat('I/O','roll_window_length')

#number of rolling window operations in the whole monitoring analysis
num_roll_window_ops = cfg.getfloat('I/O','num_roll_window_ops')

#INPUT/OUTPUT FILES

#local file paths
columnproperties_path=cfg.get('I/O','ColumnPropertiesPath')
purged_path=cfg.get('I/O','InputFilePath')
monitoring_path=cfg.get('I/O','MonitoringPath')
LastGoodData_path=cfg.get('I/O','LastGoodData')
proc_monitoring_path=cfg.get('I/O','OutputFilePathMonitoring2')

#file names
columnproperties_file = cfg.get('I/O','ColumnProperties')
purged_file = cfg.get('I/O','CSVFormat')
monitoring_file = cfg.get('I/O','CSVFormat')
LastGoodData_file = cfg.get('I/O','CSVFormat')
proc_monitoring_file = cfg.get('I/O','CSVFormat')

#file headers
columnproperties_headers = cfg.get('I/O','columnproperties_headers').split(',')
purged_file_headers = cfg.get('I/O','purged_file_headers').split(',')
monitoring_file_headers = cfg.get('I/O','monitoring_file_headers').split(',')
LastGoodData_file_headers = cfg.get('I/O','LastGoodData_file_headers').split(',')
proc_monitoring_file_headers = cfg.get('I/O','proc_monitoring_file_headers').split(',')


def generate_proc():
    #MAIN

    #1. setting date boundaries for real-time monitoring window
    roll_window_numpts=int(1+roll_window_length/data_dt)
    end, start, offsetstart=gf.get_rt_window(rt_window_length,roll_window_numpts,num_roll_window_ops)

    #2. getting all column properties
    sensors=pd.read_csv(columnproperties_path+columnproperties_file,names=columnproperties_headers,index_col=None)

##    print "Generating PROC monitoring data for:"
    for s in range(len(sensors)):

        
        
        
        #3. getting current column properties
        colname,num_nodes,seg_len=sensors['colname'][s],sensors['num_nodes'][s],sensors['seg_len'][s]
        
    ##    print "\nDATA for ",colname," as of ", end.strftime("%Y-%m-%d %H:%M")

        try:
            #4. importing monitoring csv file of current column to dataframe
            monitoring=pd.read_csv(monitoring_path+colname+monitoring_file,names=monitoring_file_headers,parse_dates=[0],index_col=[1])

            #5. extracting data within monitoring window
            monitoring=monitoring[(monitoring.ts>=offsetstart)&(monitoring.ts<=end)]
            
            #6. importing LastGoodData csv file of current column to dataframe
            LastGoodData=pd.read_csv(LastGoodData_path+colname+LastGoodData_file,names=LastGoodData_file_headers,parse_dates=[0],index_col=[1])
            if len(LastGoodData)<num_nodes: print colname, " Missing nodes in LastGoodData"

            #7. extracting last data outside monitoring window
            LastGoodData=LastGoodData[(LastGoodData.ts<offsetstart)]
##            print "\n",colname
##            print LastGoodData

            #8. appending LastGoodData to monitoring
            monitoring=monitoring.append(LastGoodData)
##          print monitoring.tail(num_nodes+1)
            
            #9. replacing date of data outside monitoring window with first date of monitoring window
            monitoring.loc[monitoring.ts < offsetstart, ['ts']] = offsetstart

            #10. computing corresponding horizontal linear displacements (xz,xy), and appending as columns to dataframe
            monitoring['xz'],monitoring['xy']=gf.accel_to_lin_xz_xy(seg_len,monitoring.x.values,monitoring.y.values,monitoring.z.values)
            
            #11. removing unnecessary columns x,y,z
            monitoring=monitoring.drop(['x','y','z'],axis=1)

            #12. setting ts as index
            monitoring['id']=monitoring.index.values
            monitoring=monitoring.set_index('ts')

            #13. reordering columns
            monitoring=monitoring[['id','xz','xy','m']]

            
        ##    print "\n",colname
        ##    print monitoring.tail(20)

            monitoring.to_csv(proc_monitoring_path+colname+proc_monitoring_file,sep=',', header=False,mode='w')

##            print "     ",colname

        except:
##            print "     ",colname, "...FAILED"
            continue 
       
