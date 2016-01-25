from datetime import datetime, date, time, timedelta
import pandas as pd
from pandas.stats.api import ols
import numpy as np
import matplotlib.pyplot as plt
import ConfigParser
from querySenslopeDb import *
from filterSensorData import *
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
proc_monitoring_path=cfg.get('I/O','OutputFilePathMonitoring2')

#file names
proc_monitoring_file = cfg.get('I/O','CSVFormat')

#file headers
LastGoodData_file_headers = cfg.get('I/O','LastGoodData_file_headers').split(',')
proc_monitoring_file_headers = cfg.get('I/O','proc_monitoring_file_headers').split(',')

print proc_monitoring_path

def generate_proc():

    print rt_window_length
    
    #1. setting date boundaries for real-time monitoring window
    roll_window_numpts=int(1+roll_window_length/data_dt)
    end, start, offsetstart=gf.get_rt_window(rt_window_length,roll_window_numpts,num_roll_window_ops)
    sensorlist=GetSensorList()

    # generating proc monitoring data for each site
    print "Generating PROC monitoring data for:"
    for s in sensorlist:     
        
        #2. getting current column properties
        colname,num_nodes,seg_len= s.name,s.nos,s.seglen
        print colname
        print num_nodes
        print seg_len
            
        #3. getting accelerometer data for site 'colname'
        monitoring=GetRawAccelData(colname,offsetstart)

        #4. evaluating which data needs to be filtered
        try:
            monitoring=applyFilters(monitoring)
            LastGoodData=GetLastGoodData(monitoring,num_nodes)
            PushLastGoodData(LastGoodData,colname)
            LastGoodData = GetLastGoodDataFromDb(colname)
            print 'Done'
        except:
            LastGoodData = GetLastGoodDataFromDb(colname)
            print 'error'
        if len(LastGoodData)<num_nodes: print colname, " Missing nodes in LastGoodData"

        #5. extracting last data outside monitoring window
        LastGoodData=LastGoodData[(LastGoodData.ts<offsetstart)]

        #6. appending LastGoodData to monitoring
        monitoring=monitoring.append(LastGoodData)
        
        #7. replacing date of data outside monitoring window with first date of monitoring window
        monitoring.loc[monitoring.ts < offsetstart, ['ts']] = offsetstart

        #8. computing corresponding horizontal linear displacements (xz,xy), and appending as columns to dataframe
        monitoring['xz'],monitoring['xy']=gf.accel_to_lin_xz_xy(seg_len,monitoring.x.values,monitoring.y.values,monitoring.z.values)
        
        #9. removing unnecessary columns x,y,z
        monitoring=monitoring.drop(['x','y','z'],axis=1)

        #10. setting ts as index
#        monitoring['id']=monitoring.index.values
        monitoring=monitoring.set_index('ts')

        #11. reordering columns
        monitoring=monitoring[['id','xz','xy']]
        
        #12. saving proc monitoring data        
        monitoring.to_csv(proc_monitoring_path+"Proc\\"+colname+proc_monitoring_file,sep=',', header=False,mode='w')