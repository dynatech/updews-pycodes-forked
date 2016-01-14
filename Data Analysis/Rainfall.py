import os
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import ConfigParser
import math

import generic_functions as gf
import rainDownload as rd

plt.ioff()

STARTTIME = datetime.now()

def set_monitoring_window(roll_window_length,data_dt,rt_window_length,num_roll_window_ops):

    ##DESCRIPTION:    
    ##returns number of data points per rolling window, endpoint of interval, starting point of interval, starting point of interval with offset to account for moving window operations, empty dataframe
    
    ##INPUT:
    ##roll_window_length; float; length of rolling/moving window operations, in hours
    ##data_dt; float; time interval between data points, in hours    
    ##rt_window_length; float; length of real-time monitoring window, in days
    ##num_roll_window_ops; float; number of rolling window operations in the whole monitoring analysis
    
    ##OUTPUT:
    ##roll_window_numpts, end, start, offsetstart, monwin
    
    roll_window_numpts=int(1+roll_window_length/data_dt)
    end, start, offsetstart=gf.get_rt_window(rt_window_length,roll_window_numpts,num_roll_window_ops)
    monwin_time=pd.date_range(start=start, end=end, freq='30Min',name='ts', closed=None)
    monwin=pd.DataFrame(data=np.nan*np.ones(len(monwin_time)), index=monwin_time)

    return roll_window_numpts, end, start, offsetstart, monwin

def ASTIplot(r,offsetstart,end,tsn):

    ##DESCRIPTION:
    ##prints timestamp and intsantaneous rainfall
    ##plots instantaneous rainfall data, 24-hr cumulative and 72-hr rainfall, and half of 2-yr max and 2-yr max rainfall for 10 days

    ##INPUT:
    ##r; string; site code
    ##offsetstart; datetime; starting point of interval with offset to account for moving window operations
    ##end; datetime; end of interval
    ##tsn; string; datetime format allowed in savefig

##    if r!='lipw': continue

    rainfall=pd.read_csv("C:\Users\Dynaslope\Desktop\Fresh\Local\ASTI\\"+r+rainfall_file,parse_dates='timestamp',
                         names=['timestamp','rain'],index_col='timestamp')
    rainfall=rainfall[(rainfall.index>=offsetstart)]
    rainfall=rainfall[(rainfall.index<=end)]
    rainfall=rainfall.resample('15min',how='sum')

    plt.xticks(rotation=70, size=5)
    
    #getting the rolling sum for the last24 hours
    rainfall2=pd.rolling_sum(rainfall,96,min_periods=1)
    rainfall2=np.round(rainfall2,4)
    rainfall2.to_csv(proc_monitoring_path+'CumSum Rainfall//'+r+' 1d'+proc_monitoring_file,sep=',',mode='w')
    
    #getting the rolling sum for the last 3 days
    rainfall3=pd.rolling_sum(rainfall,288,min_periods=1)
    rainfall3=np.round(rainfall3,4)
    rainfall3.to_csv(proc_monitoring_path+'CumSum Rainfall//'+r+' 3d'+proc_monitoring_file,sep=',',mode='w')
    
    #assigning the thresholds to their own columns for plotting 
    sub=base
    sub['maxhalf'] = halfmax  
    sub['max'] = twoyrmax

    #assigning df to plot variables (to avoid caveats ? expressed from Spyder)
    plot1=rainfall              # instantaneous rainfall data
    plot2=rainfall2             # 24-hr cumulative rainfall
    plot3=rainfall3             # 72-hr cumulative rainfall
    plot4=sub['maxhalf']        # half of 2-yr max rainfall
    plot5=sub['max']            # 2-yr max rainfall
            
    plt.plot(plot1.index,plot1,color='#db4429') # instantaneous rainfall data
    plt.plot(plot2.index,plot2,color='#5ac126') # 24-hr cumulative rainfall
    plt.plot(plot3.index,plot3,color="#0d90d0") # 72-hr cumulative rainfall
    plt.plot(plot4.index,plot4,color="#fbb714") # half of 2-yr max rainfall
    plt.plot(plot5.index,plot5,color="#963bd6")  # 2-yr max rainfall
    plt.savefig('C:\Users\Dynaslope\Desktop\\Fresh\Local\Rainfall Plots\\'+tsn+" "+r,
        dpi=320, facecolor='w', edgecolor='w',orientation='landscape',mode='w')
    plt.close()
    print rainfall[-1:]

def onethree_val_writer(colname):

    ##DESCRIPTION:
    ##returns cumulative sum for one day and three days

    ##INPUT:
    ##colname; string; site code

    ##OUTPUT:
    ##one, three

    try:
        one=pd.read_csv(proc_monitoring_path+'CumSum Rainfall//'+colname+' 1d'+proc_monitoring_file,sep=',', header = 0, names = ['timestamp', 'rain'], index_col = 'timestamp')

        if end - pd.to_datetime(one.index[-1:]) > timedelta(1):
            blankdf_time=pd.date_range(start=start_time, end=end, freq='15Min',name='timestamp', closed=None)
            blankdf=pd.DataFrame(data=np.nan*np.ones(len(blankdf_time)), index=blankdf_time,columns=['rain'])
            blankdf=blankdf[-1:]
            one=one.append(blankdf)

        one = float(one.rain[-1:])
     
        three=pd.read_csv(proc_monitoring_path+'CumSum Rainfall//'+colname+' 3d'+proc_monitoring_file,sep=',', header = 0, names = ['timestamp', 'rain'], index_col = 'timestamp')

        if end - pd.to_datetime(three.index[-1:]) > timedelta(1):
            blankdf_time=pd.date_range(start=start_time, end=end, freq='15Min',name='timestamp', closed=None)
            blankdf=pd.DataFrame(data=np.nan*np.ones(len(blankdf_time)), index=blankdf_time,columns=['rain'])
            blankdf=blankdf[-1:]
            three=three.append(blankdf)

        three = float(three.rain[-1:])

    except:
        one=None
        three=None
    
    return one,three
        
def summary_writer(s,r,datasource,twoyrmax,halfmax,summary,alert):

    ##DESCRIPTION:
    ##inserts data to summary

    ##INPUT:
    ##s; float; index    
    ##r; string; site code
    ##datasource; string; source of data: ASTI1-3, SENSLOPE Rain Gauge
    ##twoyrmax; float; 2-yr max rainfall, threshold for three day cumulative rainfall
    ##halfmax; float; half of 2-yr max rainfall, threshold for one day cumulative rainfall
    ##summary; dataframe; contains site codes with its corresponding one and three days cumulative sum, data source, alert level and advisory
    ##alert; array; alert summary container, r0 sites at alert[0], r1a sites at alert[1], r1b sites at alert[2],  nd sites at alert[3]

    one,three=onethree_val_writer(r)
    if one>=halfmax or three>=twoyrmax:
        ralert='r1'
        advisory='Start/Continue monitoring'
        if one>=halfmax and three>=twoyrmax:
            alert[1].append(r+' ('+str(one)+')')
            alert[2].append(r+' ('+str(three)+')')
        elif one>=halfmax:
            alert[1].append(r+' ('+str(one)+')')
        else:
            alert[2].append(r+' ('+str(three)+')')        
    elif one==None or math.isnan(one):
        ralert='nd'
        advisory='---'
        alert[3].append(r)
    else:
        ralert='r0'
        advisory='---'
        alert[0].append(r)
    summary.loc[s]=[r,one,three,datasource,ralert,advisory]
            
cfg = ConfigParser.ConfigParser()
cfg.read('IO-config.txt')    

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
proc_monitoring_path = cfg.get('I/O','OutputFilePathMonitoring')
rainfall_path = cfg.get('I/O','RainfallFilePath')
ASTIpath = cfg.get('I/O', 'ASTIpath')

#file names
proc_monitoring_file = cfg.get('I/O','CSVFormat')
rainfall_file = cfg.get('I/O','CSVFormat')

#ALERT CONSTANTS
T_disp = cfg.getfloat('I/O','T_disp')  #m
T_velA1 = cfg.getfloat('I/O','T_velA1') #m/day
T_velA2 = cfg.getfloat('I/O','T_velA2')  #m/day
k_ac_ax = cfg.getfloat('I/O','k_ac_ax')
num_nodes_to_check = cfg.getint('I/O','num_nodes_to_check')



#1. setting monitoring window
roll_window_numpts, end, start, offsetstart, monwin = set_monitoring_window(roll_window_length,data_dt,rt_window_length,num_roll_window_ops)

start_time = end - timedelta(days=1)

index = pd.date_range(end-timedelta(10), periods=11, freq='D')
columns=['maxhalf','max']
base = pd.DataFrame(index=index, columns=columns)

tsn=end.strftime("%Y-%m-%d %H-%M-%S")
tmx=pd.read_csv("C:\Users\Dynaslope\Desktop\Fresh\Local\\rainlist.csv",
                         names=['site','twoyrmx'],index_col=None)
index=range(len(tmx))
columns=['site','1D','3D','DataSource','alert','advisory']
summary = pd.DataFrame(index=index, columns=columns)


#alert summary container, r0 sites at alert[0], r1a sites at alert[1], r1b sites at alert[2],  nd sites at alert[3]
alert = [[],[],[],[]]



for s in range(len(tmx)):
    
    r,twoyrmax=tmx['site'][s],tmx['twoyrmx'][s]
    halfmax=twoyrmax/2
    print r
    
    try:
        print"\n"
        print "Generating Rainfall plots for "+r+" from rain gauge data"
    ##    if r!='lipw': continue
        rainfall=pd.read_csv(rainfall_path+r+rainfall_file,parse_dates='timestamp',
                             usecols=['timestamp','rain'],index_col='timestamp')
        if rainfall.index[-1:]<end:
            blankdf_time=pd.date_range(start=start_time, end=end, freq='15Min',name='timestamp', closed=None)
            blankdf=pd.DataFrame(data=np.nan*np.ones(len(blankdf_time)), index=blankdf_time,columns=['rain'])
            blankdf=blankdf[-1:]
            rainfall=rainfall.append(blankdf)
        rainfall=rainfall[(rainfall.index>=start_time)]
        rainfall=rainfall[(rainfall.index<=end)]
        rainfall=rainfall.resample('15min',how='sum')
        print rainfall
        rain_timecheck=rainfall[(rainfall.index>=end-timedelta(days=1))]
        if len(rain_timecheck.dropna())<1:
            print "No data within desired window"
            print "Generating Rainfall ASTI plots for "+r+" due to lack of rain gauge data within desired window"
            for n in range(1,4):            
                rd.getrain(r, n)
                if os.stat(ASTIpath+r+rainfall_file).st_size != 0:
                    a = pd.read_csv(ASTIpath+r+rainfall_file,parse_dates='timestamp', names=['timestamp','rain'])
                    latest_ts = pd.to_datetime(a[0:1]['timestamp'].values[0])
                    if end - latest_ts < timedelta(hours=0.5):
                        break
                
            
            ASTIplot(r,offsetstart,end,tsn)
            datasource="ASTI" + str(n) + " (Empty Rain Gauge Data)"
            summary_writer(s,r,datasource,twoyrmax,halfmax,summary,alert)
                    
        else:
            plt.xticks(rotation=70, size=5)       
            
            #getting the rolling sum for the last24 hours
            rainfall2=pd.rolling_sum(rainfall,96,min_periods=1)
            rainfall2=np.round(rainfall2,4)
            rainfall2.to_csv(proc_monitoring_path+'CumSum Rainfall//'+r+' 1d'+proc_monitoring_file,sep=',',mode='w')
            
            #getting the rolling sum for the last 3 days
            rainfall3=pd.rolling_sum(rainfall,288,min_periods=1)
            rainfall3=np.round(rainfall3,4)
            rainfall3.to_csv(proc_monitoring_path+'CumSum Rainfall//'+r+' 3d'+proc_monitoring_file,sep=',',mode='w')
            
            #assigning the thresholds to their own columns for plotting 
            sub=base
            sub['maxhalf'] = halfmax  
            sub['max'] = twoyrmax
    
            #assigning df to plot variables (to avoid caveats ? expressed from Spyder)
            plot1=rainfall              # instantaneous rainfall data
            plot2=rainfall2             # 24-hr cumulative rainfall
            plot3=rainfall3             # 72-hr cumulative rainfall
            plot4=sub['maxhalf']        # half of 2-yr max rainfall
            plot5=sub['max']            # 2-yr max rainfall

            plt.plot(plot1.index,plot1,color='#db4429') # instantaneous rainfall data
            plt.plot(plot2.index,plot2,color='#5ac126') # 24-hr cumulative rainfall
            plt.plot(plot3.index,plot3,color="#0d90d0") # 72-hr cumulative rainfall
            plt.plot(plot4.index,plot4,color="#fbb714") # half of 2-yr max rainfall
            plt.plot(plot5.index,plot5,color="#963bd6")  # 2-yr max rainfall
            plt.savefig('C:\Users\Dynaslope\Desktop\\Fresh\Local\Rainfall Plots\\'+tsn+" "+r,
                dpi=320, facecolor='w', edgecolor='w',orientation='landscape',mode='w')
            plt.close()
            print rainfall[-1:]           
            datasource="SENSLOPE Rain Gauge"
            summary_writer(s,r,datasource,twoyrmax,halfmax,summary,alert)

    except:
        try:
            print"\n"
            print "Generating Rainfall ASTI plots for "+r+" due to lack of site csv file"
            for n in range(1,4):            
                rd.getrain(r, n)
                if os.stat(ASTIpath+r+rainfall_file).st_size != 0:
                    a = pd.read_csv(ASTIpath+r+rainfall_file,parse_dates='timestamp', names=['timestamp','rain'])
                    latest_ts = pd.to_datetime(a[0:1]['timestamp'].values[0])
                    if end - latest_ts < timedelta(hours=0.5):
                        break
            
            ASTIplot(r,offsetstart,end,tsn)
            datasource="ASTI" + str(n) + " (No Rain Gauge Data)"
            summary_writer(s,r,datasource,twoyrmax,halfmax,summary,alert)
            
        except:
            datasource="No Alert! No ASTI/SENSLOPE Data"
            summary_writer(s,r,datasource,twoyrmax,halfmax,summary,alert)
            continue


#Writes dataframe containaining site codes with its corresponding one and three days cumulative sum, data source, alert level and advisory
summary.to_csv('C:\Users\Dynaslope\Desktop\\Fresh\Local\Rainfall Plots\\'+'Summary of Rainfall Alert Generation for '+tsn+proc_monitoring_file,sep=',',mode='w')
print summary

#Summarizing rainfall data to rainfallalerts.txt
with open (proc_monitoring_path+"rainfallalert.txt", 'wb') as t:
    t.write('As of ' + end.strftime('%Y-%m-%d %H:%M') + ':\n')
    t.write ('nd: ' + ','.join(sorted(alert[3])) + '\n')
    t.write ('r0: ' + ','.join(sorted(alert[0])) + '\n')
    t.write ('r1a: ' + ','.join(sorted(alert[1])) + '\n')
    t.write ('r1b: ' + ','.join(sorted(alert[2])) + '\n')


# Deleting old data files (more than 10 days)
for dirpath, dirnames, filenames in os.walk('C:\Users\Dynaslope\Desktop\\Fresh\Local\Rainfall Plots\\'):
    for file in filenames:
        curpath = os.path.join(dirpath, file)
        file_modified = datetime.fromtimestamp(os.path.getmtime(curpath))
        if datetime.now() - file_modified > timedelta(days = 10):
            os.remove(curpath)

ENDTIME = datetime.now()
