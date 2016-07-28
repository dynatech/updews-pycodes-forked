##### IMPORTANT matplotlib declarations must always be FIRST to make sure that matplotlib works with cron-based automation
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
plt.ioff()

import os
from datetime import datetime, timedelta, date, time
import pandas as pd
import numpy as np
import ConfigParser
import math
import sys

output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))

#include the path of "Data Analysis" folder for the python scripts searching
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
    sys.path.insert(1,path)
del path   

from querySenslopeDb import *

############################################################
##      TIME FUNCTIONS                                    ##    
############################################################

def get_rt_window(rt_window_length,roll_window_size,num_roll_window_ops):
    
    ##INPUT:
    ##rt_window_length; float; length of real-time monitoring window in days
    ##roll_window_size; integer; number of data points to cover in moving window operations
    
    ##OUTPUT: 
    ##end, start, offsetstart; datetimes; dates for the end, start and offset-start of the real-time monitoring window 

    ##set current time as endpoint of the interval
    end=datetime.now()

    ##round down current time to the nearest HH:00 or HH:30 time value
    end_Year=end.year
    end_month=end.month
    end_day=end.day
    end_hour=end.hour
    end_minute=end.minute
    if end_minute<30:end_minute=0
    else:end_minute=30
    end=datetime.combine(date(end_Year,end_month,end_day),time(end_hour,end_minute,0))

    #starting point of the interval
    start=end-timedelta(days=rt_window_length)
    
    #starting point of interval with offset to account for moving window operations 
    offsetstart=end-timedelta(days=rt_window_length+((num_roll_window_ops*roll_window_size-1)/48.))
    
    return end, start, offsetstart

def set_monitoring_window(roll_window_length,data_dt,rt_window_length,num_roll_window_ops):
    
    ##INPUT:
    ##roll_window_length; float; length of rolling/moving window operations, in hours
    ##data_dt; float; time interval between data points, in hours    
    ##rt_window_length; float; length of real-time monitoring window, in days
    ##num_roll_window_ops; float; number of rolling window operations in the whole monitoring analysis
    
    ##OUTPUT:
    ##roll_window_numpts; number of data points per rolling window, end; endpoint of interval, 
    ##start; starting point of interval, offsetstart; starting point of interval with offset to account for moving window operations,
    ##monwin; empty dataframe with length of rt_window_length
    
    roll_window_numpts=int(1+roll_window_length/data_dt)
    end, start, offsetstart=get_rt_window(rt_window_length,roll_window_numpts,num_roll_window_ops)
    monwin_time=pd.date_range(start=start, end=end, freq='30Min',name='ts', closed=None)
    monwin=pd.DataFrame(data=np.nan*np.ones(len(monwin_time)), index=monwin_time)

    return roll_window_numpts, end, start, offsetstart, monwin

def GetResampledData(r, start, end):
    
    ##INPUT:
    ##r; str; site
    ##start; datetime; start of rainfall data
    ##end; datetime; end of rainfall data
    
    ##OUTPUT:
    ##rainfall; dataframe containing start to end of rainfall data resampled to 15min
    
    #raw data from senslope rain gauge
    rainfall = GetRawRainData(r, start)
    rainfall = rainfall.set_index('ts')
    rainfall = rainfall.loc[rainfall['rain']>=0]
    
    #data resampled to 15mins
    if rainfall.index[-1:]<end:
        blankdf_time=pd.date_range(start=start, end=end, freq='15Min',name='timestamp', closed=None)
        blankdf=pd.DataFrame(data=np.nan*np.ones(len(blankdf_time)), index=blankdf_time,columns=['rain'])
        blankdf=blankdf[-1:]
        rainfall=rainfall.append(blankdf)
    rainfall=rainfall[(rainfall.index>=start)]
    rainfall=rainfall[(rainfall.index<=end)]
    rainfall=rainfall.resample('15min',how='sum')

    return rainfall

def SensorPlot(r,offsetstart,end,tsn, data, halfmax, twoyrmax):
    
    ##INPUT:
    ##r; str; site
    ##offsetstart; datetime; starting point of interval with offset to account for moving window operations
    ##end; datetime; end of rainfall data
    ##tsn; str; time format acceptable as file name
    ##data; dataframe; rainfall data
    ##halfmax; float; half of 2yr max rainfall, one-day cumulative rainfall threshold
    ##twoyrmax; float; 2yr max rainfall, three-day cumulative rainfall threshold
    
    ##OUTPUT:
    ##rainfall2, rainfall3; dataframe containing one-day and three-day cumulative rainfall
    ##also prints cumulative rainfall to csv & plots thresholds and cumulative rainfall
        
    if PrintPlot:
        plt.xticks(rotation=70, size=5)       
        
    #getting the rolling sum for the last24 hours
    rainfall2=pd.rolling_sum(data,96,min_periods=1)
    rainfall2=np.round(rainfall2,4)

    if PrintCumSum:
        rainfall2.to_csv(CumSum_file_path+r+' 1d'+CSVFormat,sep=',',mode='w')
    
    #getting the rolling sum for the last 3 days
    rainfall3=pd.rolling_sum(data,288,min_periods=1)
    rainfall3=np.round(rainfall3,4)

    if PrintCumSum:
        rainfall3.to_csv(CumSum_file_path+r+' 3d'+CSVFormat,sep=',',mode='w')
    
    if PrintPlot:
        #assigning the thresholds to their own columns for plotting 

        sub=base
        sub['maxhalf'] = halfmax  
        sub['max'] = twoyrmax
        
        #assigning df to plot variables (to avoid caveats ? expressed from Spyder)
        plot1=data.dropna()     # instantaneous rainfall data
        plot2=rainfall2             # 24-hr cumulative rainfall
        plot3=rainfall3             # 72-hr cumulative rainfall
        plot4=sub['maxhalf']        # half of 2-yr max rainfall
        plot5=sub['max']            # 2-yr max rainfall

        #plots instantaneous rainfall data, 24-hr cumulative rainfall, 72-hr cumulative rainfall,
        #half of 2-yr max rainfall, 2-yr max rainfall
        plt.plot(plot1.index,plot1,color='#db4429', label = 'instantaneous rainfall') # instantaneous rainfall data
        plt.plot(plot2.index,plot2,color='#5ac126', label = '24hr cumulative rainfall') # 24-hr cumulative rainfall
        plt.plot(plot3.index,plot3,color='#0d90d0', label = '72hr cumulative rainfall') # 72-hr cumulative rainfall
        plt.plot(plot4.index,plot4,color="#fbb714", label = 'half of 2yr max rainfall') # half of 2-yr max rainfall
        plt.plot(plot5.index,plot5,color="#963bd6", label = '2yr max rainfall')  # 2-yr max rainfall
        plt.legend(loc='upper left', fontsize = 8)        
        plt.title(r)
        plt.savefig(RainfallPlotsPath+tsn+"_"+r, dpi=160, 
            facecolor='w', edgecolor='w',orientation='landscape',mode='w')
        plt.close()
    
    return rainfall2, rainfall3
    
def GetASTIdata(site, rain_noah, offsetstart):

    ##INPUT:
    ##site; str
    ##offsetstart; datetime; starting point of interval with offset to account for moving window operations
    ##rain_noah; float; rain noah id of noah rain gauge near the site
    
    ##OUTPUT:
    ##df; dataframe; rainfall from noah rain gauge

    #data from noah rain gauge saved at local database
    try:    
        if not math.isnan(rain_noah):
            rain_noah = int(rain_noah)
    
        db, cur = SenslopeDBConnect(Namedb)
        
        query = "select timestamp,rval from senslopedb.rain_noah_%s" % str(rain_noah)
        query = query + " where timestamp >= timestamp('%s')" % offsetstart
        query = query + " order by timestamp desc"
        df =  GetDBDataFrame(query)
        df.columns = ['timestamp','rain']
        df.timestamp = pd.to_datetime(df.timestamp)
        if PrintASTIdata:
            df.to_csv(ASTIpath + site + CSVFormat, sep = ',', mode = 'w', index = False, header = False)
        df.set_index('timestamp', inplace = True)
        return df
    except:
        print 'Table senslopedb.rain_noah_' + str(rain_noah) + " doesn't exist"
        df = pd.DataFrame(data=None)
        return df

def GetUnemptyASTIdata(r, rainprops, offsetstart):
    
    ##INPUT:
    ##r; str; site
    ##offsetstart; datetime; starting point of interval with offset to account for moving window operations
    ##rainprops; dataframe; contains rain noah ids of noah rain gauge near the site
    
    ##OUTPUT:
    ##df; dataframe; rainfall from noah rain gauge    
    
    #gets data from nearest noah rain gauge
    #moves to next nearest until data is updated
    for n in range(1,4):            
        if n == 1:
            rain_noah = rainprops.loc[rainprops.site == r]['rain_noah'].values[0]
        else:
            rain_noah = rainprops.loc[rainprops.site == r]['rain_noah'+str(n)].values[0]
        
        ASTIdata = GetASTIdata(r, rain_noah, offsetstart)
        if len(ASTIdata) != 0:
            latest_ts = pd.to_datetime(ASTIdata.index.values[0])
            if end - latest_ts < timedelta(hours=1):
                return ASTIdata, n
    return pd.DataFrame(data = None), n

def ASTIplot(r,offsetstart,end,tsn, data, halfmax, twoyrmax):

    ##INPUT:
    ##r; str; site
    ##offsetstart; datetime; starting point of interval with offset to account for moving window operations
    ##end; datetime; end of rainfall data
    ##tsn; str; time format acceptable as file name
    ##data; dataframe; rainfall data
    ##halfmax; float; half of 2yr max rainfall, one-day cumulative rainfall threshold
    ##twoyrmax; float; 2yr max rainfall, three-day cumulative rainfall threshold
    
    ##OUTPUT:
    ##rainfall2, rainfall3; dataframe containing one-day and three-day cumulative rainfall
    ##also prints cumulative rainfall to csv & plots thresholds and cumulative rainfall

    try:
        #data is resampled to 15mins
        rainfall = data
        rainfall = rainfall.loc[rainfall['rain']>=0]
        rainfall = rainfall[(rainfall.index>=offsetstart)]
        rainfall = rainfall[(rainfall.index<=end)]
        rainfall = rainfall.resample('15min',how='sum')
    
        if PrintPlot:
            plt.xticks(rotation=70, size=5)
        
        #getting the rolling sum for the last24 hours
        rainfall2 = rainfall.rolling(min_periods=1,window=96,center=False).sum()
        rainfall2=np.round(rainfall2,4)

        #prints rolling sum from 24hrs to csv file
        if PrintCumSum:
            rainfall2.to_csv(CumSum_file_path+r+' 1d'+CSVFormat,sep=',',mode='w')
        
        #getting the rolling sum for the last 3 days
        rainfall3 = rainfall.rolling(min_periods=1,window=288,center=False).sum()
        rainfall3=np.round(rainfall3,4)

        #prints rolling sum from 72hrs to csv file
        if PrintCumSum:
            rainfall3.to_csv(CumSum_file_path+r+' 3d'+CSVFormat,sep=',',mode='w')
        
        if PrintPlot:
            #assigning the thresholds to their own columns for plotting 
            sub=base
            sub['maxhalf'] = halfmax  
            sub['max'] = twoyrmax
        
            #assigning df to plot variables (to avoid caveats ? expressed from Spyder)
            plot1=rainfall.dropna()     # instantaneous rainfall data
            plot2=rainfall2             # 24-hr cumulative rainfall
            plot3=rainfall3             # 72-hr cumulative rainfall
            plot4=sub['maxhalf']        # half of 2-yr max rainfall
            plot5=sub['max']            # 2-yr max rainfall
        
            #plots instantaneous rainfall data, 24-hr cumulative rainfall, 72-hr cumulative rainfall,
            #half of 2-yr max rainfall, 2-yr max rainfall
            plt.plot(plot1.index,plot1,color='#db4429', label = 'instantaneous rainfall') # instantaneous rainfall data
            plt.plot(plot2.index,plot2,color='#5ac126', label = '24hr cumulative rainfall') # 24-hr cumulative rainfall
            plt.plot(plot3.index,plot3,color='#0d90d0', label = '72hr cumulative rainfall') # 72-hr cumulative rainfall
            plt.plot(plot4.index,plot4,color="#fbb714", label = 'half of 2yr max rainfall') # half of 2-yr max rainfall
            plt.plot(plot5.index,plot5,color="#963bd6", label = '2yr max rainfall')  # 2-yr max rainfall
            plt.legend(loc='upper left', fontsize = 8)
            plt.title(r)
            plt.savefig(RainfallPlotsPath+tsn+"_"+r,
                dpi=160, facecolor='w', edgecolor='w',orientation='landscape',mode='w')
            plt.close()
        
    except:
        rainfall2 = pd.DataFrame(data=None)
        rainfall3 = pd.DataFrame(data=None)
    
    return rainfall2, rainfall3

def onethree_val_writer(colname, one, three):

    ##INPUT:
    ##colname; string; site code
    ##one; dataframe; one-day cumulative rainfall
    ##three; dataframe; three-day cumulative rainfall

    ##OUTPUT:
    ##one, three; float; cumulative sum for one day and three days

    try:
        one, three = one, three
        
        #adds blank dataframe if last data is more than 3.5hrs
        if end - pd.to_datetime(one.index[-1:]) > timedelta(hours=3.5):
            blankdf_time=pd.date_range(start=start, end=end, freq='15Min',name='timestamp', closed=None)
            blankdf=pd.DataFrame(data=np.nan*np.ones(len(blankdf_time)), index=blankdf_time,columns=['rain'])
            blankdf=blankdf[-1:]
            one=one.append(blankdf)
        
        #returns null is last data is more than 3.5hrs
        one = float(one.rain[-1:])
     
        #adds blank dataframe if last data is more than 3.5hrs
        if end - pd.to_datetime(three.index[-1:]) > timedelta(hours=3.5):
            blankdf_time=pd.date_range(start=start, end=end, freq='15Min',name='timestamp', closed=None)
            blankdf=pd.DataFrame(data=np.nan*np.ones(len(blankdf_time)), index=blankdf_time,columns=['rain'])
            blankdf=blankdf[-1:]
            three=three.append(blankdf)

        #returns null is last data is more than 3.5hrs
        three = float(three.rain[-1:])

    except:
        one=None
        three=None
    
    return one,three
        
def summary_writer(sum_index,r,datasource,twoyrmax,halfmax,summary,alert,alert_df,one,three):

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
    ##alert_df;array of tuples; alert summary container; format: (site,alert)
    ##one; dataframe; one-day cumulative rainfall
    ##three; dataframe; three-day cumulative rainfall        
    
    one,three = onethree_val_writer(r, one, three)

    #threshold is reached
    if one>=halfmax or three>=twoyrmax:
        ralert='r1'
        advisory='Start/Continue monitoring'
        #both threshholds are reached
        if one>=halfmax and three>=twoyrmax:
            alert[1].append(r+' ('+str(one)+')')
            alert[2].append(r+' ('+str(three)+')')
            alert_df.append((r,'r1a, r1b'))
        #only one-day threshold is reached
        elif one>=halfmax:
            alert[1].append(r+' ('+str(one)+')')
            alert_df.append((r,'r1a'))
        #only three-day threshold is reached
        else:
            alert[2].append(r+' ('+str(three)+')')
            alert_df.append((r,'r1b'))
    #no data
    elif one==None or math.isnan(one):
        datasource="No Alert! No ASTI/SENSLOPE Data"
        ralert='nd'
        advisory='---'
        alert[3].append(r)
        alert_df.append((r,'nd'))
    #rainfall below threshold
    else:
        ralert='r0'
        advisory='---'
        alert[0].append(r)
        alert_df.append((r,'r0'))
    summary.loc[sum_index]=[r,one,three,datasource,ralert,advisory]

def RainfallAlert(siterainprops):

    ##INPUT:
    ##siterainprops; DataFrameGroupBy; contains rain noah ids of noah rain gauge near the site, one and three-day rainfall threshold
    
    ##OUTPUT:
    ##evaluates rainfall alert
    
    #rainfall properties from siterainprops
    r,twoyrmax = siterainprops.site.values[0], siterainprops.max_rain_2year.values[0]
    halfmax=twoyrmax/2
    sum_index = rainprops.loc[rainprops.site == r].index[0]

    try:
        #resampled data from senslope rain gauge
        rainfall = GetResampledData(r, start, end)

        #data not more than a day from end
        rain_timecheck=rainfall[(rainfall.index>=end-timedelta(days=1))]
        
        #if data from senslope rain gauge is not updated, data is gathered from noah
        if len(rain_timecheck.dropna())<1:
            #from noah data, plots and alerts are processed
            ASTIdata, n = GetUnemptyASTIdata(r, rainprops, offsetstart)
            one, three = ASTIplot(r,offsetstart,end,tsn, ASTIdata, halfmax, twoyrmax)
            
            datasource="ASTI" + str(n) + " (Empty Rain Gauge Data)"
            summary_writer(sum_index,r,datasource,twoyrmax,halfmax,summary,alert,alert_df,one,three)

                    
        else:
            #plots and alerts are processed if senslope rain gauge data is updated
            one, three = SensorPlot(r, offsetstart, end, tsn, rainfall, halfmax, twoyrmax)
            
            datasource="SENSLOPE Rain Gauge"
            summary_writer(sum_index,r,datasource,twoyrmax,halfmax,summary,alert,alert_df,one,three)


    except:
        #if no data from senslope rain gauge, data is gathered from noah then plots and alerts are processed
        ASTIdata, n = GetUnemptyASTIdata(r, rainprops, offsetstart)
        one, three = ASTIplot(r,offsetstart,end,tsn, ASTIdata, halfmax, twoyrmax)
        
        datasource="ASTI" + str(n) + " (No Rain Gauge Data)"
        summary_writer(sum_index,r,datasource,twoyrmax,halfmax,summary,alert,alert_df,one,three)

def site_level_alerts_updater(df):
    query = 'UPDATE senslopedb.site_level_alert SET updateTS = "{}" WHERE site = "{}" AND timestamp = "{}" AND source = "{}"'.format(pd.to_datetime(str(df.updateTS.values[0])),df.site.values[0],pd.to_datetime(str(df.timestamp.values[0])),df.source.values[0])
    db, cur = SenslopeDBConnect(Namedb)
    cur.execute(query)
    db.commit()
    db.close()
    
def uptoDB_site_level_alerts(df):
    #INPUT: Dataframe containing site level alerts
    #OUTPUT: Writes to sql database the alerts of sites who recently changed their alert status
    
    #Get the latest site_level_alert
    query = 'SELECT s1.timestamp,s1.site,s1.source,s1.alert, COUNT(*) num FROM senslopedb.site_level_alert s1 JOIN senslopedb.site_level_alert s2 ON s1.site = s2.site AND s1.source = s2.source AND s1.timestamp <= s2.timestamp group by s1.timestamp,s1.site, s1.source HAVING COUNT(*) <= 1 ORDER BY site, source, num desc'
    df2 = GetDBDataFrame(query)
    
    #Merge the two data frames to determine overlaps in alerts
    overlap = pd.merge(df,df2,how = 'left', on = ['site','source','alert'],suffixes=['','_r'])    
    
    #Get the site with no changes in its latest alert
    persistent_alerts = df[~overlap['timestamp_r'].isnull()]
    persistent_alerts['updateTS'] = persistent_alerts['timestamp']
    persistent_alerts['timestamp'] = overlap[~overlap['timestamp_r'].isnull()]['timestamp_r']
    persistent_alerts = persistent_alerts.groupby('site')
    persistent_alerts.apply(site_level_alerts_updater)
    
    #Get the site with change in its latest alert
    changed_alerts = df[overlap['timestamp_r'].isnull()]
    changed_alerts['updateTS'] = changed_alerts['timestamp']
    changed_alerts = changed_alerts[['timestamp','site','source','alert','updateTS']].set_index('timestamp')
    
    engine=create_engine('mysql://root:senslope@192.168.1.102:3306/senslopedb')
    changed_alerts.to_sql(name = 'site_level_alert', con = engine, if_exists = 'append', schema = Namedb, index = True)


###############################################################################

start_time = datetime.now()

cfg = ConfigParser.ConfigParser()
cfg.read('IO-Config.txt')    

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
output_file_path = output_path + cfg.get('I/O', 'OutputFilePath')
CumSum_file_path = output_path + cfg.get('I/O', 'CumSumFilePath')
ASTIpath = output_path + cfg.get('I/O', 'ASTIpath')
RainfallPlotsPath = output_path + cfg.get('I/O', 'RainfallPlotsPath')

#file names
CSVFormat = cfg.get('I/O','CSVFormat')
rainfallalert = cfg.get('I/O','rainfallalert')

#ALERT CONSTANTS
T_disp = cfg.getfloat('I/O','T_disp')  #m
T_velA1 = cfg.getfloat('I/O','T_velA1') #m/day
T_velA2 = cfg.getfloat('I/O','T_velA2')  #m/day
k_ac_ax = cfg.getfloat('I/O','k_ac_ax')
num_nodes_to_check = cfg.getint('I/O','num_nodes_to_check')

#To Output File or not
PrintPlot = cfg.getboolean('I/O','PrintPlot')
PrintSummaryAlert = cfg.getboolean('I/O','PrintSummaryAlert')
PrintCumSum = cfg.getboolean('I/O','PrintCumSum')
PrintRAlert = cfg.getboolean('I/O','PrintRAlert')
PrintASTIdata = cfg.getboolean('I/O','PrintASTIdata')

#creates directory if it doesn't exist
if not os.path.exists(output_file_path):
    os.makedirs(output_file_path)
if PrintPlot or PrintSummaryAlert:
    if not os.path.exists(RainfallPlotsPath):
        os.makedirs(RainfallPlotsPath)
if PrintCumSum:
    if not os.path.exists(CumSum_file_path):
        os.makedirs(CumSum_file_path)
if PrintASTIdata:
    if not os.path.exists(ASTIpath):
        os.makedirs(ASTIpath)
    
################################     MAIN     ################################

#1. setting monitoring window
roll_window_numpts, end, start, offsetstart, monwin = set_monitoring_window(roll_window_length,data_dt,rt_window_length,num_roll_window_ops)

index = pd.date_range(end-timedelta(10), periods=11, freq='D')
columns=['maxhalf','max']
base = pd.DataFrame(index=index, columns=columns)

tsn=end.strftime("%Y-%m-%d_%H-%M-%S")

#rainprops containing noah id and threshold
rainprops = GetRainProps()
rainprops['rain_arq'] = rainprops['rain_arq'].fillna(rainprops['rain_senslope'])
rainprops = rainprops[['name', 'rain_arq', 'max_rain_2year', 'rain_noah', 'rain_noah2', 'rain_noah3']]
rainprops['rain_arq'] = rainprops['rain_arq'].fillna(rainprops['name'])
rainprops['site'] = rainprops['rain_arq']
rainprops = rainprops[['site', 'max_rain_2year', 'rain_noah', 'rain_noah2', 'rain_noah3']]
rainprops = rainprops.drop_duplicates(['site'], take_last = True)
rainprops = rainprops.reset_index(drop = True)

#empty dataframe
index = range(len(rainprops))
columns=['site','1D','3D','DataSource','alert','advisory']
summary = pd.DataFrame(index=index, columns=columns)


#alert summary container, r0 sites at alert[0], r1a sites at alert[1], r1b sites at alert[2],  nd sites at alert[3]
alert = [[],[],[],[]]
alert_df = []

#Set if JSON format will be printed
set_json = True

siterainprops = rainprops.groupby('site')

### Processes Rainfall Alert ###
siterainprops.apply(RainfallAlert)

#Writes dataframe containaining site codes with its corresponding one and three days cumulative sum, data source, alert level and advisory
if PrintSummaryAlert:
    summary.to_csv(RainfallPlotsPath+'SummaryOfRainfallAlertGenerationFor'+tsn+CSVFormat,sep=',',mode='w')

#### writing to db ####
from sqlalchemy import create_engine
engine=create_engine('mysql://root:senslope@192.168.1.102:3306/senslopedb')

#writes alert summary to db
summary['timestamp'] = [str(end)]*len(summary)
summary['source'] = 'rain'
summary['site'] = summary['site'].map(lambda x: str(x)[:3])
msl_raindf = summary.loc[summary.site == 'mes']
msl_raindf.site = 'msl'
msu_raindf = summary.loc[summary.site == 'mes']
msu_raindf.site = 'msu'
summary = summary.append(msl_raindf).append(msu_raindf)
summary = summary.loc[summary.site != 'mes']
summary = summary.reset_index(drop = True)
summary = summary.sort('site')
df_for_db = summary[['timestamp', 'site', 'source', 'alert']]
df_for_db = df_for_db.dropna()
print df_for_db

#Write to site_level_alerts
uptoDB_site_level_alerts(df_for_db)

#Summarizing rainfall data to rainfallalerts.txt
if PrintRAlert:
    with open (output_file_path+rainfallalert, 'wb') as t:
        t.write('As of ' + end.strftime('%Y-%m-%d %H:%M') + ':\n')
        t.write ('nd: ' + ','.join(sorted(alert[3])) + '\n')
        t.write ('r0: ' + ','.join(sorted(alert[0])) + '\n')
        t.write ('r1a: ' + ','.join(sorted(alert[1])) + '\n')
        t.write ('r1b: ' + ','.join(sorted(alert[2])) + '\n')

#Printing of alerts using JSON format
if set_json:
    #create data frame as for easy conversion to JSON format
    
    for i in range(len(alert_df)): alert_df[i] = (end,) + alert_df[i]
    dfa = pd.DataFrame(alert_df,columns = ['timestamp','site','r alert'])
    
    #converting the data frame to JSON format
    dfajson = dfa.to_json(orient="records",date_format='iso')
    
    #ensuring proper datetime format
    i = 0
    while i <= len(dfajson):
        if dfajson[i:i+9] == 'timestamp':
            dfajson = dfajson[:i] + dfajson[i:i+36].replace("T"," ").replace("Z","").replace(".000","") + dfajson[i+36:]
            i += 1
        else:
            i += 1

end_time = datetime.now()
print "time = ", end_time - start_time