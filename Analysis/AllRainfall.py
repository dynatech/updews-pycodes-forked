##### IMPORTANT matplotlib declarations must always be FIRST to make sure that matplotlib works with cron-based automation
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as md
plt.ion()

import os
from datetime import datetime, timedelta, date, time
import pandas as pd
import numpy as np
import ConfigParser
import math
from sqlalchemy import create_engine

import querySenslopeDb as q

output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))

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
    ##rainfall; dataframe containing start to end of rainfall data resampled to 30min
    
    #raw data from senslope rain gauge
    rainfall = q.GetRawRainData(r, start)
    rainfall = rainfall.set_index('ts')
    rainfall = rainfall.loc[rainfall['rain']>=0]
    
    try:
        if rainfall.index[-1] <= end-timedelta(1):
            return pd.DataFrame(data=None)
        
        #data resampled to 30mins
        if rainfall.index[-1]<end:
            blankdf=pd.DataFrame({'ts': [end], 'rain': [0]})
            blankdf=blankdf.set_index('ts')
            rainfall=rainfall.append(blankdf)
        rainfall=rainfall[(rainfall.index>=start)]
        rainfall=rainfall[(rainfall.index<=end)]
        rainfall=rainfall.resample('30min',how='sum', label='right')
    
        return rainfall
    except:
        return pd.DataFrame(data=None)
        
def GetData(r, start, end):
    #raw data from senslope rain gauge
    rainfall = q.GetRawRainData(r, start)
    rainfall = rainfall.loc[rainfall['rain']>=0]
    rain_timecheck = rainfall
        
    #data resampled to 30mins
    blankdf = pd.DataFrame({'ts': [start, end], 'rain': [np.nan]*2})
    rainfall = rainfall.append(blankdf)
    rainfall = rainfall.drop_duplicates('ts')
    rainfall = rainfall.set_index('ts')
    rainfall = rainfall[(rainfall.index>=start)]
    rainfall = rainfall[(rainfall.index<=end)]
    if len(rain_timecheck) < 1:
        rainfall = rainfall.resample('30min',how='pad', label='right')
    else:
        rainfall = rainfall.resample('30min',how='sum', label='right')

    return rainfall

def PlotData(rain_gauge_col, start, end, sub, col, insax, cumax, fig, name):
    data = GetData(rain_gauge_col['rain_gauge'].values[0], start, end)
    
    #getting the rolling sum for the last24 hours
    rainfall2=pd.rolling_sum(data,48,min_periods=1)
    rainfall2=np.round(rainfall2,4)
    
    #getting the rolling sum for the last 3 days
    rainfall3=pd.rolling_sum(data,144,min_periods=1)
    rainfall3=np.round(rainfall3,4)

    data['1D'] = rainfall2.rain
    data['3D'] = rainfall3.rain
    
    data = data.reset_index()
    data = data.set_index('ts')
    plot1 = data['rain']
    plot2 = data['1D']
    plot3 = data['3D']
    plot4 = sub['maxhalf']
    plot5 = sub['max']
    
    RG_num = col.loc[col.rain_gauge == rain_gauge_col['rain_gauge'].values[0]].index[0]
    inscurax = insax[RG_num]
    cumcurax = cumax[RG_num]
    
    try:
        if RG_num == 0:
            inscurax.plot(plot1.index,plot1,color='#db4429', label = 'instantaneous rainfall') # instantaneous rainfall data
            cumcurax.plot(plot2.index,plot2,color='#5ac126', label = '24hr cumulative rainfall') # 24-hr cumulative rainfall
            cumcurax.plot(plot3.index,plot3,color='#0d90d0', label = '72hr cumulative rainfall') # 72-hr cumulative rainfall
            cumcurax.plot(plot4.index,plot4,color="#fbb714", label = 'half of 2yr max rainfall') # half of 2-yr max rainfall
            cumcurax.plot(plot5.index,plot5,color="#963bd6", label = '2yr max rainfall')  # 2-yr max rainfall
            inscurax.set_ylabel(rain_gauge_col['rain_gauge'].values[0].replace('rain_noah_', 'NOAH'), fontsize='medium')
        else:
            inscurax.plot(plot1.index,plot1,color='#db4429') # instantaneous rainfall data
            cumcurax.plot(plot2.index,plot2,color='#5ac126') # 24-hr cumulative rainfall
            cumcurax.plot(plot3.index,plot3,color='#0d90d0') # 72-hr cumulative rainfall
            cumcurax.plot(plot4.index,plot4,color="#fbb714") # half of 2-yr max rainfall
            cumcurax.plot(plot5.index,plot5,color="#963bd6")  # 2-yr max rainfall
            inscurax.set_ylabel(rain_gauge_col['rain_gauge'].values[0].replace('rain_noah_', 'NOAH'), fontsize='medium')
    except:
        pass
    
    dfmt = md.DateFormatter('%m-%d')
    inscurax.xaxis.set_major_formatter(dfmt)
    cumcurax.xaxis.set_major_formatter(dfmt)
    
    fig.subplots_adjust(top=0.93, right=0.9, left=0.08, bottom=0.08, hspace=0.23, wspace=0.13)
    fig.suptitle(name+" as of "+str(end),fontsize='large')

def SensorPlot(name, col, start, end, tsn, halfmax, twoyrmax, base, RainfallPlotsPath):
    
    ##INPUT:
    ##name; str; site name
    ##col; list; rain gauge table name
    ##end; datetime; end of rainfall data
    ##tsn; str; time format acceptable as file name
    ##halfmax; float; half of 2yr max rainfall, one-day cumulative rainfall threshold
    ##twoyrmax; float; 2yr max rainfall, three-day cumulative rainfall threshold    
    
    plt.xticks(rotation=70, size=5)       
    fig=plt.figure(figsize = (15,20))
    
    ins1 = fig.add_subplot(len(col),2,1)
    ins2 = fig.add_subplot(len(col),2,3, sharex=ins1, sharey=ins1)
    ins3 = fig.add_subplot(len(col),2,5, sharex=ins1, sharey=ins1)

    cum1 = fig.add_subplot(len(col),2,2)
    cum2 = fig.add_subplot(len(col),2,4, sharex=cum1, sharey=cum1)
    cum3 = fig.add_subplot(len(col),2,6, sharex=cum1, sharey=cum1)

    insax = [ins1, ins2, ins3]
    cumax = [cum1, cum2, cum3]

    if len(col) >= 4:
        ins4 = fig.add_subplot(len(col),2,7, sharex=ins1, sharey=ins1)
        cum4 = fig.add_subplot(len(col),2,8, sharex=cum1, sharey=cum1)
        insax.append(ins4)
        cumax.append(cum4)

    if len(col) == 5:
        ins5 = fig.add_subplot(len(col),2,9, sharex=ins1, sharey=ins1)
        cum5 = fig.add_subplot(len(col),2,10, sharex=cum1, sharey=cum1)
        insax.append(ins5)
        cumax.append(cum5)

    rain_gauge_col = col.groupby('rain_gauge')
    
    sub=base
    sub['maxhalf'] = halfmax  
    sub['max'] = twoyrmax
    
    rain_gauge_col.apply(PlotData, start= start, end=end, sub=sub, col=col, insax=insax, cumax=cumax, fig=fig, name=name) 
    
    lgd = plt.legend(loc='center left', bbox_to_anchor=(1, 0.5), fontsize='medium')
    plt.savefig(RainfallPlotsPath+tsn+"_"+name, dpi=200, 
        facecolor='w', edgecolor='w',orientation='landscape',mode='w',
        bbox_extra_artists=(lgd,))#, bbox_inches='tight')

def GetUnemptyOtherRGdata(col, start, end):
    
    ##INPUT:
    ##r; str; site
    ##offsetstart; datetime; starting point of interval with offset to account for moving window operations
    
    ##OUTPUT:
    ##df; dataframe; rainfall from noah rain gauge    
    
    #gets data from nearest noah/senslope rain gauge
    #moves to next nearest until data is updated
    
    for n in range(3):            
        r = col[n]
        
        OtherRGdata = GetResampledData(r, start, end)
        if len(OtherRGdata) != 0:
            latest_ts = pd.to_datetime(OtherRGdata.index.values[-1])
            if end - latest_ts < timedelta(1):
                return OtherRGdata, r
    return pd.DataFrame(data = None), r

def onethree_val_writer(rainfall):

    ##INPUT:
    ##one; dataframe; one-day cumulative rainfall
    ##three; dataframe; three-day cumulative rainfall

    ##OUTPUT:
    ##one, three; float; cumulative sum for one day and three days

    try:

        #getting the rolling sum for the last24 hours
        rainfall2=pd.rolling_sum(rainfall,48,min_periods=1)
        rainfall2=np.round(rainfall2,4)
        
        #getting the rolling sum for the last 3 days
        rainfall3=pd.rolling_sum(rainfall,144,min_periods=1)
        rainfall3=np.round(rainfall3,4)

                
        one = float(rainfall2.rain[-1:])
        three = float(rainfall3.rain[-1:])

    except:
        one=None
        three=None
    
    return one,three
        
def summary_writer(sum_index,r,datasource,twoyrmax,halfmax,summary,alert,alert_df,rainfall):

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
    
    one,three = onethree_val_writer(rainfall)

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

def RainfallAlert(siterainprops, start, end, offsetstart, tsn, summary, alert, alert_df, base, PrintPlot, RainfallPlotsPath):

    ##INPUT:
    ##siterainprops; DataFrameGroupBy; contains rain noah ids of noah rain gauge near the site, one and three-day rainfall threshold
    
    ##OUTPUT:
    ##evaluates rainfall alert
    
    #rainfall properties from siterainprops
    name = siterainprops['name'].values[0]
    twoyrmax = siterainprops['max_rain_2year'].values[0]
    halfmax=twoyrmax/2
    sum_index = siterainprops.index[0]
    
    print name
    
    rain_arq = siterainprops['rain_arq'].values[0]
    rain_senslope = siterainprops['rain_senslope'].values[0]
    RG1 = siterainprops['RG1'].values[0]
    RG2 = siterainprops['RG2'].values[0]
    RG3 = siterainprops['RG3'].values[0]
        
    if PrintPlot:
        col = [rain_arq, rain_senslope, RG1, RG2, RG3]
        col = filter(None, col)
        col = pd.DataFrame({'rain_gauge': col})
        SensorPlot(name, col, start, end, tsn, halfmax, twoyrmax, base, RainfallPlotsPath)
    
    try:
        if rain_arq == None:
            rain_timecheck = pd.DataFrame()
        else:
            #resampled data from senslope rain gauge
            rainfall = GetResampledData(rain_arq, start, end)
            #data not more than a day from end
            rain_timecheck = rainfall[(rainfall.index>=end-timedelta(days=1))]
        
        #if data from rain_arq is not updated, data is gathered from rain_senslope
        if len(rain_timecheck.dropna())<1:
            #from rain_senslope, plots and alerts are processed
            rainfall = GetResampledData(rain_senslope, start, end)
            datasource="rain_senslope"
            summary_writer(sum_index,name,datasource,twoyrmax,halfmax,summary,alert,alert_df,rainfall)
                    
        else:
            #plots and alerts are processed if senslope rain gauge data is updated
            datasource = "rain_arq"
            summary_writer(sum_index,name,datasource,twoyrmax,halfmax,summary,alert,alert_df,rainfall)

    except:
        try:
            #if no data from senslope rain gauge, data is gathered from nearest senslope rain gauge from other site or noah rain gauge
            col = [RG1, RG2, RG3]
            rainfall, r = GetUnemptyOtherRGdata(col, start, end)
            datasource = "Other Rain Gauge: %s" %r
            summary_writer(sum_index,name,datasource,twoyrmax,halfmax,summary,alert,alert_df,rainfall)
        except:
            #if no data for all rain gauge
            rainfall = pd.DataFrame()
            datasource="No Alert! No ASTI/SENSLOPE Data"
            summary_writer(sum_index,name,datasource,twoyrmax,halfmax,summary,alert,alert_df,rainfall)

def alert_toDB(df, end):
    
    query = "SELECT * FROM senslopedb.site_level_alert WHERE site = '%s' AND source = 'rain' AND updateTS <= '%s' ORDER BY updateTS DESC LIMIT 1" %(df.site.values[0], end)
    
    df2 = q.GetDBDataFrame(query)
    
    if len(df2) == 0 or df2.alert.values[0] != df.alert.values[0]:
        df['updateTS'] = end
        engine = create_engine('mysql://'+q.Userdb+':'+q.Passdb+'@'+q.Hostdb+':3306/'+q.Namedb)
        df.to_sql(name = 'site_level_alert', con = engine, if_exists = 'append', schema = q.Namedb, index = False)
    elif df2.alert.values[0] == df.alert.values[0]:
        db, cur = q.SenslopeDBConnect(q.Namedb)
        query = "UPDATE senslopedb.site_level_alert SET updateTS='%s' WHERE site = '%s' and source = 'rain' and alert = '%s' and timestamp = '%s'" %(end, df2.site.values[0], df2.alert.values[0], pd.to_datetime(str(df2.timestamp.values[0])))
        cur.execute(query)
        db.commit()
        db.close()

################################     MAIN     ################################

def main():
    
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
    
    
    #INPUT/OUTPUT FILES
    
    #local file paths
    output_file_path = output_path + cfg.get('I/O', 'OutputFilePath')
    RainfallPlotsPath = output_path + cfg.get('I/O', 'RainfallPlotsPath')
    
    #file names
    CSVFormat = cfg.get('I/O','CSVFormat')
    
    #To Output File or not
    PrintPlot = cfg.getboolean('I/O','PrintPlot')
    PrintSummaryAlert = cfg.getboolean('I/O','PrintSummaryAlert')
    
    #creates directory if it doesn't exist
    if not os.path.exists(output_file_path):
        os.makedirs(output_file_path)
    if PrintPlot or PrintSummaryAlert:
        if not os.path.exists(RainfallPlotsPath):
            os.makedirs(RainfallPlotsPath)

    #1. setting monitoring window
    roll_window_numpts, end, start, offsetstart, monwin = set_monitoring_window(roll_window_length,data_dt,rt_window_length,num_roll_window_ops)
    
    index = pd.date_range(end-timedelta(rt_window_length), periods=rt_window_length+1, freq='D')
    columns=['maxhalf','max']
    base = pd.DataFrame(index=index, columns=columns)
    
    tsn=end.strftime("%Y-%m-%d_%H-%M-%S")
    
    #rainprops containing noah id and threshold
    rainprops = q.GetRainProps('rain_props')
    rainprops = rainprops.loc[rainprops.name == 'bak']
    
    #empty dataframe; summary writer
    index = range(len(rainprops))
    columns=['site','1D','3D','DataSource','alert','advisory']
    summary = pd.DataFrame(index=index, columns=columns)
    
    #alert summary container, r0 sites at alert[0], r1a sites at alert[1], r1b sites at alert[2],  nd sites at alert[3]
    alert = [[],[],[],[]]
    alert_df = []
    
    siterainprops = rainprops.groupby('name')
    
    ### Processes Rainfall Alert ###
    siterainprops.apply(RainfallAlert, start=start, end=end, offsetstart=offsetstart, tsn=tsn, summary=summary, alert=alert, alert_df=alert_df, base=base, PrintPlot=PrintPlot, RainfallPlotsPath=RainfallPlotsPath)

    summary = summary.sort('site')

    #Writes dataframe containaining site codes with its corresponding one and three days cumulative sum, data source, alert level and advisory
    if PrintSummaryAlert:
        summary.to_csv(RainfallPlotsPath+'SummaryOfRainfallAlertGenerationFor'+tsn+CSVFormat,sep=',',mode='w')

#    #writes alert summary to db
#    summary['timestamp'] = [str(end)]*len(summary)
#    summary['source'] = 'rain'
#    summary['site'] = summary['site'].map(lambda x: str(x)[:3])
#    msl_raindf = summary.loc[summary.site == 'mes']
#    msl_raindf.site = 'msl'
#    msu_raindf = summary.loc[summary.site == 'mes']
#    msu_raindf.site = 'msu'
#    summary = summary.append(msl_raindf).append(msu_raindf)
#    summary = summary.loc[summary.site != 'mes']
#    summary = summary.reset_index(drop = True)
#    summary = summary.sort('site')
#    df_for_db = summary[['timestamp', 'site', 'source', 'alert']]
#    df_for_db = df_for_db.dropna()
#    
#    #Write to senslopedb.site_level_alerts
#    site_DBdf = df_for_db.groupby('site')
#    site_DBdf.apply(alert_toDB, end=end)
    
    print summary
    
    return summary

###############################################################################

if __name__ == "__main__":
    start_time = datetime.now()
    main()
    print "runtime = ", datetime.now()-start_time

