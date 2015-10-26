import os
from datetime import datetime, date, time, timedelta
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import ConfigParser

import generic_functions as gf
import rainDownload as rd
from querySenslopeDb import *

def set_monitoring_window(roll_window_length,data_dt,rt_window_length,num_roll_window_ops):

    ##DESCRIPTION:    
    ##returns number of data points per rolling window, endpoint of interval, starting point of interval, time interval for real-time monitoring, empty dataframe
    
    ##INPUT:
    ##roll_window_length; float; length of rolling/moving window operations, in hours
    ##data_dt; float; time interval between data points, in hours    
    ##rt_window_length; float; length of real-time monitoring window, in days
    ##num_roll_window_ops
    
    ##OUTPUT:
    ##roll_window_numpts, end, start, offsetstart, monwin
    
    roll_window_numpts=int(1+roll_window_length/data_dt)
    end, start, offsetstart=gf.get_rt_window(rt_window_length,roll_window_numpts,num_roll_window_ops)
    start=datetime(2010,10,1)
    monwin_time=pd.date_range(start=start, end=end, freq='30Min',name='ts', closed=None)
    monwin=pd.DataFrame(data=np.nan*np.ones(len(monwin_time)), index=monwin_time)

    return roll_window_numpts, end, start, offsetstart, monwin

def create_series_list(input_df,monwin,colname,num_nodes):
    
    ##DESCRIPTION:
    ##returns list of xz node series, xy node series and m node series
    
    ##INPUT:
    ##input_df; dataframe
    ##monwin; empty dataframe
    ##colname; string; name of site
    ##num_nodes; integer; number of nodes

    ##OUTPUT:
    ##xz_series_list, xy_series_list, m_series_list

    #a. initializing lists
    m_series_list=[]

    #b.appending monitoring window dataframe to lists
    m_series_list.append(monwin)
   
    for n in range(1,1+num_nodes):
        #c.creating node series        
        curm=input_df.loc[input_df.id==n,['m']]

        #d.resampling node series to 30-min exact intervals
        finite_data=np.sum(np.where(np.isfinite(curm.values))[0])

        if finite_data>0:
            curm=curm.resample('30min',how='mean',base=0)
        else:
            print colname, n, "ERROR missing node data"
            #zeroing tilt data if node data is missing
            curm=pd.DataFrame(data=np.nan*np.ones(len(monwin)), index=monwin.index)      
        #5e. appending node series to list
        m_series_list.append(curm)
    
    #concatenating series list into dataframe
    df=pd.concat(m_series_list, axis=1, join='outer', names=None)

    #renaming columns
    df.columns=[a for a in np.arange(0,1+num_nodes)]
    df=df.drop(0,1)
    df=df.resample('30min',how='mean',base=0)
    df=df.dropna(thresh=1)

    return np.round(df,4)

def norm_minmax(moi_data,m_df):

    ##DESCRIPTION:
    ##returns normalized

    ##INPUT:
    ##moi_data; array
    ##m_df; array

    ##OUTPUT:
    ##norm_minmax
    
    normalized=[]
    for n in range(1,1+num_nodes):
        rel_minmax=moi_data[moi_data.index==colname]
        nod_rel_min=float(rel_minmax.loc[rel_minmax.id==n,['min']].values)
        nod_rel_max=float(rel_minmax.loc[rel_minmax.id==n,['max']].values)
        norm_minmax=(m_df[n] - nod_rel_min)/(nod_rel_max - nod_rel_min)
        normalized.append(norm_minmax)

    norm_minmax=pd.concat(normalized, axis=1, join='outer')
    return norm_minmax

def ASTIplot(r,offsetstart,end,tsn):
    print"\n"
##    if r!='lipw': continue
    rainfall=pd.read_csv("C:\Users\Dynaslope\Desktop\Fresh\Local\ASTI\\"+r+rainfall_file,parse_dates='timestamp',
                         names=['timestamp','rain'],index_col='timestamp')
    rainfall=rainfall[(rainfall.index>=start)]
    rainfall=rainfall[(rainfall.index<=end)]
    rainfall=rainfall.resample('15min',how='sum')
#        print rainfall
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
    ##colname; array; list of sites

    ##OUTPUT:
    ##one, three

    try:
        one=pd.read_csv(proc_monitoring_path+'CumSum Rainfall//'+colname+' 1d'+proc_monitoring_file,sep=',')
        one = float(one.rain[-1:])
     
        three=pd.read_csv(proc_monitoring_path+'CumSum Rainfall//'+colname+' 3d'+proc_monitoring_file,sep=',')
        three = float(three.rain[-1:])
    except:
        one=None
        three=None
    
    return one,three
        
def summary_writer(s,r,datasource,twoyrmax,halfmax,summary,alert):

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
    elif one==None:
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

#file names of rainfall data
rain=cfg.get('I/O','rainfall_sites').split(',')

#INPUT/OUTPUT FILES

#local file paths
proc_monitoring_path = cfg.get('I/O','OutputFilePathMonitoring')
rainfall_path = cfg.get('I/O','RainfallFilePath') #dropbox

#file names
proc_monitoring_file = cfg.get('I/O','CSVFormat')
rainfall_file = cfg.get('I/O','CSVFormat')

#ALERT CONSTANTS
T_disp = cfg.getfloat('I/O','T_disp')  #m
T_velA1 = cfg.getfloat('I/O','T_velA1') #m/day
T_velA2 = cfg.getfloat('I/O','T_velA2')  #m/day
k_ac_ax = cfg.getfloat('I/O','k_ac_ax')
num_nodes_to_check = cfg.getint('I/O','num_nodes_to_check')



#MAIN

#1. setting monitoring window
roll_window_numpts, end, start, offsetstart, monwin = set_monitoring_window(roll_window_length,data_dt,rt_window_length,num_roll_window_ops)

end = datetime.now()
#end = roundtime(end)
start = end - timedelta(days=10)

index = pd.date_range(end-timedelta(10), periods=11, freq='D')
columns=['maxhalf','max']
base = pd.DataFrame(index=index, columns=columns)

#rd.getrain()

tsn=end.strftime("%Y-%m-%d %H-%M-%S")
#tmx=pd.read_csv("C:\Users\Dynaslope\Desktop\Fresh\Local\\rainlist.csv",
#                         names=['site','twoyrmx'],index_col=None)

tmx = GetRainList()
oldsite = []
newsite = []
twoyrmx = []
name = []
for s in tmx:
    oldsite += [s.oldsite]
    newsite += [s.newsite]
    twoyrmx += [s.twoyrmx]
    name += [s.name]
for s in range(len(name)):
    name[s] = name[s][0:3]+'w'
oldrainlist = pd.DataFrame({'twoyrmx': twoyrmx, 'site': oldsite})
newrainlist = pd.DataFrame({'twoyrmx': twoyrmx, 'site': newsite})
allrainlist = pd.DataFrame({'twoyrmx': twoyrmx, 'site': name})
rainlist = newrainlist.fillna(oldrainlist)
rainlist = rainlist.fillna(allrainlist)
rainlist =rainlist.dropna()
rainlist = rainlist.drop_duplicates(['site'], take_last = True)
site = rainlist['site'].values
twoyrmx = rainlist['twoyrmx'].values

#empty dataframe
#index=range(len(tmx))
index = range(len(rainlist))
columns=['site','1D','3D','DataSource','alert','advisory']
summary = pd.DataFrame(index=index, columns=columns)


#alert summary container, r0 sites at alert[0], r1 sites at alert[1], nd sites at alert[2]
alert = [[],[],[],[]]

#for s in range(len(tmx)):
for s in range(len(rainlist)):
    
#    r,twoyrmax=tmx['site'][s],tmx['twoyrmx'][s]
    r,twoyrmax = site[s], twoyrmx[s]
    halfmax=twoyrmax/2
    print r
    print "GR"
    
    try:
        print"\n"
        print "Generating Rainfall plots for "+r+" from rain gauge data"
        
#        rainfall=pd.read_csv(rainfall_path+r+rainfall_file,parse_dates='timestamp',
#                             usecols=['timestamp','rain'],index_col='timestamp')
        rainfall = GetRawRainData(r, start)
        rainfall = rainfall.set_index('ts')
        
        if rainfall.index[-1:]<end:
            blankdf_time=pd.date_range(start=start, end=end, freq='15Min',name='timestamp', closed=None)
            blankdf=pd.DataFrame(data=np.nan*np.ones(len(blankdf_time)), index=blankdf_time,columns=['rain'])
            blankdf=blankdf[-1:]
            rainfall=rainfall.append(blankdf)
        
#        rainfall=rainfall[(rainfall.index>=start)] #not needed in db based input
#        rainfall=rainfall[(rainfall.index<=end)] #not needed in db based input
        
        rainfall=rainfall.resample('15min',how='sum')
        if len(rainfall.index)<1:
            print "No data within desired window"
            print "Generating Rainfall ASTI plots for "+r+" due to lack of rain gauge data within desired window"
            rd.getrain(r)
            ASTIplot(r,offsetstart,end,tsn)
            datasource="ASTI (Empty Rain Gauge Data)"
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
            rd.getrain(r)
            ASTIplot(r,offsetstart,end,tsn)
            datasource="ASTI (No Rain Gauge Data)"
            summary_writer(s,r,datasource,twoyrmax,halfmax,summary,alert)
            
        except:
            datasource="No Alert! No ASTI/SENSLOPE Data"
            summary_writer(s,r,datasource,twoyrmax,halfmax,summary,alert)
            continue

summary.to_csv('C:\Users\Dynaslope\Desktop\\Fresh\Local\Rainfall Plots\\'+'Summary of Rainfall Alert Generation for '+tsn+proc_monitoring_file,sep=',',mode='w')
print summary

#Summarizing rainfall data to rainfallalerts.txt
with open (proc_monitoring_path+"rainfallalert.txt", 'wb') as t:
    t.write('As of ' + end.strftime('%Y-%m-%d %H:%M') + ':\n')
    t.write ('nd: ' + ','.join(sorted(alert[3])) + '\n')
    t.write ('r0: ' + ','.join(sorted(alert[0])) + '\n')
    t.write ('r1a: ' + ','.join(sorted(alert[1])) + '\n')
    t.write ('r1b: ' + ','.join(sorted(alert[2])) + '\n')


# Deleting old data files (less than 10 days)
for dirpath, dirnames, filenames in os.walk('C:\Users\Dynaslope\Desktop\\Fresh\Local\Rainfall Plots\\'):
    for file in filenames:
        curpath = os.path.join(dirpath, file)
        file_modified = datetime.fromtimestamp(os.path.getmtime(curpath))
        if datetime.now() - file_modified > timedelta(days = 10):
            os.remove(curpath)
    