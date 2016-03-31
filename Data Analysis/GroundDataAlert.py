
import os
from datetime import datetime, date, time, timedelta
import pandas as pd
from pandas.stats.api import ols
import numpy as np
import matplotlib.pyplot as plt
import ConfigParser
from collections import Counter
import csv
import fileinput
from querySenslopeDb import *
from scipy import stats

import generic_functions as gf
import generateProcMonitoring as genproc
import alertEvaluation as alert

#Step 0: Specify mode of output, mode = 1: txt1; mode = 2 txt 2; mode = 3 json
mode = 1
output_file_path = cfg.get('I/O','OutputFilePath')

#Set the date of the report as the current date rounded to HH:30 or HH:00
end=datetime.now()
end_Year=end.year
end_month=end.month
end_day=end.day
end_hour=end.hour
end_minute=end.minute
if end_minute<30:end_minute=0
else:end_minute=30

end=datetime.combine(date(end_Year,end_month,end_day),time(end_hour,end_minute,0))
#end = datetime(2016,03,10,12,00,00)
#Set the container of the date of measurements
measurement_dates = []




#Step 1: Get the ground data from local database 

#Set the ground data alert as dict
ground_data_alert = {}

#connecting to localdb
db = MySQLdb.connect(host = Hostdb, user = Userdb, passwd = Passdb)
cur = db.cursor()
cur.execute("USE %s"%Namedb)

#get all the ground data from local database
query = "SELECT * FROM gndmeas g;"
df =  GetDBDataFrame(query)

#reindexing using timestamp
df.timestamp = pd.to_datetime(df.timestamp)
#df = df.reindex(index = index)
df = df.set_index(['timestamp'])
df = df[['site_id','crack_id','meas']]

#Step 2: Evaluate the ground measurement per site
sitelist = np.unique(df['site_id'].values)

for cur_site in sitelist:
    
#    if cur_site != 'Nin':
#        continue
    
    df_cur_site = df[df['site_id']==cur_site]
    df_cur_site.sort(inplace = True)    
    
    #get the latest timestamp as reference for the latest data record it on the date of measurement container
    last_data_time = df_cur_site.index[-1]
    measurement_dates.append(last_data_time)
    
    print df_cur_site
    
    #Evaluate ground measurement per crack
    site_eval = []
    to_p_value = False
    
    featurelist = np.unique(df_cur_site['crack_id'].values)
    for cur_feature in featurelist:
        df_cur_feature = df_cur_site[df_cur_site['crack_id']==cur_feature]
        
        #disregard the current feature if the time of latest measurement is not the most recent
        if df_cur_feature.index[-1] != last_data_time:
            continue
        
        feature_measure = df_cur_feature['meas'].values
           
        
        #get the time delta of the last two values
        try:
            time_delta_last = (df_cur_feature.index[-1] - df_cur_feature.index[-2])/np.timedelta64(1,'D')
            feature_displacement = abs(feature_measure[-1] - feature_measure[-2])
             
            print df_cur_feature, time_delta_last, feature_displacement
                
            #Check if p value computation is needed
            if feature_displacement <= 1:
                to_p_value = True
            
        except IndexError:
            print "Site: '{}' Feature: '{}' has {} measurement".format(cur_site,cur_feature,len(df_cur_feature))
            continue
        
        #Evaluating the Alert of the specific crack base on look up table
        if time_delta_last >= 7:
            if feature_displacement >= 75:
                feature_alert = 'L2'
            elif feature_displacement >= 3:
                feature_alert = 'L1'
            else:
                feature_alert = 'L0'
        elif time_delta_last >= 3:
            if feature_displacement >= 30:
                feature_alert = 'L2'
            elif feature_displacement >= 1.5:
                feature_alert = 'L1'
            else:
                feature_alert = 'L0'
        elif time_delta_last >= 1:
            if feature_displacement >= 10:
                feature_alert = 'L2'
            elif feature_displacement >= 0.5:
                feature_alert = 'L1'
            else:
                feature_alert = 'L0'
        else:
            if feature_displacement >= 5:
                feature_alert = 'L2'
            elif feature_displacement >= 0.5:
                feature_alert = 'L1'
            else:
                feature_alert = 'L0'
        
        #Perform p value computation for specific crack
        if to_p_value:
            if len(feature_measure) >= 4:
                #get the last 4 data values for the current feature
            
                df_last_cur_feature = df_cur_feature.tail(4)
                last_cur_feature_measure = df_last_cur_feature['meas'].values
                last_cur_feature_time = (df_last_cur_feature.index - df_last_cur_feature.index[0])/np.timedelta64(1,'D')

                #perform linear regression to get p value
                m, b, r, p, std = stats.linregress(last_cur_feature_time,last_cur_feature_measure)
                
                #Evaluate p value
                if p <= 0.05:
                    feature_alert = 'L0p'
                    
        site_eval.append(feature_alert)
        
    #Evaluate site alert based on crack alerts
    if end - last_data_time > np.timedelta64(4, 'h'):
        ground_data_alert.update({cur_site:'ND'})
    else:
        if 'L2' in site_eval:
            ground_data_alert.update({cur_site:'L2'})
        elif 'L1' in site_eval:
            ground_data_alert.update({cur_site:'L1'})
        elif 'L0p' in site_eval:
            ground_data_alert.update({cur_site:'L0p'})
        else:
            ground_data_alert.update({cur_site:'L0'})
    
    #change dict format to tuple for more easy output writing
    ground_alert_release = sorted(ground_data_alert.items())
    
if mode == 1:
    print "Ground measurement report as of {}".format(end)
    print "{:5}: {:5}; Last Date of Measurement".format('Site','Alert')
    i = 0
    for site, galert in ground_alert_release:
        print "{:5}: {:5}; {}".format(site,galert,measurement_dates[i])
        i += 1
    with open (output_file_path+'groundalert.txt', 'w') as t:
        i = 0
        t.write("Ground measurement report as of {}".format(end)+'\n')
        for site, galert in ground_alert_release:
            t.write ("{:5}: {:5}; {}".format(site,galert,measurement_dates[i])+'\n')
            i += 1

#if mode == 2:
#    print "As of {}".format(end)
#    ground_data_alert2 = {}
#    for site, galert in ground_data_alert.iteritems():
#        ground_data_alert2.setdefault(galert, []).append(site)
#    ground_alert_release2 = sorted(ground_data_alert2.items())
#    i = 0
#    for galert, site in ground_alert_release2:
#        print "{:3}: {}; {}".format(galert, ','.join(sorted(site)), measurement_dates[i])
#        i += 1

if mode == 3:
    #create data frame as for easy conversion to JSON format
    
    for i in range(len(ground_alert_release)): ground_alert_release[i] = (measurement_dates[i],) + ground_alert_release[i]
    dfa = pd.DataFrame(ground_alert_release,columns = ['timestamp','site_id','g alert'])
    
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
    print dfajson