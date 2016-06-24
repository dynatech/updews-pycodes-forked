#Importing relevant functions
from datetime import datetime, date, time
import pandas as pd
import numpy as np
import ConfigParser
from scipy import stats
import os
import sys
import platform
from sqlalchemy import create_engine

#Include the path of "Data Analysis" folder for the python scripts searching
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
    sys.path.insert(1,path)
del path 
from querySenslopeDb import *

#Check the current OS of the machine
curOS = platform.system()

#import MySQLdb according to the OS
if curOS == "Windows":
    import MySQLdb
elif curOS == "Linux":
    import pymysql as MySQLdb

#Defining important local functions

def up_one(p):
    #INPUT: Path or directory
    #OUTPUT: Parent directory
    out = os.path.abspath(os.path.join(p, '..'))
    return out  

def uptoDB_gndmeas_alerts(df):
    #INPUT: Dataframe containing all alerts
    #OUTPUT: Writes to sql all ground measurement related alerts database
    engine=create_engine('mysql://root:senslope@192.168.1.102:3306/senslopedb')
    df.to_sql(name = 'gndmeas_alerts', con = engine, if_exists = 'append', schema = Namedb, index = True)

def uptoDB_site_level_alerts(df):
    #INPUT: Dataframe containing site level alerts
    #OUTPUT: Writes to sql database the alerts of sites who recently changed their alert status
    
    #Get the latest site_level_alert
    query = 'SELECT s1.timestamp,s1.site,s1.source,s1.alert, COUNT(*) num FROM senslopedb.site_level_alert s1 JOIN senslopedb.site_level_alert s2 ON s1.site = s2.site AND s1.source = s2.source AND s1.timestamp <= s2.timestamp group by s1.timestamp,s1.site, s1.source HAVING COUNT(*) <= 1 ORDER BY site, source, num desc'
    df2 = GetDBDataFrame(query)
    
    #Merge the two data frames to determine overlaps in alerts
    overlap = pd.merge(df,df2,how = 'left', on = ['site','source','alert'],suffixes=['','_r'])
    
    #Get the site with change in its latest alert
    to_db = df[overlap['timestamp_r'].isnull()]
    to_db = to_db[['timestamp','site','source','alert']].set_index('timestamp')
    
    engine=create_engine('mysql://root:senslope@192.168.1.102:3306/senslopedb')
    to_db.to_sql(name = 'site_level_alert', con = engine, if_exists = 'append', schema = Namedb, index = True)

def get_latest_ground_df():
    #OUTPUT: Dataframe of the last 4 recent ground measurement in the database
    query = 'SELECT g1.timestamp,g1.site_id,g1.crack_id,g1.meas, COUNT(*) num FROM senslopedb.gndmeas g1 JOIN senslopedb.gndmeas g2 ON g1.site_id = g2.site_id AND g1.crack_id = g2.crack_id AND g1.timestamp <= g2.timestamp group by g1.timestamp,g1.site_id, g1.crack_id HAVING COUNT(*) <= 4 ORDER BY site_id, crack_id, num desc'
    df = GetDBDataFrame(query)
    return df[['timestamp','site_id','crack_id','meas']]

def crack_eval(df,end):
    #INPUT: df containing crack parameters
    #OUTPUT: crack alert according to protocol table
    
    #Obtain the time difference and displacement between the latest values
    if len(df) >= 2:
        time_delta = (df.timestamp.iloc[-1]  - df.timestamp.iloc[-2]) / np.timedelta64(1,'D')
        abs_disp = np.abs(df.meas.iloc[-1]-df.meas.iloc[-2])
        
        crack_alert = 'nd'    
        
        #Based on alert table
        if time_delta >= 7:
            if abs_disp >= 75:
                crack_alert = 'l3'
            elif abs_disp >= 3:
                crack_alert = 'l2'
            else:
                crack_alert = 'l0'
        elif time_delta >= 3:
            if abs_disp >= 30:
                crack_alert = 'l3'
            elif abs_disp >= 1.5:
                crack_alert = 'l2'
            else:
                crack_alert = 'l0'
        elif time_delta >= 1:
            if abs_disp >= 10:
                crack_alert = 'l3'
            elif abs_disp >= 0.5:
                crack_alert = 'l2'
            else:
                crack_alert = 'l0'
        else:
            if abs_disp >= 5:
                crack_alert = 'l3'
            elif abs_disp >= 0.5:
                crack_alert = 'l2'
            else:
                crack_alert = 'l0'
        
        #Perform p value computation for specific crack
        if abs_disp >= 0.5 and abs_disp <= 1:
            if len(df) >= 4:
                #get the last 4 data values for the current feature
                last_cur_feature_measure = df.meas.values
                last_cur_feature_time = (df.timestamp.values - df.timestamp.values[0])/np.timedelta64(1,'D')
    
                #perform linear regression to get p value
                m, b, r, p, std = stats.linregress(last_cur_feature_time,last_cur_feature_measure)
                
                #Evaluate p value
                if p <= 0.05:
                    crack_alert = 'l0p'
    else:
        crack_alert = 'l0'
        
    #Impose the 4 hour validity of the groundmeasurement
    try:
        if end - df.timestamp.iloc[-1] > np.timedelta64(4,'h'):
            crack_alert = 'nd'
    except:
        print 'Timestamp error for '+' '.join(list(np.concatenate((df.site_id.values,df.crack_id.values))))
        crack_alert = 'nd'
    
    return crack_alert

def site_eval(df):
    #INPUT: Dataframe containing crack alerts
    #OUTPUT: Site alert based on the higher perceived risk from the crack alerts, list of cracks that exhibited l2 or l3 alerts, and timestamp

    crack_alerts = df.crack_alerts.values
    if 'l3' in crack_alerts:
        site_alert = 'l3'
    elif 'l2' in crack_alerts:
        site_alert = 'l2'
    elif 'l0p' in crack_alerts:
        site_alert = 'l0p'
    elif 'l0' in crack_alerts:
        site_alert = 'l0'
    else:
        site_alert = 'nd'
    
    #Determine which crack has an l2 or l3 alert
    mask = np.logical_or(crack_alerts == 'l3', crack_alerts == 'l2')
    cracks_to_check = df.crack_id.values[mask]
    
    return pd.Series([site_alert,', '.join(list(cracks_to_check))], index = ['site_alert','cracks_to_check'])

def GenerateGroundDataAlert():

    #Monitoring output directory
    path2 = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    out_path = up_one(up_one(path2)) 
    
    cfg = ConfigParser.ConfigParser()
    cfg.read(path2 + '/server-config.txt')
    
    #Retrieving important declaration files
    printtostdout = True
    output_file_path = cfg.get('I/O','OutputFilePath')
    PrintJSON = cfg.get('I/O','PrintJSON')
    PrintGAlert = cfg.get('I/O','PrintGAlert')
    Namedb = cfg.get('DB I/O','Namedb') 
    
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
#    Use this so set the end time    
#    end = datetime(2016,06,21,12,00,00)

############################################ MAIN ############################################

    #Step 1: Get the ground data from local database 
    df = get_latest_ground_df()
    
    #lower caps all site_id names while cracks should be in title form
    df['site_id'] = map(lambda x: x.lower(),df['site_id'])
    df['crack_id'] = map(lambda x: x.title(),df['crack_id'])
    
    #Step 2: Evaluate the alerts per crack
    crack_alerts = df.groupby(['site_id','crack_id']).apply(crack_eval,end).reset_index(name = 'crack_alerts')
    
    #Step 3: Evaluate alerts per site
    site_alerts = crack_alerts.groupby(['site_id']).apply(site_eval).reset_index()    
    
    #Step 4: Include the timestamp of the run, create release ready data frame
    ground_alert_release = site_alerts
    ground_alert_release['timestamp'] = end
    ground_alert_release.columns = ['site','alert','cracks','timestamp']
    ground_alert_release = ground_alert_release.set_index(['timestamp'])
            
    print ground_alert_release
    
    #Step 5: Upload the results to the gndmeas_alerts database
    uptoDB_gndmeas_alerts(ground_alert_release)
    
    #Step 6: Upload to site_level_alert        
    ground_site_level = ground_alert_release.reset_index()
    del ground_site_level['cracks']
    ground_site_level['source'] = 'ground'
    
    uptoDB_site_level_alerts(ground_site_level[['timestamp','site','source','alert']])


################## #Stand by Functionalities

#    if PrintGAlert:
#        #Creating Monitoring Output directory if it doesn't exist
#        print_out_path = out_path + output_file_path
#        print print_out_path        
#        if not os.path.exists(print_out_path):
#            os.makedirs(print_out_path)
#        
#        print "Ground measurement report as of {}".format(end)
#        print "{:5}: {:5}; Last Date of Measurement; Features to Check".format('Site','Alert')
#        i = 0
#        for site, galert in ground_alert_release:
#            print "{:5}: {:5}; {:24}; {}".format(site,galert[0],str(galert[1]),galert[2])
#            i += 1
#        
#
#        with open (print_out_path+'groundalert.txt', 'w') as t:
#            i = 0
#            t.write("Ground measurement report as of {}".format(end)+'\n')
#            t.write("{:5}: {:5}; Last Date of Measurement; Features to Check".format('Site','Alert')+'\n')
#            for site, galert in ground_alert_release:
#                t.write ("{:5}: {:5}; {:25}; {}".format(site,galert[0],str(galert[1]),galert[2])+'\n')
#                i += 1

#    if PrintJSON:        
#        #converting the data frame to JSON format
#        dfajson = ground_alert_release.to_json(orient="records",date_format='iso')
#        
#        #ensuring proper datetime format
#        i = 0
#        while i <= len(dfajson):
#            if dfajson[i:i+9] == 'timestamp':
#                dfajson = dfajson[:i] + dfajson[i:i+36].replace("T"," ").replace("Z","").replace(".000","") + dfajson[i+36:]
#                i += 1
#            else:
#                i += 1
#        print dfajson
#    print dfa[['alert_timestamp','site_id','g alert','features to check']].set_index(['alert_timestamp'])
#    uptoDB(dfa[['alert_timestamp','site_id','g alert','features to check']].set_index(['alert_timestamp']))
