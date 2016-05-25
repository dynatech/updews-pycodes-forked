import os
from datetime import datetime, timedelta, date, time
import pandas as pd
from pandas.stats.api import ols
import numpy as np
import matplotlib.pyplot as plt
import ConfigParser
from collections import Counter
import csv
import fileinput
from querySenslopeDb import *

def up_one(p):
    out = os.path.abspath(os.path.join(p, '..'))
    return out

def CreateAllAlertsTable(table_name, nameDB):
    db = MySQLdb.connect(host = Hostdb, user = Userdb, passwd = Passdb)
    cur = db.cursor()
    #cur.execute("CREATE DATABASE IF NOT EXISTS %s" %nameDB)
    cur.execute("USE %s"%nameDB)
    cur.execute("CREATE TABLE IF NOT EXISTS %s(timestamp datetime, id int, xvalue int, yvalue int, zvalue int, mvalue int, PRIMARY KEY (timestamp, id))" %table_name)
    db.close()

#Step 0: File path and initializations
cfg = ConfigParser.ConfigParser()
cfg.read('server-config.txt')  

path2 = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
out_path = up_one(path2)

nd_path = out_path + cfg.get('I/O', 'NDFilePath')
output_file_path = out_path + cfg.get('I/O','OutputFilePath')
proc_file_path = out_path + cfg.get('I/O','ProcFilePath')
ColAlerts_file_path = out_path + cfg.get('I/O','ColAlertsFilePath')
TrendAlerts_file_path = out_path + cfg.get('I/O','TrendAlertsFilePath')

CSVFormat = cfg.get('I/O','CSVFormat')
webtrends = cfg.get('I/O','webtrends')
textalert = cfg.get('I/O','textalert')
textalert2 = cfg.get('I/O','textalert2')
rainfallalert = cfg.get('I/O','rainfallalert')
groundalert = cfg.get('I/O','groundalert')
allalerts = cfg.get('I/O','allalerts')
eqsummary = cfg.get('I/O','eqsummary')
timer = cfg.get('I/O','timer')
NDlog = cfg.get('I/O','NDlog')
ND7x = cfg.get('I/O','ND7x')

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


#alert container
alerts = {}
all_alerts = {}

#Get the sitelist from database
sitelist = []
sensorlist = GetSensorList()
for i in sensorlist:
    if i.name != 'messb' and i.name != 'mesta':  
        sitelist.append(i.name[:3].title())
    else:
        sitelist.append(i.name.title())

sitelist = set(sitelist)

#Step 1: Collect Rain Alerts
with open (output_file_path+rainfallalert) as rainalert:
    n = 0
    for line in rainalert:
        if n == 0:
            timestamp = line.split('of')[1][1:-2]
        elif n==1 or n==2:
            ralert,rain_gauges = line.split(':')
            ralert = ralert.upper()
            rain_gauges = rain_gauges.strip().split(',')
            for r in rain_gauges:
                alerts.update({r[:3].title():(ralert,timestamp)})
        elif n == 3 or n==4:
            ralert = line.split(':')[0][:2]
            ralert = ralert.upper()
            rain_gauges = line.split(':')[1].replace(' ',"").replace('\n',"").split(',')
            for r in rain_gauges:
                alerts.update({r[:3].title():(ralert,timestamp)})
        else:
            n+=1
            continue
        n+=1
    alerts['Lpa'] = alerts.pop('Lpt')
    alerts['Messb'] = alerts['Nin']
    alerts['Mesta'] = alerts['Nin']
    try:
        del alerts['']
    except:
        pass
        
#Step 2: Collect eq alerts
#with open (output_file_path+eqsummary) as eqalert:
#    n = 0
#    for line in eqalert:
#        if n == 0:
#            timestamp = line.split('of')[1][1:-2]
#        elif n == 1 and line[-2:] == 'E0':
#            for e in sitelist:
#                alerts[e] = alerts[e] + ('E0',timestamp)
#        elif line.split('of')[-1][-2:] == 'E1':
#                alerts[e] = alerts[e] + ('E1',timestamp)
#        else:
#            n+=1
#            continue
#        n+=1

#Step 3: Collect sensor alerts
with open (output_file_path+textalert) as txtalert_output:
    n = 0
    sensor_alerts = {}
    site_sensor_alert = {}
    for line in txtalert_output:
        if n == 0:
            timestamp = line.split('of')[1][1:-2]
        else:
            sensor,salert = line.split(':')
            sensor_alerts.update({sensor:salert.replace('\n',"").upper()})
        n+=1
    for s in sensor_alerts.keys():
        if s == 'mesta' or s == 'messb':
            site_sensor_alert.update({s.title():(s,sensor_alerts[s])})
        else:
            site = s.title()[:3]
            try:
                site_sensor_alert[site] = site_sensor_alert[site] + (s,sensor_alerts[s])
            except KeyError:
                site_sensor_alert.update({site:(s,sensor_alerts[s])})
    for s in alerts.keys():
        try:
            if 'L3' in site_sensor_alert[s]:
                alerts[s] = alerts[s] + ((site_sensor_alert[s] + ('L3',timestamp)),)
            elif 'L2' in site_sensor_alert[s]:
                alerts[s] = alerts[s] + ((site_sensor_alert[s] + ('L2',timestamp)),)
            elif 'L0' in site_sensor_alert[s]:
                alerts[s] = alerts[s] + ((site_sensor_alert[s] + ('L0',timestamp)),)
            else:
                alerts[s] = alerts[s] + ((site_sensor_alert[s] + ('ND',timestamp)),)
        except KeyError:
            alerts[s] = alerts[s] + (('No Sensor Installed',timestamp),)
#Step 4: Collect Ground Alerts
with open (output_file_path+groundalert) as groundalert_output:
    n = 0
    for line in groundalert_output:
        if n == 0:
            timestamp = line.split('of')[1][1:-2]
        else:
            line = line.split(';')[0]
            site,galert = line.replace(" ","").split(':')
            try:
                galert = galert[0:2]
                if site == 'Mesta' or site == 'Messb':
                    alerts[site] = alerts[site] + (galert,timestamp)
                else:
                    alerts[site[0:3].title()] = alerts[site[0:3].title()] + (galert,timestamp)
            except:
                pass
        n+=1

#Step 5: Collating all alerts, arranging them using timestamp
all_alerts = []
for site in alerts.keys():
    site_alert = {}
    site_alert.update({'site':site})
    for i in range(len(alerts[site])):
        if i == 0:
            site_alert.update({'rain_alert':alerts[site][i]})
        if i == 1:
            site_alert.update({'rain_ts':alerts[site][i]})
        if i == 2:
            for k in range(len(alerts[site][i])):
                if k < len(alerts[site][i])-2:
                    if k%2 == 0:
                        site_alert.update({'sensor_{}'.format(int(0.5*k+1)):alerts[site][i][k]})
                    else:
                        site_alert.update({'sensor_{}_alert'.format(int(0.5*(k+1))):alerts[site][i][k]})
                elif k == len(alerts[site][i])-2:
                    site_alert.update({'sensor_all_alert':alerts[site][i][k]})
                else:
                    site_alert.update({'sensor_ts':alerts[site][i][k]})
        if i == 3:
            site_alert.update({'ground_alert':alerts[site][i]})
        if i == 4:
            site_alert.update({'ground_ts':alerts[site][i]})
    all_alerts.append(site_alert)

df_all_alerts = pd.DataFrame(all_alerts)
df_all_alerts = df_all_alerts[['site','rain_ts','rain_alert','sensor_ts','sensor_1','sensor_1_alert','sensor_2','sensor_2_alert','sensor_3','sensor_3_alert','sensor_all_alert','ground_ts','ground_alert']]

#Evaluate Landslide Alert
sensor_all_alert = df_all_alerts['sensor_all_alert'].values
ground_alert = df_all_alerts['ground_alert'].values
landslide_alert = []
for g_alert,s_alert in zip(ground_alert,sensor_all_alert):
    if g_alert == 'L3' or s_alert == 'L3':
        landslide_alert.append('L3')
    elif g_alert == 'L2' or s_alert == 'L2':
        landslide_alert.append('L2')
    elif g_alert == 'L0' or s_alert == 'L0' or g_alert == 'L0p':
        landslide_alert.append('L0')
    else:
        landslide_alert.append('ND')

df_all_alerts['landslide_alert'] = landslide_alert

#Evaluate Current Public Alert

#Get array of operational triggers
rain_alert = df_all_alerts['rain_alert'].values
landslide_alert = df_all_alerts['landslide_alert'].values
public_alert = []
for r_alert,l_alert in zip(rain_alert,landslide_alert):
    if l_alert == 'L3':
        public_alert.append('A3')
    elif l_alert == 'L2':
        public_alert.append('A2')
    elif r_alert == 'R1':
        public_alert.append('A1')
    else:
        public_alert.append('A0')

df_all_alerts['public_alert'] = public_alert



#Get the latest public alert from database for each site

#Create Database table if all_alerts does not exists



#Step 5: Create json ready format
alerts_release = sorted(alerts.items())
for i in range(len(alerts_release)): alerts_release[i] = (end,) + alerts_release[i]
dfa = pd.DataFrame(alerts_release,columns = ['timestamp','site','alerts'])

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
print end