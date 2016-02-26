import os
from datetime import datetime, timedelta
import pandas as pd
from pandas.stats.api import ols
import numpy as np
import matplotlib.pyplot as plt
import ConfigParser
from collections import Counter
import csv
import fileinput
from querySenslopeDb import *

import generic_functions as gf
import generateProcMonitoring as genproc
import alertEvaluation as alert

#Step 0: File path and initializations
cfg = ConfigParser.ConfigParser()
cfg.read('server-config.txt')  

nd_path = cfg.get('I/O', 'NDFilePath')
output_file_path = cfg.get('I/O','OutputFilePath')
proc_file_path = cfg.get('I/O','ProcFilePath')
ColAlerts_file_path = cfg.get('I/O','ColAlertsFilePath')
TrendAlerts_file_path = cfg.get('I/O','TrendAlertsFilePath')

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

#sitelist
sitelist = ['Agb','Bak','Ban','Bar','Bat','Bay','Blc','Bol','Car','Cud','Dad','Gaa','Gam','Hin','Hum','Ime','Imu','Ina','Kan','Lab','Lay','Lip','Lpa','Lun','Mag','Mam','Man','Mar','Mca','Messb','Mesta','Nag','Nur','Osl','Pan','Par','Pep','Pin','Pla','Pob','Pug','Sag','Sib','Sin','Sum','Tag','Tal','Tam','Tue','Umi']

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
    del alerts['']
        
#Step 2: Collect eq alerts
with open (output_file_path+eqsummary) as eqalert:
    n = 0
    for line in eqalert:
        if n == 0:
            timestamp = line.split('of')[1][1:-2]
        elif n == 1 and line[-2:] == 'E0':
            for e in sitelist:
                alerts[e] = alerts[e] + ('E0',timestamp)
        elif line.split('of')[-1][-2:] == 'E1':
                alerts[e] = alerts[e] + ('E1',timestamp)
        else:
            n+=1
            continue
        n+=1

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
            sensor_alerts.update({sensor:salert.replace('\n',"")})
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
            if 'a2' in site_sensor_alert[s]:
                alerts[s] = alerts[s] + ((site_sensor_alert[s] + ('L2s',timestamp)),)
            elif 'a1' in site_sensor_alert[s]:
                alerts[s] = alerts[s] + ((site_sensor_alert[s] + ('L1s',timestamp)),)
            elif 'a0' in site_sensor_alert[s]:
                alerts[s] = alerts[s] + ((site_sensor_alert[s] + ('L0s',timestamp)),)
            else:
                alerts[s] = alerts[s] + ((site_sensor_alert[s] + ('NDs',timestamp)),)
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
                    alerts[site] = alerts[site] + (galert+'g',timestamp)
                else:
                    alerts[site[0:3].title()] = alerts[site[0:3].title()] + (galert+'g',timestamp)
            except:
                pass
        n+=1

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