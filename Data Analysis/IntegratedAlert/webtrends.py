import os
from datetime import datetime, timedelta, date, time
import pandas as pd
from pandas.stats.api import ols
import numpy as np
import ConfigParser
from collections import Counter
import csv
import fileinput
import sys

#include the path of "Data Analysis" folder for the python scripts searching
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
    sys.path.insert(1,path)
del path   

from querySenslopeDb import *

#Function for directory manipulations
def up_one(p):
    out = os.path.abspath(os.path.join(p, '..'))
    return out

output_path = up_one(up_one(up_one(os.path.dirname(__file__))))

cfg = ConfigParser.ConfigParser()
cfg.read(up_one(os.path.dirname(__file__))+'/server-config.txt')

#INPUT/OUTPUT FILES

#local file paths
output_file_path = output_path + cfg.get('I/O','OutputFilePath')

#file names
CSVFormat = cfg.get('I/O','CSVFormat')
webtrends = cfg.get('I/O','webtrends')


sensorlist = GetSensorList()

site_code = []

for s in sensorlist:
    site_code += [s.name]
    
alerts = pd.read_csv(output_file_path + webtrends, sep = ';', names = ['ts', 'alert'])
alerts.ts = pd.to_datetime(alerts.ts)
alerts = alerts.set_index('ts')

while True:
    timestamp = raw_input('timestamp of data in YYYY-MM-DD HH:MM (e.g. 2016-04-14 14:00): ')

    try:
        timestamp = pd.to_datetime(timestamp)
    except:
        print 'incorrect timestamp format'
        continue
    
    timestamp_Year = timestamp.year
    timestamp_month = timestamp.month
    timestamp_day = timestamp.day
    timestamp_hour = timestamp.hour
    timestamp_minute = timestamp.minute
    if timestamp_minute < 30: timestamp_minute = 0
    else: timestamp_minute = 30
    timestamp = datetime.combine(date(timestamp_Year,timestamp_month,timestamp_day),time(timestamp_hour,timestamp_minute))

#    try:    
    site_alert = alerts.loc[alerts.index == timestamp]['alert'].values[0].split(',')
#    except:
#        print 'no data for the given timestamp'
#        continue
    
    site_code = site_code[0:len(site_alert)]
    
    df = pd.DataFrame({'site_code': site_code, 'site_alert': site_alert})
    df.set_index('site_code', inplace = True)
    
    print "\nChoose among the following sites:"
    print "##########################################"
    for n in range(6,len(site_code),6):
        space_length = 42 - (len(' '.join(site_code[n-6:n])) + 4)
        start_space = (' ' * (space_length / 2)) + (' ' * (space_length % 2))
        end_space = ' ' * (space_length / 2)
        print '##' + start_space + ' '.join(site_code[n-6:n]) + end_space + '##'
    space_length = 42 - (len(' '.join(site_code[n:len(site_code)])) + 4)
    start_space = (' ' * (space_length / 2)) + (' ' * (space_length % 2))
    end_space = ' ' * (space_length / 2)
    print '##' + start_space + ' '.join(site_code[n:len(site_code)]) + end_space + '##'
    print "##########################################"
    print "If site is not among the choices, alert is nd"
    print "Separated by a comma if more than 1 site (e.g. agbsb, nurta)"
    
    while True:
        sites = raw_input('Choose site: ')
        sites = sites.replace(' ', '')
        sitelist = sites.split(',')
        if not all(s in site_code for s in sitelist):
            print "Please answer only the given sites"
            continue
        else:
            sitelist = sites.split(',')
            df2 = pd.DataFrame(data = None)
            for i in sitelist:
                df2 = pd.concat([df2, df.loc[df.index == i]])
            print df2
            break
        
    while True:
        another_timestamp = raw_input('Would you like to look for other timestamp or site? (Y or N): ')
        if another_timestamp != 'Y' and another_timestamp != 'N':
            print 'Answer only Y or N'
            continue
        else:
            break
    
    if another_timestamp == 'Y':
        continue
    elif another_timestamp == 'N':
        break