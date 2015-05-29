import pandas as pd
import ConfigParser
from datetime import datetime, timedelta, date, time

cfg = ConfigParser.ConfigParser()
cfg.read('IO-config.txt')
columnproperties_path = cfg.get('I/O','ColumnPropertiesPath')
columnproperties_file = cfg.get('I/O','ColumnProperties')
columnproperties_headers = cfg.get('I/O','columnproperties_headers').split(',')

proc_monitoring_path = cfg.get('I/O','OutputFilePathMonitoring2')
now = datetime.now()
now_Year=now.year
now_month=now.month
now_day=now.day
now_hour=now.hour
now_minute=now.minute
if now_minute<30:now_minute=0
else:now_minute=30
now=datetime.combine(date(now_Year,now_month,now_day),time(now_hour,now_minute,0))
start=now - timedelta(hours=24)
fmt = '%Y-%m-%d %H:%M'

def getwebtrend ():

    
    sensors=pd.read_csv(columnproperties_path+columnproperties_file,names=columnproperties_headers,index_col=None)
    names = ['ts'] + pd.Series.tolist(sensors.colname)
    alert = pd.read_csv(proc_monitoring_path+'webtrends.csv', names=names)
    alert = alert.drop_duplicates(subset='ts')
    alert['ts'] = pd.to_datetime(alert['ts'], format=fmt)
    
    alert = alert.set_index(pd.DatetimeIndex(alert['ts']))
    alert = alert.drop('ts', axis = 1)
    alert = alert[start:now]
    
    return alert.T