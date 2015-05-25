import pandas as pd
import ConfigParser

cfg = ConfigParser.ConfigParser()
cfg.read('IO-config.txt')
columnproperties_path = cfg.get('I/O','ColumnPropertiesPath')
columnproperties_file = cfg.get('I/O','ColumnProperties')
columnproperties_headers = cfg.get('I/O','columnproperties_headers').split(',')

proc_monitoring_path = cfg.get('I/O','OutputFilePathMonitoring2')

def getwebtrend ():
    
    sensors=pd.read_csv(columnproperties_path+columnproperties_file,names=columnproperties_headers,index_col=None)
    names = ['ts'] + pd.Series.tolist(sensors.colname)
    alert = pd.read_csv(proc_monitoring_path+'webtrends.csv', names=names)
    alert = alert.drop_duplicates(subset='ts')
        
    
    return alert.T