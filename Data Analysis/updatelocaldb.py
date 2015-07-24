# -*- coding: utf-8 -*-
"""
Created on Tue Jun 02 10:20:04 2015

@author: chocolate server
"""

import urllib
import urllib2
from StringIO import StringIO
import pandas as pd
import MySQLdb, ConfigParser

dbname = "senslopedb"


def getLatestTimestamp(col):
    dbc = MySQLdb.connect(host="127.0.0.1",user="root",passwd="dyn4m1ght",db=dbname)
    cur = dbc.cursor()
    query = "select max(timestamp) from %s.%s" % (dbname, col)
    a = cur.execute(query)
    ret = ''
    try:
        ret = cur.fetchall()[0][0]
    except TypeError:
        print "Error"
        ret = ''
    finally:
        dbc.close()
        return ret
        
def downloadLatestData(col,fromDate='',toDate=''):
    url = 'http://www.dewslandslide.com/ajax/getSenslopeData.php?db=%s&accelsite&site=%s&start=%s&end=%s' % (dbname,col,fromDate,toDate)
    # print url
    print "Downloading", col, "data from", fromDate, "...",
    
    # comment out this part for direct (no proxy) connection
    # urllib2.install_opener(
        # urllib2.build_opener(
            # urllib2.ProxyHandler({'http': 'proxy.engg.upd.edu.ph:8080'})
        # )
    # )    
    
    try:
        f = urllib2.urlopen(url);
        s = f.read().strip()
        conv = StringIO(s)
        df = pd.DataFrame.from_csv(conv, sep=',', parse_dates=False)
        print "done"
        return df
    except urllib2.URLError:
        print "<urlopen error [Errno 10060] A connection attempt failed because the connected party did not properly respond after a period of time, or established connection failed because connected host has failed to respond>"

        
   
def writeDFtoLocalDB(col,df):
    dbc = MySQLdb.connect(host="127.0.0.1",user="root",passwd="dyn4m1ght",db=dbname)
    print "writing df to", col, "db",
    df.to_sql(con=dbc, name=col, if_exists='append', flavor='mysql')
    dbc.close()
    print "done"

#ts = getLatestTimestamp("labb")    
#df = downloadLatestData("labb", "2013-06-01","2013-07-15")
#writeDFtoLocalDB("labb",df)
#local file paths
cfg = ConfigParser.ConfigParser()
cfg.read('IO-config.txt')    

columnproperties_path = cfg.get('I/O','ColumnPropertiesPath')
purged_path = cfg.get('I/O','InputFilePath')
monitoring_path = cfg.get('I/O','MonitoringPath')
LastGoodData_path = cfg.get('I/O','LastGoodData')
proc_monitoring_path = cfg.get('I/O','OutputFilePathMonitoring2')

#file names
columnproperties_file = cfg.get('I/O','ColumnProperties')
purged_file = cfg.get('I/O','CSVFormat')
monitoring_file = cfg.get('I/O','CSVFormat')
LastGoodData_file = cfg.get('I/O','CSVFormat')
proc_monitoring_file = cfg.get('I/O','CSVFormat')

#file headers
columnproperties_headers = cfg.get('I/O','columnproperties_headers').split(',')
purged_file_headers = cfg.get('I/O','purged_file_headers').split(',')
monitoring_file_headers = cfg.get('I/O','monitoring_file_headers').split(',')
LastGoodData_file_headers = cfg.get('I/O','LastGoodData_file_headers').split(',')
proc_monitoring_file_headers = cfg.get('I/O','proc_monitoring_file_headers').split(',')
alert_headers = cfg.get('I/O','alert_headers').split(',')

sensors=pd.read_csv(columnproperties_path+columnproperties_file,names=columnproperties_headers,index_col=None)

for col in sensors['colname']:
	ts = getLatestTimestamp(col)
	ts2 = ts.strftime("%Y-%m-%d+%H:%M:%S")
	df = downloadLatestData(col,ts2)
	writeDFtoLocalDB(col,df)
	
    