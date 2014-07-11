import MySQLdb
import ConfigParser
from datetime import datetime as dtm
from datetime import timedelta as tda
import re
import pandas.io.sql as psql
import pandas as pd

# Scripts for connecting to local database
# Needs config file: server-config.txt

class columnArray:
    def __init__(self, name, number_of_segments, segment_length):
        self.name = name
        self.nos = number_of_segments
        self.seglen = segment_length     


def SenslopeDBConnect(nameDB):
    while True:
        try:
            db = MySQLdb.connect(host = Hostdb, user = Userdb, passwd = Passdb, db=nameDB)
            cur = db.cursor()
            return db, cur
        except MySQLdb.OperationalError:
            print '.',

def PrintOut(line):
    if printtostdout:
        print line
		
def CreateTable(table_name, nameDB):
    db = MySQLdb.connect(host = Hostdb, user = Userdb, passwd = Passdb)
    cur = db.cursor()
    cur.execute("CREATE DATABASE IF NOT EXISTS %s" %nameDB)
    cur.execute("USE %s"%nameDB)
    cur.execute("CREATE TABLE IF NOT EXISTS %s(timestamp datetime, id int, xvalue int, yvalue int, zvalue int, mvalue int, PRIMARY KEY (timestamp, id))" %table_name)
    db.close()
	
	
def GetDBResultset(query):
    a = ''
    try:
        db, cur = SenslopeDBConnect(Namedb)

        a = cur.execute(query)

        db.close()
    except:
        PrintOut("Exception detected")

    if a:
        return cur.fetchall()
    else:
        return ""

def GetDBDataFrame(query):
    a = ''
    try:
        db, cur = SenslopeDBConnect(Namedb)

        df = psql.read_sql(query, db)
        df.columns = ['ts','id','x','y','z','m']
        # change ts column to datetime
        df.ts = pd.to_datetime(df.ts)

        db.close()
        return df
    except KeyboardInterrupt:
        PrintOut("Exception detected in accessing database")

def GetRawColumnData(siteid = "", fromTime = "", maxnode = 40):

    if not siteid:
        raise ValueError('no site id entered')
    
    if printtostdout:
        PrintOut('Querying database ...')

    cond = "(xvalue < %s or abs(yvalue) > %s or abs(zvalue) > %s)" % (xlim,ylim,zlim)
    query = "select timestamp, id, \
            if(%s, null, xvalue) xvalue, \
            if(%s, null, yvalue) yvalue, \
            if(%s, null, zvalue) zvalue, \
            if(mvalue < %s or mvalue > %s, null, mvalue) mvalue \
            from senslopedb.%s" % (cond,cond,cond,mlowlim,muplim,siteid)
    
    query = "select * from (%s) query1 where (xvalue is not null or mvalue is not null)" % query

    if fromTime:
        query = query + " and timestamp > '%s'" % fromTime

    query = query + " and id >= 1 and id <= %s ;" % (str(maxnode))

    return GetDBDataFrame(query)

def GetSensorList():
    try:
        db, cur = SenslopeDBConnect(Namedb)
        cur.execute("use "+ Namedb)
        
        query = 'SELECT name, num_nodes, seg_length FROM site_column_props inner join site_column on site_column_props.s_id=site_column.s_id'
        #try:
        cur.execute(query)
        #except:
        #print '>> Error parsing database'
        
        data = cur.fetchall()
        
        # make a sensor list of columnArray class functions
        sensors = []
        for entry in data:
            s = columnArray(entry[0],int(entry[1]),float(entry[2]))
            sensors.append(s)
        
        return sensors
    except:
        print '>> Error getting list from database'
        return ''

    
# import values from config file
configFile = "server-config.txt"
cfg = ConfigParser.ConfigParser()
cfg.read(configFile)

DBIOSect = "DB I/O"
Hostdb = cfg.get(DBIOSect,'Hostdb')
Userdb = cfg.get(DBIOSect,'Userdb')
Passdb = cfg.get(DBIOSect,'Passdb')
Namedb = cfg.get(DBIOSect,'Namedb')
NamedbPurged = cfg.get(DBIOSect,'NamedbPurged')
printtostdout = cfg.getboolean(DBIOSect,'Printtostdout')

valueSect = 'Value Limits'
xlim = cfg.get(valueSect,'xlim')
ylim = cfg.get(valueSect,'ylim')
zlim = cfg.get(valueSect,'zlim')
xmax = cfg.get(valueSect,'xmax')
mlowlim = cfg.get(valueSect,'mlowlim')
muplim = cfg.get(valueSect,'muplim')
islimval = cfg.getboolean(valueSect,'LimitValues')







