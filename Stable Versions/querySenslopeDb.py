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

def GetLatestTimestamp(nameDb, table):
    db = MySQLdb.connect(host = Hostdb, user = Userdb, passwd = Passdb)
    cur = db.cursor()
    #cur.execute("CREATE DATABASE IF NOT EXISTS %s" %nameDB)
    try:
        cur.execute("select max(timestamp) from %s.%s" %(nameDb,table))
    except:
        print "Error in getting maximum timstamp"

    a = cur.fetchall()
    if a:
        return a[0][0]
    else:
        return ''
		
def CreateAccelTable(table_name, nameDB):
    db = MySQLdb.connect(host = Hostdb, user = Userdb, passwd = Passdb)
    cur = db.cursor()
    #cur.execute("CREATE DATABASE IF NOT EXISTS %s" %nameDB)
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

    # adjust out of bounds limits for xvalue mainly
    boundsQuery = "select timestamp, id, \
                  if((xvalue<(1126-4096))&(xvalue>(1024-4096)),xvalue+4096,xvalue) xvalue, \
                  if((yvalue<(1126-4096))&(yvalue>(1024-4096)),yvalue+4096,yvalue) yvalue, \
                  if((zvalue<(1126-4096))&(zvalue>(1024-4096)),zvalue+4096,zvalue) zvalue, \
                  mvalue from senslopedb.%s" % (siteid)

    # nullify all other values once an invalid value is detected
    cond = "(xvalue < %s or abs(yvalue) > %s or abs(zvalue) > %s)" % (xlim,ylim,zlim)
    query = "select timestamp, id, \
            if(%s, null, xvalue) xvalue, \
            if(%s, null, yvalue) yvalue, \
            if(%s, null, zvalue) zvalue, \
            if(mvalue < %s or mvalue > %s, null, mvalue) mvalue \
            from (%s) q2" % (cond,cond,cond,mlowlim,muplim,boundsQuery)
    
    query = "select * from (%s) query1 where (xvalue is not null or mvalue is not null)" % query

    if fromTime:
        query = query + " and timestamp > '%s'" % fromTime

    query = query + " and id >= 1 and id <= %s ;" % (str(maxnode))

    return GetDBDataFrame(query)

def GetSensorList():
    try:
        db, cur = SenslopeDBConnect(Namedb)
        cur.execute("use "+ Namedb)
        
        query = 'SELECT name, num_nodes, seg_length FROM site_column_props'
        
        df = psql.read_sql(query, db)

        df.to_csv("column_properties.csv",index=False,header=False);
        
        # make a sensor list of columnArray class functions
        sensors = []
        for s in range(len(df)):
            s = columnArray(df.name[s],df.num_nodes[s],df.seg_length[s])
            sensors.append(s)
        
        return sensors
    except:
        raise ValueError('Could not get sensor list from database')

def GetLastGoodData(sitename):
    try:
        db, cur = SenslopeDBConnect(NamedbPurged)
        cur.execute("use "+ NamedbPurged)

        tblname = "%s.%s" % (NamedbPurged, sitename)

        q1 = """select t.timestamp, t.id, t.xvalue, t.yvalue, t.zvalue from %s t
inner join( select max(timestamp) mt, id, xvalue, yvalue, zvalue from %s
where xvalue is not null group by id ) ss
on t.timestamp = ss.mt and t.id = ss.id""" % (tblname, tblname)

        q2 = """select tm.id, tm.mvalue from %s tm
inner join( select max(timestamp) mt, id, mvalue from %s
where mvalue is not null group by id ) ssm
on tm.timestamp = ssm.mt and tm.id = ssm.id""" % (tblname, tblname)

        query = """select t2.timestamp, t2.id, t2.xvalue, t2.yvalue, t2.zvalue, t3.mvalue
from (%s) t2 inner join (%s) t3 on t2.id = t3.id;""" % (q1, q2)
        
        df = psql.read_sql(query, db)
        df.columns = ['ts','id','x','y','z','m']

        return df
    except ValueError:
        raise ValueError('Could not get last good data')
            
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







