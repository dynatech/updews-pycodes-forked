import MySQLdb
import ConfigParser
from datetime import datetime as dtm
from datetime import timedelta as tda
import re
import pandas.io.sql as psql
import pandas as pd
import numpy as np

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
        # df.columns = ['ts','id','x','y','z','m']
        # change ts column to datetime
        # df.ts = pd.to_datetime(df.ts)

        db.close()
        return df
    except KeyboardInterrupt:
        PrintOut("Exception detected in accessing database")

def GetRawAccelData(siteid = "", fromTime = "", maxnode = 40):

    if not siteid:
        raise ValueError('no site id entered')
    
    if printtostdout:
        PrintOut('Querying database ...')

    # adjust out of bounds limits for xvalue mainly
    boundsQuery = "select timestamp, id, \
                  if((xvalue<(1126-4096))&(xvalue>(1024-4096)),xvalue+4096,xvalue) xvalue, \
                  if((yvalue<(1126-4096))&(yvalue>(1024-4096)),yvalue+4096,yvalue) yvalue, \
                  if((zvalue<(1126-4096))&(zvalue>(1024-4096)),zvalue+4096,zvalue) zvalue \
                  from senslopedb.%s" % (siteid)

    # nullify all other values once an invalid value is detected
    cond = "(xvalue < %s or abs(yvalue) > %s or abs(zvalue) > %s)" % (xlim,ylim,zlim)
    query = "select timestamp, id, \
            if(%s, null, xvalue) xvalue, \
            if(%s, null, yvalue) yvalue, \
            if(%s, null, zvalue) zvalue \
            from (%s) q2" % (cond,cond,cond,boundsQuery)
    
    query = "select * from (%s) query1 where (xvalue is not null)" % query

    if not fromTime:
        fromTime = "2010-01-01"
        
    query = query + " and timestamp > '%s'" % fromTime

    query = query + " and id >= 1 and id <= %s ;" % (str(maxnode))

    df =  GetDBDataFrame(query)
    
    df.columns = ['ts','id','x','y','z']
    # change ts column to datetime
    df.ts = pd.to_datetime(df.ts)
    
    return df

def GetSensorList():
    try:
        db, cur = SenslopeDBConnect(Namedb)
        cur.execute("use "+ Namedb)
        
        query = 'SELECT name, num_nodes, seg_length FROM site_column_props inner join site_column on site_column_props.s_id=site_column.s_id order by name asc'
        
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

def GetLastGoodData(df, colLength):
    # groupby id first
    dfa = df.groupby('id')
    # extract the latest timestamp per id, drop the index
    dfa =  dfa.apply(lambda x: x[x.ts==x.ts.max()]).reset_index(level=1,drop=True)

    # below are routines to handle nodes that have no data whatsoever
    # create a list of missing nodes       
    missing = [i for i in range(1,colLength+1) if i not in dfa.id.unique()]

    # create a dataframe with default values
    x = np.array([[dfa.ts.min(),1,1023,0,0,]])   
    x = np.repeat(x,len(missing),axis=0)
    dfd = pd.DataFrame(x, columns=['ts','id','x','y','z'])
    # change their ids to the missing ids
    dfd.id = pd.Series(missing)
    # append to the lgd datframe
    dflgd = dfa.append(dfd).sort(['id']).reset_index(level=1,drop=True)
    print dflgd
    
    return dflgd

            
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







