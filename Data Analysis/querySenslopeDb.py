import MySQLdb
import ConfigParser
from datetime import datetime as dtm
from datetime import timedelta as tda
import re
import pandas.io.sql as psql
import pandas as pd
import numpy as np
import StringIO
import filterSensorData

# Scripts for connecting to local database
# Needs config file: server-config.txt

class columnArray:
    def __init__(self, name, number_of_segments, segment_length, col_length):
        self.name = name
        self.nos = number_of_segments
        self.seglen = segment_length
        self.collength = col_length

class rainArray:
    def __init__(self, name, max_rain_2year, rain_senslope, rain_arq):
        self.name = name
        self.twoyrmx = max_rain_2year
        self.oldsite = rain_senslope
        self.newsite = rain_arq


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
	
#GetDBResultset(query): executes a mysql like code "query"
#    Parameters:
#        query: str
#             mysql like query code
#    Returns:
#        resultset: str
#             result value of the query made
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
        
#GetDBDataFrame(query): queries a specific sensor data table and returns it as
#    a python dataframe format
#    Parameters:
#        query: str
#            mysql like query code
#    Returns:
#        df: dataframe object
#            dataframe object of the result set
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
        

#GetRawAccelData(siteid = "", fromTime = "", maxnode = 40): 
#    retrieves raw data from the database table specified by parameters
#    
#    Parameters:
#        siteid: str
#            sitename or column name of the sensor column
#        fromTime: str 
#            starting time of the query that needs to be retrieved
#        maxnode: int, default 40
#            maximum node expected from this particular sensor column. Used
#            to remove extraneous node ids which may not belong to the sensor column
#            
#    Returns:
#        df: dataframe object 
#            dataframe object of the result set 
def GetRawAccelData(siteid = "", fromTime = "", toTime = "", maxnode = 40, msgid = 32, targetnode = -1):

    if not siteid:
        raise ValueError('no site id entered')
    
    if printtostdout:
        PrintOut('Querying database ...')

    query = "select timestamp,id,xvalue,yvalue,zvalue from senslopedb.%s " % (siteid)        

    if not fromTime:
        fromTime = "2010-01-01"
        
    query = query + " where timestamp > '%s'" % fromTime
    
    if toTime:
        query = query + " and timestamp < '%s'" % toTime

    if len(siteid) == 5:
        query = query + " and msgid = %s" % (msgid);
    
    if targetnode <= 0:
        query = query + " and id >= 1 and id <= %s ;" % (str(maxnode))
    else:
        query = query + " and id = %s;" % (targetnode)
    
    PrintOut(query)
    df =  GetDBDataFrame(query)
    
    df.columns = ['ts','id','x','y','z']
    # change ts column to datetime
    df.ts = pd.to_datetime(df.ts)
    
    return df

#GetRawRainData(siteid = "", fromTime = "", maxnode = 40): 
#    retrieves raw data from the database table specified by parameters
#    
#    Parameters:
#        siteid: str
#            sitename or column name of the sensor column
#        fromTime: str 
#            starting time of the query that needs to be retrieved
#            
#    Returns:
#        df: dataframe object 
#            dataframe object of the result set 
def GetRawRainData(siteid = "", fromTime = ""):

    if not siteid:
        raise ValueError('no site id entered')
    
    if printtostdout:
        PrintOut('Querying database ...')
    
    oldsite = []
    newsite = []
    rainlist = GetRainList()
    for s in rainlist:
        oldsite += [s.oldsite]
        newsite += [s.newsite]
    
    try:    
        if siteid in oldsite:
        
            query = "select timestamp, rain from senslopedb.%s " % (siteid)
            
        elif siteid in newsite:
        
            query = "select timestamp, r15m from senslopedb.%s " % (siteid)
        
        if not fromTime:
            fromTime = "2010-01-01"
            
        query = query + " where timestamp > '%s'" % fromTime
    
        df =  GetDBDataFrame(query)
        
        df.columns = ['ts','rain']
        # change ts column to datetime
        df.ts = pd.to_datetime(df.ts)
        
        return df
        
    except UnboundLocalError:
        print 'No ' + siteid + ' table in SQL'



#GetSensorList():
#    returns a list of columnArray objects from the database tables
#    
#    Returns:
#        sensorlist: list
#            list of columnArray (see class definition above)
def GetSensorList():
    try:
        db, cur = SenslopeDBConnect(Namedb)
        cur.execute("use "+ Namedb)
        
        query = 'SELECT name, num_nodes, seg_length, col_length FROM site_column_props'
        
        df = psql.read_sql(query, db)
        df.to_csv("column_properties.csv",index=False,header=False);
        
        # make a sensor list of columnArray class functions
        sensors = []
        for s in range(len(df)):
            if df.name[s] == 'mcatb' or df.name[s] == 'messb':
                continue
            s = columnArray(df.name[s],df.num_nodes[s],df.seg_length[s],df.col_length[s])
            sensors.append(s)
        return sensors
    except:
        raise ValueError('Could not get sensor list from database')

def GetSensorDF():
    try:
        #db, cur = SenslopeDBConnect(Namedb)
        #cur.execute("use "+ Namedb)
        
        query = 'SELECT name, num_nodes, seg_length, col_length FROM site_column_props'
        
        #df = psql.read_sql(query, db)
        
        df = GetDBDataFrame(query)
        return df
    except:
        raise ValueError('Could not get sensor list from database')

#GetRainList():
#    returns a list of columnArray objects from the database tables
#    
#    Returns:
#        sensorlist: list
#            list of columnArray (see class definition above)
def GetRainList():
    try:
        db, cur = SenslopeDBConnect(Namedb)
        cur.execute("use "+ Namedb)
        
        query = 'SELECT name, max_rain_2year, rain_senslope, rain_arq FROM site_rain_props'
        
        df = psql.read_sql(query, db)
        
        # make a sensor list of columnArray class functions
        sensors = []
        for s in range(len(df)):
            s = rainArray(df.name[s],df.max_rain_2year[s],df.rain_senslope[s],df.rain_arq[s])
            sensors.append(s)
            
        return sensors
    except:
        raise ValueError('Could not get sensor list from database')

        
#GetLastGoodData(df, nos, fillMissing=False):
#    evaluates the last good data from the input df
#    
#    Parameters:
#        df: dataframe object
#            input dataframe object where the last good data is to be evaluated
#        nos: int
#            number of segments of a sensor column
#        fillMissing: boolean, default False
#            True: fills in the missing sensor node data that is not present from
#                the evaluated dataframe input df based on the nos value. The filled
#                data is data for a perfect vertical line
#            False: evaluated dataframe is returned without filled nodes
#        
#    Returns:
#        dflgd: dataframe object
#            dataframe object of the resulting last good data
def GetLastGoodData(df, nos, fillMissing=False):
    # groupby id first
    dfa = df.groupby('id')
    # extract the latest timestamp per id, drop the index
    dfa =  dfa.apply(lambda x: x[x.ts==x.ts.max()]).reset_index(level=1,drop=True)

    if fillMissing:
        # below are routines to handle nodes that have no data whatsoever
        # create a list of missing nodes       
        missing = [i for i in range(1,nos+1) if i not in dfa.id.unique()]
    
        # create a dataframe with default values
        x = np.array([[dfa.ts.min(),1,1023,0,0,]])   
        x = np.repeat(x,len(missing),axis=0)
        dfd = pd.DataFrame(x, columns=['ts','id','x','y','z'])
        # change their ids to the missing ids
        dfd.id = pd.Series(missing)
        # append to the lgd datframe
        dflgd = dfa.append(dfd).sort(['id']).reset_index(level=1,drop=True)
    else:
        dflgd = dfa.sort(['id']).reset_index(level=1,drop=True)
        
    print dflgd
    
    return dflgd
    
#GetLastGoodDataFromDb(col):
#    queries the database table of the previously generated last good data
#    
#    Parameters:
#        col: str
#            sensor column name
#        ext: str
#            more query options
#            
#    Returns:
#        df: dataframe object
#            dataframe object of the resultset
def GetLastGoodDataFromDb(col):
    df = GetDBDataFrame("""SELECT timestamp, id, xvalue, yvalue, zvalue FROM
                        senslopedb.lastgooddata l where name='%s';""" % (col))
    df.columns = ['ts','id','x','y','z']
    # change ts column to datetime
    df.ts = pd.to_datetime(df.ts)
    
    return df
    
#PushLastGoodData(df,name):
#    writes a dataframe of the last good data to the database table lastgooddata
#    
#    Parameters:
#        df: dataframe object
#            dataframe object of the last good data to be written
#        name: str
#            sensor column name
def PushLastGoodData(df,name):
    db, cur = SenslopeDBConnect(Namedb)
    
    df['name'] = [name]*len(df)
    df = df[['name','id','ts','x','y','z']]
        
    q = StringIO.StringIO()
    df.to_csv(q,header=False, index=False,sep=',',line_terminator='),(')
    query = '(' + q.getvalue()
    query = query[:-2]    
    query = re.sub(r"[a-z]{4,5}",lambda x: '"' + x.group(0) + '"',query) 
    query = re.sub(r"[0-9\-\s:]{19}",lambda x: '"' + x.group(0) + '"',query)
    
    query = """INSERT INTO %s.lastgooddata (name,id,timestamp,xvalue,yvalue,zvalue) 
                VALUES %s ON DUPLICATE KEY UPDATE timestamp=values(timestamp),  
                xvalue=values(xvalue), yvalue=values(yvalue), zvalue=values(zvalue)""" %(Namedb,query)    
    
    cur.execute(query)
    db.commit()
    db.close()
    
#GenerateLastGoodData():
#    cycles through the whole list of sensor columns and writes the evaluated 
#    last good data set to the database    
def GenerateLastGoodData():
    
    db = MySQLdb.connect(host = Hostdb, user = Userdb, passwd = Passdb)
    cur = db.cursor()
    #cur.execute("CREATE DATABASE IF NOT EXISTS %s" %nameDB)
    
    query = """    DROP TABLE IF EXISTS `senslopedb`.`lastgooddata`;
        CREATE TABLE  `senslopedb`.`lastgooddata` (
          `name` varchar(8) NOT NULL DEFAULT '',
          `id` int(11) NOT NULL DEFAULT '0',
          `timestamp` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
          `xvalue` int(11) DEFAULT NULL,
          `yvalue` int(11) DEFAULT NULL,
          `zvalue` int(11) DEFAULT NULL,
          PRIMARY KEY (`name`,`id`)
          ); """
    
    cur.execute(query)
    db.close()
    
    slist = GetSensorList()
    
    for s in slist:
        print s.name, s.nos
        
        df = GetRawAccelData(s.name,'',s.nos)
        df = filterSensorData.applyFilters(df,True,True,False)         
        
        dflgd = GetLastGoodData(df,s.nos,True)
        del df           
          
        PushLastGoodData(dflgd,s.name)
   

            
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






