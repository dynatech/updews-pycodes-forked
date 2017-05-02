from datetime import datetime, timedelta
import pandas.io.sql as psql
import pandas as pd
import platform
from sqlalchemy import create_engine

curOS = platform.system()

if curOS == "Windows":
    import MySQLdb as mysqlDriver
elif curOS == "Linux":
    import pymysql as mysqlDriver

import configfileio as cfg

# Scripts for connecting to local database

class loggerArray:
    def __init__(self, site_id, tsm_id, tsm_name, number_of_segments, segment_length):
        self.site_id = site_id
        self.tsm_id = tsm_id
        self.tsm_name = tsm_name
        self.nos = number_of_segments
        self.seglen = segment_length
        
class coordsArray:
    def __init__(self, name, lat, lon, barangay):
        self.name = name
        self.lat = lat
        self.lon = lon
        self.bgy = barangay


def SenslopeDBConnect(nameDB):
    while True:
        try:
            db = mysqlDriver.connect(host = Hostdb, user = Userdb, passwd = Passdb, db=nameDB)
            cur = db.cursor()
            return db, cur
        except mysqlDriver.OperationalError:
            print '.',

def PrintOut(line):
    if printtostdout:
        print line

#Check if table exists
#   Returns true if table exists
def DoesTableExist(table_name):
    db, cur = SenslopeDBConnect(Namedb)
    cur.execute("use "+ Namedb)
    cur.execute("SHOW TABLES LIKE '%s'" %table_name)

    if cur.rowcount > 0:
        db.close()
        return True
    else:
        db.close()
        return False
        
#GetDBDataFrame(query): queries a specific data table and returns it as
#    a python dataframe format
#    Parameters:
#        query: str
#            mysql like query code
#    Returns:
#        df: dataframe object
#            dataframe object of the result set
def GetDBDataFrame(query):
    try:
        db, cur = SenslopeDBConnect(Namedb)
        df = psql.read_sql(query, db)
        db.close()
        return df
    except KeyboardInterrupt:
        PrintOut("Exception detected in accessing database")
        
#Push a dataframe object into a table
def PushDBDataFrame(df,table_name,index=True):
    engine = create_engine('mysql://'+Userdb+':'+Passdb+'@'+Hostdb+':3306/'+Namedb)
    try:
        df.to_sql(name = table_name, con = engine, if_exists = 'append', schema = Namedb, index=index)
    except:
        print 'already in db'


def GetRawAccelData(tsm_name = "", fromTime = "", toTime = "", msgid = "", targetnode ="", batt=0, voltf=False, returndb=True):
    if not tsm_name:
        raise ValueError('no site id entered')
        
    if printtostdout:
        PrintOut('Querying database ...')

    if (len(tsm_name) == 5):
        query ="""Select ts,'%s' as 'name',node_id,xval,yval,zval,batt from 
                (SELECT ts,'%s' as 'name',times.node_id,xval,yval,zval,batt,type_num, accel_number, voltage_max, voltage_min
                 from (select * from tilt_%s""" %(tsm_name,tsm_name,tsm_name)
        if not fromTime:
            fromTime = "2010-01-01"
        query += " WHERE ts>='%s'" %fromTime
        
        if toTime != '':
            toTime = pd.to_datetime(toTime)+timedelta(hours=0.5)
            toTime_query =  " AND ts <= '%s'" %toTime
        else:
            toTime_query = ''
            
        query += toTime_query + ") times"
        
        targetnode_query=""" inner join (select accelerometers.node_id, voltage_min, voltage_max, accel_number from accelerometers
                            inner join tsm_sensors on tsm_sensors.tsm_id = accelerometers.tsm_id 
                            where tsm_name = '%s') nodes""" %tsm_name
        if targetnode!='':
            targetnode_query=""" inner join (select accelerometers.node_id, voltage_min, voltage_max, accel_number from accelerometers
                                inner join tsm_sensors on tsm_sensors.tsm_id = accelerometers.tsm_id 
                                where tsm_name = '%s' and node_id=%d) nodes""" %(tsm_name,targetnode)
        query += targetnode_query
                
        query += " on times.node_id = nodes.node_id) raw"

        volt_query=''
        if voltf:
            volt_query=" and batt>=(raw.voltage_min) and batt<=(raw.voltage_max)"        

        if msgid in (11,12,32,33):
            query =query+ " where type_num=%d" %msgid + volt_query
        else:
            query += " where (raw.accel_number = 1 and type_num in (11,32)" + volt_query+")"
            query += " or (raw.accel_number = 2 and type_num in (12,33) " + volt_query +")"


    elif (len(tsm_name) == 4):
        query ="""Select ts,'%s' as 'name',node_id,xval,yval,zval from 
                (SELECT ts,'%s' as 'name',times.node_id,xval,yval,zval
                 from (select * from tilt_%s""" %(tsm_name,tsm_name,tsm_name)
        if not fromTime:
            fromTime = "2010-01-01"
        query += " WHERE ts>='%s'" %fromTime
        
        if toTime != '':
            toTime = pd.to_datetime(toTime)+timedelta(hours=0.5)
            toTime_query =  " AND ts <= '%s'" %toTime
        else:
            toTime_query = ''
            
        query += toTime_query + ") times"
        
        targetnode_query=""" inner join (select accelerometers.node_id from accelerometers
                            inner join tsm_sensors on tsm_sensors.tsm_id = accelerometers.tsm_id 
                            where tsm_name = '%s') nodes""" %tsm_name
        if targetnode!='':
            targetnode_query=""" inner join (select accelerometers.node_id from accelerometers
                                inner join tsm_sensors on tsm_sensors.tsm_id = accelerometers.tsm_id 
                                where tsm_name = '%s' and node_id=%d) nodes""" %(tsm_name,targetnode)
        query += targetnode_query
        
        query += " on times.node_id = nodes.node_id) raw"

    if returndb:
        if (len(tsm_name) == 5):
            df =  GetDBDataFrame(query)

            if (batt == 1):                
                df.columns = ['ts','name','id','x','y','z','batt']
                df.ts = pd.to_datetime(df.ts)
                return df
            else:
                df = df.drop('batt',axis=1)
                
        else:
            df =  GetDBDataFrame(query)
        
        df.columns = ['ts','name','id','x','y','z']
        df.ts = pd.to_datetime(df.ts)
        return df
        
    else:
        return query

def GetSOMSRaw(siteid = "", fromTime = "", toTime = "", msgid="", targetnode = ""):

    if not siteid:
        raise ValueError('invalid siteid')
    
    query_accel = "SELECT version FROM senslopedb.tsm_sensors where tsm_name = '%s'" %siteid  
    df_accel =  GetDBDataFrame(query_accel) 
    query = "select * from senslopedb.soms_%s" %siteid
    
    if not fromTime:
        fromTime = "2010-01-01"
    
        
    query += " where ts > '%s'" %fromTime
    
    if toTime:
        query += " and ts < '%s'" %toTime
    
    
    if targetnode:
        query += " and node_id = '%s'" %targetnode
    
    if msgid:
        query += " and msid = '%s'" %msgid
        
    df =  GetDBDataFrame(query)
    
    
    df.ts = pd.to_datetime(df.ts)
    
    if (df_accel.version[0] == 2):
        if (siteid== 'nagsa'):
            df['mval1-n'] =(((8000000/(df.mval1))-(8000000/(df.mval2)))*4)/10
        else:
            df['mval1-n'] =(((20000000/(df.mval1))-(20000000/(df.mval2)))*4)/10      
    
    #df = df.replace("-inf", "NAN")         
        

    return df
    
    

def GetCoordsList():
    try:
        db, cur = SenslopeDBConnect(Namedb)
        cur.execute("use "+ Namedb)
        
        query = 'SELECT name, lat, lon, barangay FROM site_column'
        
        df = psql.read_sql(query, db)
        
        # make a sensor list of columnArray class functions
        sensors = []
        for s in range(len(df)):
            s = coordsArray(df.name[s],df.lat[s],df.lon[s],df.barangay[s])
            sensors.append(s)
            
        return sensors
    except:
        raise ValueError('Could not get sensor list from database')

#loggerArrayList():
#    transforms dataframe TSMdf to list of loggerArray
def loggerArrayList(TSMdf):
    return loggerArray(TSMdf['site_id'].values[0], TSMdf['tsm_id'].values[0], TSMdf['tsm_name'].values[0], TSMdf['number_of_segments'].values[0], TSMdf['segment_length'].values[0])

#GetTSMList():
#    returns a list of loggerArray objects from the database tables
def GetTSMList(tsm_name='', end=datetime.now()):
    if tsm_name == '':
        try:
            query = "SELECT site_id, logger_id, tsm_id, tsm_name, number_of_segments, segment_length, date_activated"
            query += " FROM senslopedb.tsm_sensors WHERE (date_deactivated > '%s' OR date_deactivated IS NULL)" %end
            df = GetDBDataFrame(query)
            df = df.sort_values(['logger_id', 'date_activated'], ascending=[True, False])
            df = df.drop_duplicates('logger_id')
            
            # make a sensor list of loggerArray class functions
            TSMdf = df.groupby('logger_id', as_index=False)
            sensors = TSMdf.apply(loggerArrayList)
            return sensors
        except:
            raise ValueError('Could not get sensor list from database')
    else:
        try:
            query = "SELECT site_id, logger_id, tsm_id, tsm_name, number_of_segments, segment_length, date_activated"
            query += " FROM senslopedb.tsm_sensors WHERE (date_deactivated > '%s' OR date_deactivated IS NULL)" %end
            query += " AND tsm_name = '%s'" %tsm_name
            df = GetDBDataFrame(query)
            df = df.sort_values(['logger_id', 'date_activated'], ascending=[True, False])
            df = df.drop_duplicates('logger_id')
            
            # make a sensor list of loggerArray class functions
            TSMdf = df.groupby('logger_id', as_index=False)
            sensors = TSMdf.apply(loggerArrayList)
            return sensors
        except:
            raise ValueError('Could not get sensor list from database')

#returns list of non-working nodes from the node status table
#function will only return the latest entry per site per node with
#"Not OK" status
def GetNodeStatus(tsm_id, status=1):
    if status == 1:
        status = "Not OK"
    elif status == 2:
        status = "Use with Caution"
    elif status == 3:
        status = "Special Case"
    
    try:
        query = "SELECT DISTINCT node_id FROM ("
        query += " SELECT a.node_id FROM"
        query += " accelerometer_status as s"
        query += " left join accelerometers as a"
        query += " on s.accel_id = a.accel_id"
        query += " where tsm_id = %s" %tsm_id
        query += " and status = '%s'" %status
        query += " ) AS sub"
        df = GetDBDataFrame(query)
        return df['node_id'].values
    except:
        raise ValueError('Could not get node status from database')
    
#GetSingleLGDPM
#   This function returns the last good data prior to the monitoring window
#   Inputs:
#       site (e.g. sinb, mamb, agbsb)
#       node (e.g. 1,2...15...30)
#       startTS (e.g. 2016-04-25 15:00:00, 2016-02-01 05:00:00, 
#                YYYY-MM-DD HH:mm:SS)
#   Output:
#       returns the dataframe for the last good data prior to the monitoring window
    
def GetSingleLGDPM(tsm_name, node_id, startTS):
    query = "SELECT ts,node_id, xval, yval, zval "
    query += "FROM %s WHERE node_id IN (%s) AND ts < '%s' AND ts >= '%s' " %('tilt_'+tsm_name, ','.join(map(str, node_id)), startTS, startTS-timedelta(3))
    if len(tsm_name) == 5:
        query += "and (type_num = 32 or type_num = 11) "        
    query += "ORDER BY ts DESC"
    
    lgdpm = GetDBDataFrame(query)   
    lgdpm.columns = ['ts','id','x','y','z']        
    lgdpm['name'] = tsm_name

    return lgdpm

            
s = cfg.config()

Hostdb = s.dbio.hostdb
Userdb = s.dbio.userdb
Passdb = s.dbio.passdb
Namedb = s.dbio.namedb
printtostdout = s.dbio.printtostdout

xlim = s.value.xlim
ylim = s.value.ylim
zlim = s.value.zlim
xmax = s.value.xmax
mlowlim = s.value.mlowlim
muplim = s.value.muplim
islimval = s.value.limitvalues