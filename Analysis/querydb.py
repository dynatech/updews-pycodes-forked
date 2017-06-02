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

#ExecuteQuery(query): executes a mysql like code "query" without expecting a return
#    Parameters:
#        query: str
#             mysql like query code
def ExecuteQuery(query):
    db, cur = SenslopeDBConnect(Namedb)
    cur.execute(query)
    db.commit()
    db.close()

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
    df.to_sql(name = table_name, con = engine, if_exists = 'append', schema = Namedb, index=index)

def GetRawAccelData(tsm_id='',tsm_name = "", fromTime = "", toTime = "", msgid = "", targetnode ="", batt=0, voltf=False, returndb=True):
    #query tsm_name if input tsm_id
    if tsm_id!='':
        query_tsm_name='select tsm_name from tsm_sensors where tsm_id=%d' %tsm_id
        t =  GetDBDataFrame(query_tsm_name)
        tsm_name=t.loc[0][0]
        
        tsm_query=' where tsm_sensors.tsm_id=%d' %tsm_id
    else:
        tsm_query=" where tsm_name='%s'" %tsm_name
    
    if not tsm_name and not tsm_id:
        raise ValueError('no site id entered')
        
    if printtostdout:
        PrintOut('Querying database ...')

    if (len(tsm_name) == 5):
        query ="""SELECT ts,'%s' as 'tsm_name',times.node_id,xval,yval,zval,batt,accel_id
                 from (select *, if(type_num in (32,11), 1,if(type_num in (33,12),2,0)) as 'accel_number' from tilt_%s""" %(tsm_name,tsm_name)
        if not fromTime:
            fromTime = "2010-01-01"
        query += " WHERE ts>='%s'" %fromTime
        
        if toTime != '':
            toTime = pd.to_datetime(toTime)+timedelta(hours=0.5)
            toTime_query =  " AND ts <= '%s'" %toTime
        else:
            toTime=pd.to_datetime(datetime.now())
            toTime_query = ''
            
        query += toTime_query  
        
        
        query=query+" ) times"
        
        targetnode_query=""" inner join (SELECT  accel_id,accelerometers.tsm_id,tsm_name,node_id,accel_number,voltage_min, voltage_max, in_use,version FROM senslopedb.accelerometers
                            inner join (select * from tsm_sensors"""
        if tsm_id!='':
            targetnode_query= targetnode_query + tsm_query
        else:
            targetnode_query= targetnode_query + tsm_query + " and (date_deactivated>='%s' or date_deactivated is NULL) limit 1"%toTime
        targetnode_query= targetnode_query + """ ) tsm 
                            on tsm.tsm_id = accelerometers.tsm_id) nodes"""
        if targetnode!='':
            targetnode_query=""" inner join (SELECT  accel_id,accelerometers.tsm_id,tsm_name,node_id,accel_number,voltage_min, voltage_max, in_use,version FROM senslopedb.accelerometers
                            inner join (select * from tsm_sensors"""
            if tsm_id!='':
                targetnode_query= targetnode_query + tsm_query
            else:
                targetnode_query= targetnode_query + tsm_query+" and (date_deactivated>='%s' or date_deactivated is NULL) limit 1"%toTime
                
            targetnode_query= targetnode_query + """) tsm 
                            on tsm.tsm_id = accelerometers.tsm_id where node_id=%d) nodes""" %(targetnode)
        query += targetnode_query
                
        query += " on times.node_id = nodes.node_id and times.accel_number=nodes.accel_number"
        if msgid in (11,12,32,33):
            query=query+" where type_num =%d " %msgid
            inUse_query=''
        else:
            query=query+" where if(nodes.version=2,type_num in (32,33),type_num in (11,12))"
            inUse_query=" and in_use=1"
        query= query + inUse_query

        if voltf:
            query=query + " and batt>voltage_min and batt<voltage_max"        


    elif (len(tsm_name) == 4):
        query ="""SELECT ts,'%s' as 'tsm_name',times.node_id,xval,yval,zval,accel_id
                 from (select * from tilt_%s""" %(tsm_name,tsm_name)
        if not fromTime:
            fromTime = "2010-01-01"
        query += " WHERE ts>='%s'" %fromTime
        
        if toTime != '':
            toTime = pd.to_datetime(toTime)+timedelta(hours=0.5)
            toTime_query =  " AND ts <= '%s'" %toTime
        else:
            toTime_query = ''
            
        query += toTime_query + ") times"
        
        targetnode_query=""" inner join (select accelerometers.node_id,accel_id from accelerometers
                            inner join tsm_sensors on tsm_sensors.tsm_id = accelerometers.tsm_id""" 
        targetnode_query= targetnode_query + tsm_query
        targetnode_query= targetnode_query + ") nodes" 
        if targetnode!='':
            targetnode_query=""" inner join (select accelerometers.node_id,accel_id from accelerometers
                                inner join tsm_sensors on tsm_sensors.tsm_id = accelerometers.tsm_id""" 
            targetnode_query= targetnode_query + tsm_query
            targetnode_query= targetnode_query + " and node_id=%d) nodes" %(targetnode)
        query += targetnode_query
        
        query += " on times.node_id = nodes.node_id"

    if returndb:
        df =  GetDBDataFrame(query)
        if (len(tsm_name) == 5):
            if (batt == 1):                
                df.columns = ['ts','tsm_name','node_id','x','y','z','batt','accel_id']
                df.ts = pd.to_datetime(df.ts)
                return df
            else:
                df = df.drop('batt',axis=1)
                
        df.columns = ['ts','tsm_name','node_id','x','y','z','accel_id']
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
        
        df = df.drop('mval1', axis=1, inplace=False)
        df = df.drop('mval2', axis=1, inplace=False)
        df['mval1'] = df['mval1-n']
        df = df.drop('mval1-n', axis=1, inplace=False)
    
    #df = df.replace("-inf", "NAN")         
    df = df.drop('mval2', axis=1, inplace=False)

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
def GetNodeStatus(tsm_id, status=4):   
    try:
        query = "SELECT DISTINCT node_id FROM ("
        query += " SELECT a.node_id FROM"
        query += " accelerometer_status as s"
        query += " left join accelerometers as a"
        query += " on s.accel_id = a.accel_id"
        query += " where tsm_id = %s" %tsm_id
        query += " and status = %s" %status
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
    lgdpm.columns = ['ts','node_id','x','y','z']        
    lgdpm['tsm_name'] = tsm_name

    return lgdpm

#create_tsm_alerts
#    creates table named 'tsm_alerts' which contains alerts for all tsm
def create_tsm_alerts():    
    query = "CREATE TABLE `tsm_alerts` ("
    query += "  `ta_id` INT(5) UNSIGNED NOT NULL AUTO_INCREMENT,"
    query += "  `ts` TIMESTAMP NULL,"
    query += "  `tsm_id` SMALLINT(5) UNSIGNED NOT NULL,"
    query += "  `alert_level` TINYINT(2) NOT NULL,"
    query += "  `ts_updated` TIMESTAMP NULL,"
    query += "  PRIMARY KEY (`ta_id`),"
    query += "  UNIQUE INDEX `uq_tsm_alerts` (`ts` ASC, `tsm_id` ASC),"
    query += "  INDEX `fk_tsm_alerts_tsm_sensors1_idx` (`tsm_id` ASC),"
    query += "  CONSTRAINT `fk_tsm_alerts_tsm_sensors1`"
    query += "    FOREIGN KEY (`tsm_id`)"
    query += "    REFERENCES `tsm_sensors` (`tsm_id`)"
    query += "    ON DELETE NO ACTION"
    query += "    ON UPDATE CASCADE)"
    
    ExecuteQuery(query)

#create_operational_triggers
#    creates table named 'operational_triggers' which contains alerts for all operational triggers
def create_operational_triggers():
    query = "CREATE TABLE `operational_triggers` ("
    query += "  `trigger_id` INT(5) UNSIGNED NOT NULL AUTO_INCREMENT,"
    query += "  `ts` TIMESTAMP NULL,"
    query += "  `site_id` TINYINT(3) UNSIGNED NOT NULL,"
    query += "  `trigger_sym_id` TINYINT(2) UNSIGNED NOT NULL,"
    query += "  `ts_updated` TIMESTAMP NULL,"
    query += "  PRIMARY KEY (`trigger_id`),"
    query += "  UNIQUE INDEX `uq_operational_triggers` (`ts` ASC, `site_id` ASC, `trigger_sym_id` ASC),"
    query += "  INDEX `fk_operational_triggers_sites1_idx` (`site_id` ASC),"
    query += "  CONSTRAINT `fk_operational_triggers_sites1`"
    query += "    FOREIGN KEY (`site_id`)"
    query += "    REFERENCES `sites` (`site_id`)"
    query += "    ON DELETE NO ACTION"
    query += "    ON UPDATE CASCADE,"
    query += "  INDEX `fk_operational_triggers_operational_trigger_symbols1_idx` (`trigger_sym_id` ASC),"
    query += "  CONSTRAINT `fk_operational_triggers_operational_trigger_symbols1`"
    query += "    FOREIGN KEY (`trigger_sym_id`)"
    query += "    REFERENCES `operational_trigger_symbols` (`trigger_sym_id`)"
    query += "    ON DELETE NO ACTION"
    query += "    ON UPDATE CASCADE)"
    
    ExecuteQuery(query)

#create_public_alerts
#    creates table named 'public_alerts' which contains alerts for all public alerts
def create_public_alerts():
    query = "CREATE TABLE `public_alerts` ("
    query += "  `public_id` INT(5) UNSIGNED NOT NULL AUTO_INCREMENT,"
    query += "  `ts` TIMESTAMP NULL,"
    query += "  `site_id` TINYINT(3) UNSIGNED NOT NULL,"
    query += "  `pub_sym_id` TINYINT(1) UNSIGNED NOT NULL,"
    query += "  `ts_updated` TIMESTAMP NULL,"
    query += "  PRIMARY KEY (`public_id`),"
    query += "  UNIQUE INDEX `uq_public_alerts` (`ts` ASC, `site_id` ASC, `pub_sym_id` ASC),"
    query += "  INDEX `fk_public_alerts_sites1_idx` (`site_id` ASC),"
    query += "  CONSTRAINT `fk_public_alerts_sites1`"
    query += "    FOREIGN KEY (`site_id`)"
    query += "    REFERENCES `sites` (`site_id`)"
    query += "    ON DELETE NO ACTION"
    query += "    ON UPDATE CASCADE,"
    query += "  INDEX `fk_public_alerts_public_alert_symbols1_idx` (`pub_sym_id` ASC),"
    query += "  CONSTRAINT `fk_public_alerts_public_alert_symbols1`"
    query += "    FOREIGN KEY (`pub_sym_id`)"
    query += "    REFERENCES `public_alert_symbols` (`pub_sym_id`)"
    query += "    ON DELETE NO ACTION"
    query += "    ON UPDATE CASCADE)"
    
    ExecuteQuery(query)

#create_internal_alerts
#    creates table named 'internal_alerts' which contains alerts for all internal alerts
def create_internal_alerts():
    query = "CREATE TABLE `internal_alerts` ("
    query += "  `internal_id` INT(5) UNSIGNED NOT NULL AUTO_INCREMENT,"
    query += "  `ts` TIMESTAMP NULL,"
    query += "  `site_id` TINYINT(3) UNSIGNED NOT NULL,"
    query += "  `internal_sym` VARCHAR(9) NULL,"
    query += "  `ts_updated` TIMESTAMP NULL,"
    query += "  PRIMARY KEY (`internal_id`),"
    query += "  UNIQUE INDEX `uq_internal_alerts` (`ts` ASC, `site_id` ASC),"
    query += "  INDEX `fk_internal_alerts_sites_idx` (`site_id` ASC),"
    query += "  CONSTRAINT `fk_internal_alerts_sites`"
    query += "    FOREIGN KEY (`site_id`)"
    query += "    REFERENCES `sites` (`site_id`)"
    query += "    ON DELETE NO ACTION"
    query += "    ON UPDATE CASCADE)"
    
    ExecuteQuery(query)

#alert_toDB
#    writes to alert tables
#    Inputs:
#        df- dataframe to be written in table_name
#        table_name- str; name of table in database ('tsm_alerts' or 'operational_triggers')
def alert_toDB(df, table_name):
    
    if DoesTableExist(table_name) == False:
        #Create a tsm_alerts table if it doesn't exist yet
        if table_name == 'tsm_alerts':
            create_tsm_alerts()
        #Create a public_alerts table if it doesn't exist yet
        elif table_name == 'public_alerts':
            create_public_alerts()
        #Create a internal_alerts table if it doesn't exist yet
        elif table_name == 'internal_alerts':
            create_internal_alerts()
        #Create a operational_triggers table if it doesn't exist yet
        elif table_name == 'operational_triggers':
            create_operational_triggers()
        else:
            print 'unrecognized table:', table_name
            return

    if table_name == 'operational_triggers':
        query = "SELECT * FROM operational_trigger_symbols"
        all_trig = GetDBDataFrame(query)
        trigger_source = all_trig[all_trig.trigger_sym_id == df['trigger_sym_id'].values[0]]['trigger_source'].values[0]
        trigger_sym_ids = ','.join(map(str, all_trig[all_trig.trigger_source == trigger_source]['trigger_sym_id'].values))

    query = "SELECT * FROM %s WHERE" %table_name
    
    if table_name == 'tsm_alerts':
        query += " tsm_id = '%s'" %df['tsm_id'].values[0]
    elif table_name == 'public_alerts':
        query += " site_id = '%s'" %(df['site_id'].values[0])
    elif table_name == 'internal_alerts':
        query += " site_id = '%s'" %(df['site_id'].values[0])
    else:
        query += " site_id = '%s' and trigger_sym_id in (%s)" %(df['site_id'].values[0], trigger_sym_ids)

    query += " and ts <= '%s' and ts_updated >= '%s' ORDER BY ts DESC LIMIT 1" %(df['ts_updated'].values[0], pd.to_datetime(df['ts_updated'].values[0])-timedelta(hours=0.5))

    df2 = GetDBDataFrame(query)

    if table_name == 'tsm_alerts':
        try:
            same_alert = df2['alert_level'].values[0] == df['alert_level'].values[0]
        except:
            same_alert = False
        query = "SELECT EXISTS(SELECT * FROM tsm_alerts"
        query += " WHERE ts = '%s' AND tsm_id = %s)" %(df['ts_updated'].values[0], df['tsm_id'].values[0])
        if GetDBDataFrame(query).values[0][0] == 1:
            inDB = True
        else:
            inDB = False

    elif table_name == 'public_alerts':
        try:
            same_alert = df2['pub_sym_id'].values[0] == df['pub_sym_id'].values[0]
        except:
            same_alert = False
        query = "SELECT EXISTS(SELECT * FROM public_alerts"
        query += " WHERE ts = '%s' AND site_id = %s" %(df['ts_updated'].values[0], df['site_id'].values[0])
        query += " AND pub_sym_id)" %df['pub_sym_id'].values[0]
        if GetDBDataFrame(query).values[0][0] == 1:
            inDB = True
        else:
            inDB = False

    elif table_name == 'internal_alerts':
        try:
            same_alert = df2['internal_sym'].values[0] == df['internal_sym'].values[0]
        except:
            same_alert = False
        query = "SELECT EXISTS(SELECT * FROM internal_alerts"
        query += " WHERE ts = '%s' AND site_id = %s" %(df['ts_updated'].values[0], df['site_id'].values[0])
        query += " AND internal_sym = '%s')" %df['internal_sym'].values[0]
        if GetDBDataFrame(query).values[0][0] == 1:
            inDB = True
        else:
            inDB = False

    else:
        try:
            same_alert = df2['trigger_sym_id'].values[0] == df['trigger_sym_id'].values[0]
        except:
            same_alert = False
        query = "SELECT EXISTS(SELECT * FROM operational_triggers"
        query += " WHERE ts = '%s' AND site_id = %s" %(df['ts_updated'].values[0], df['site_id'].values[0])
        query += " AND trigger_sym_id)" %df['trigger_sym_id'].values[0]
        if GetDBDataFrame(query).values[0][0] == 1:
            inDB = True
        else:
            inDB = False

    if (len(df2) == 0 or not same_alert) and not inDB:
        PushDBDataFrame(df, table_name, index=False)
        
    elif same_alert and df2['ts_updated'].values[0] < df['ts_updated'].values[0]:
        query = "UPDATE %s SET ts_updated = '%s' WHERE" %(table_name, df['ts_updated'].values[0])
        if table_name == 'tsm_alerts':
            query += " ta_id = %s" %df2['ta_id'].values[0]
        elif table_name == 'public_alerts':
            query += " public_id = %s" %df2['public_id'].values[0]
        elif table_name == 'internal_alerts':
            query += " internal_id = %s" %df2['internal_id'].values[0]
        else:
            query += " trigger_id = %s" %df2['trigger_id'].values[0]
        ExecuteQuery(query)

            
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