from datetime import datetime, timedelta
import memcache
import pandas.io.sql as psql
import pandas as pd
import platform
from sqlalchemy import create_engine

curOS = platform.system()

if curOS == "Windows":
    import MySQLdb as mysqlDriver
elif curOS == "Linux":
    import pymysql as mysqlDriver

# Scripts for connecting to local database

class LoggerArray:
    def __init__(self, site_id, tsm_id, tsm_name, number_of_segments, segment_length):
        self.site_id = site_id
        self.tsm_id = tsm_id
        self.tsm_name = tsm_name
        self.nos = number_of_segments
        self.seglen = segment_length
        
class CoordsArray:
    def __init__(self, name, lat, lon, barangay):
        self.name = name
        self.lat = lat
        self.lon = lon
        self.bgy = barangay


def senslopedb_connect(hostdb='local'):
    sc = memcached()
    Hostdb = sc['hosts'][hostdb]
    Userdb = sc['db']['user']
    Passdb = sc['db']['password']
    Namedb = sc['db']['name']
    while True:
        try:
            db = mysqlDriver.connect(host = Hostdb, user = Userdb, passwd = Passdb, db=Namedb)
            cur = db.cursor()
            cur.execute("use "+ Namedb)
            return db, cur
        except mysqlDriver.OperationalError:
            print_out('.')

def print_out(line):
    sc = memcached()
    if sc['print']['print_stdout']:
        print line

#Check if table exists
#   Returns true if table exists
def does_table_exist(table_name, hostdb='local'):
    db, cur = senslopedb_connect(hostdb)
    cur.execute("SHOW TABLES LIKE '%s'" %table_name)

    if cur.rowcount > 0:
        db.close()
        return True
    else:
        db.close()
        return False

#execute_query(query): executes a mysql like code "query" without expecting a return
#    Parameters:
#        query: str
#             mysql like query code
def execute_query(query, hostdb='local'):
    db, cur = senslopedb_connect(hostdb)
    cur.execute(query)
    db.commit()
    db.close()

#get_db_dataframe(query): queries a specific data table and returns it as
#    a python dataframe format
#    Parameters:
#        query: str
#            mysql like query code
#    Returns:
#        df: dataframe object
#            dataframe object of the result set
def get_db_dataframe(query, hostdb='local'):
    try:
        db, cur = senslopedb_connect(hostdb)
        df = psql.read_sql(query, db)
        db.close()
        return df
    except KeyboardInterrupt:
        print_out("Exception detected in accessing database")
        
#Push a dataframe object into a table
def push_db_dataframe(df,table_name,index=True, hostdb='local'):
    sc = memcached()
    Hostdb = sc['hosts'][hostdb]
    Userdb = sc['db']['user']
    Passdb = sc['db']['password']
    Namedb = sc['db']['name']
    engine = create_engine('mysql://'+Userdb+':'+Passdb+'@'+Hostdb+':3306/'+Namedb)
    df.to_sql(name = table_name, con = engine, if_exists = 'append', schema = Namedb, index=index)

#update memcache if ever there is changes 
#in accelerometers and tsm_sensors tables in senslopedb
def update_memcache():
    #memcached
    memc = memcache.Client(['127.0.0.1:11211'], debug=1)
    
    query_tsm=("SELECT tsm_id, tsm_name, date_deactivated,"
               " number_of_segments, version"
               " FROM senslopedb.tsm_sensors")
    query_accel=("SELECT accel_id, voltage_min, voltage_max"
                 " FROM senslopedb.accelerometers")
    
    memc.set('tsm', get_db_dataframe(query_tsm))
    memc.set('accel', get_db_dataframe(query_accel))
    
    print_out("Updated memcached with MySQL data")

#Get raw accel data
#    if batt is True, it will return batt voltage of each accel
#    if analysis is True, it will return the accel in use 
#       and it will drop columns 'in_use' and 'accel_number'
#    if voltf is True, it will apply voltage filter
#    if return_db is True, it will return dataframe, else it will return query
def get_raw_accel_data(tsm_id='',tsm_name = "", from_time = "", to_time = "", 
                       accel_number = "", node_id ="", batt=False, 
                       analysis=False, voltf=False, return_db=True):
    #memcached
    memc = memcache.Client(['127.0.0.1:11211'], debug=1)
    
    try:
        if not memc.get('tsm') and not memc.get('accel'):
            update_memcache()
            
    except ValueError:
        print_out("Data already saved in memcached")
        pass
    
    tsm_details = memc.get('tsm')
    accelerometers = memc.get('accel')
        
    tsm_details.date_deactivated=pd.to_datetime(tsm_details.date_deactivated)
    
    #range time
    if from_time == "":
        from_time = "2010-01-01"
    if to_time == "":
        to_time = pd.to_datetime(datetime.now())
    
    if not tsm_name and not tsm_id:
        raise ValueError('no tsm_sensor entered')
        
    #get tsm_name if input tsm_id
    if tsm_id != '':
        tsm_name = tsm_details.tsm_name[tsm_details.tsm_id==tsm_id].iloc[0]
    
    #get tsm_id if input is tsm_name and not tsm_id
    else:
        #if tsm_name has more than 1 tsm_id, it will return tsm_name 
        #where the date_deactivation is NULL or greater than or equal to_time 
        if tsm_details.tsm_id[tsm_details.tsm_name==tsm_name].count()>1:
            
            tsm_id = (tsm_details.tsm_id[(tsm_details.tsm_name==tsm_name) & 
                                         ((tsm_details.date_deactivated>=to_time) 
                                         | (tsm_details.date_deactivated.isnull()))
                                        ].iloc[0])
        else:
            tsm_id = tsm_details.tsm_id[tsm_details.tsm_name==tsm_name].iloc[0]
                   
    #query
    print_out('Querying database ...')

    query = ("SELECT ts,'%s' as 'tsm_name',times.node_id,xval,yval,zval,batt,"
             " times.accel_number,accel_id, in_use from (select *, if(type_num"
             " in (32,11) or type_num is NULL, 1,if(type_num in (33,12),2,0)) "
             " as 'accel_number' from tilt_%s" %(tsm_name,tsm_name))

    query += " WHERE ts >= '%s'" %from_time
    query += " AND ts <= '%s'" %to_time
    
    if node_id != '':
        #check node_id
        if ((node_id>tsm_details.number_of_segments
             [tsm_details.tsm_id==tsm_id].iloc[0]) or (node_id<1)):
            raise ValueError('Error node_id')
        else:
            query += ' AND node_id = %d' %node_id
        
    query += " ) times"
    
    node_id_query = " inner join (SELECT * FROM senslopedb.accelerometers"

    node_id_query += " where tsm_id=%d" %tsm_id
    
    #check accel_number
    if accel_number in (1,2):
        if len(tsm_name)==5:
            node_id_query += " and accel_number = %d" %accel_number
            analysis = False
    elif accel_number == '':
        pass
    else:
        raise ValueError('Error accel_number')

    query += node_id_query + ") nodes"
            
    query += (" on times.node_id = nodes.node_id"
              " and times.accel_number=nodes.accel_number")

    if return_db:
        df =  get_db_dataframe(query)
        df.columns = ['ts','tsm_name','node_id','x','y','z'
                      ,'batt','accel_number','accel_id','in_use']
        df.ts = pd.to_datetime(df.ts)
        
        #filter accel in_use
        if analysis:
            df = df[df.in_use==1]
            df = df.drop(['accel_number','in_use'],axis=1)
        
        #voltage filter
        if voltf:
            if len(tsm_name)==5:
                df = df.merge(accelerometers,how='inner', on='accel_id')
                df = df[(df.batt>=df.voltage_min) & (df.batt<=df.voltage_max)]
                df = df.drop(['voltage_min','voltage_max'],axis=1)
        
        if not batt:                
            df = df.drop('batt',axis=1)

        return df
        
    else:
        return query
    
def get_soms_raw(tsm_name = "", from_time = "", to_time = "", type_num="", node_id = ""):

    if not tsm_name:
        raise ValueError('invalid tsm_name')
    
    query_accel = "SELECT version FROM senslopedb.tsm_sensors where tsm_name = '%s'" %tsm_name  
    df_accel =  get_db_dataframe(query_accel) 
    query = "select * from senslopedb.soms_%s" %tsm_name
    
    if not from_time:
        from_time = "2010-01-01"
    
        
    query += " where ts > '%s'" %from_time
    
    if to_time:
        query += " and ts < '%s'" %to_time
    
    
    if node_id:
        query += " and node_id = '%s'" %node_id
    
    if type_num:
        query += " and msid = '%s'" %type_num
        
    df =  get_db_dataframe(query)
    
    
    df.ts = pd.to_datetime(df.ts)
    
    if (df_accel.version[0] == 2):
        if (tsm_name== 'nagsa'):
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
    
    

def get_coords_list():
    try:
        db, cur = senslopedb_connect()
        
        query = 'SELECT name, lat, lon, barangay FROM site_column'
        
        df = psql.read_sql(query, db)
        
        # make a sensor list of columnArray class functions
        sensors = []
        for s in range(len(df)):
            s = CoordsArray(df.name[s],df.lat[s],df.lon[s],df.barangay[s])
            sensors.append(s)
            
        return sensors
    except:
        raise ValueError('Could not get sensor list from database')

#logger_array_list():
#    transforms dataframe TSMdf to list of loggerArray
def logger_array_list(TSMdf):
    return LoggerArray(TSMdf['site_id'].values[0], TSMdf['tsm_id'].values[0], TSMdf['tsm_name'].values[0], TSMdf['number_of_segments'].values[0], TSMdf['segment_length'].values[0])

#get_tsm_list():
#    returns a list of loggerArray objects from the database tables
def get_tsm_list(tsm_name='', end=datetime.now()):
    if tsm_name == '':
        try:
            query = "SELECT site_id, logger_id, tsm_id, tsm_name, number_of_segments, segment_length, date_activated"
            query += " FROM senslopedb.tsm_sensors WHERE (date_deactivated > '%s' OR date_deactivated IS NULL)" %end
            df = get_db_dataframe(query)
            df = df.sort_values(['logger_id', 'date_activated'], ascending=[True, False])
            df = df.drop_duplicates('logger_id')
            
            # make a sensor list of loggerArray class functions
            TSMdf = df.groupby('logger_id', as_index=False)
            sensors = TSMdf.apply(logger_array_list)
            return sensors
        except:
            raise ValueError('Could not get sensor list from database')
    else:
        try:
            query = "SELECT site_id, logger_id, tsm_id, tsm_name, number_of_segments, segment_length, date_activated"
            query += " FROM senslopedb.tsm_sensors WHERE (date_deactivated > '%s' OR date_deactivated IS NULL)" %end
            query += " AND tsm_name = '%s'" %tsm_name
            df = get_db_dataframe(query)
            df = df.sort_values(['logger_id', 'date_activated'], ascending=[True, False])
            df = df.drop_duplicates('logger_id')
            
            # make a sensor list of loggerArray class functions
            TSMdf = df.groupby('logger_id', as_index=False)
            sensors = TSMdf.apply(logger_array_list)
            return sensors
        except:
            raise ValueError('Could not get sensor list from database')

#returns list of non-working nodes from the node status table
#function will only return the latest entry per site per node with
#"Not OK" status
def get_node_status(tsm_id, status=4):   
    try:
        query = "SELECT DISTINCT node_id FROM ("
        query += " SELECT a.node_id FROM"
        query += " accelerometer_status as s"
        query += " left join accelerometers as a"
        query += " on s.accel_id = a.accel_id"
        query += " where tsm_id = %s" %tsm_id
        query += " and status = %s" %status
        query += " ) AS sub"
        df = get_db_dataframe(query)
        return df['node_id'].values
    except:
        raise ValueError('Could not get node status from database')
    
#get_single_lgdpm
#   This function returns the last good data prior to the monitoring window
#   Inputs:
#       site (e.g. sinb, mamb, agbsb)
#       node (e.g. 1,2...15...30)
#       startTS (e.g. 2016-04-25 15:00:00, 2016-02-01 05:00:00, 
#                YYYY-MM-DD HH:mm:SS)
#   Output:
#       returns the dataframe for the last good data prior to the monitoring window
    
def get_single_lgdpm(tsm_name, node_id, startTS):
    query = "SELECT ts,node_id, xval, yval, zval "
    query += "FROM %s WHERE node_id IN (%s) AND ts < '%s' AND ts >= '%s' " %('tilt_'+tsm_name, ','.join(map(str, node_id)), startTS, startTS-timedelta(3))
    if len(tsm_name) == 5:
        query += "and (type_num = 32 or type_num = 11) "        
    query += "ORDER BY ts DESC"
    
    lgdpm = get_db_dataframe(query)   
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
    
    execute_query(query)

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
    
    execute_query(query)

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
    
    execute_query(query)

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
    
    execute_query(query)

#alert_to_db
#    writes to alert tables
#    Inputs:
#        df- dataframe to be written in table_name
#        table_name- str; name of table in database ('tsm_alerts' or 'operational_triggers')
def alert_to_db(df, table_name):
    
    if does_table_exist(table_name) == False:
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
            print_out('unrecognized table : ' + table_name)
            return

    if table_name == 'operational_triggers':
        query = "SELECT * FROM operational_trigger_symbols"
        all_trig = get_db_dataframe(query)
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

    df2 = get_db_dataframe(query)

    if table_name == 'tsm_alerts':
        try:
            same_alert = df2['alert_level'].values[0] == df['alert_level'].values[0]
        except:
            same_alert = False
        query = "SELECT EXISTS(SELECT * FROM tsm_alerts"
        query += " WHERE ts = '%s' AND tsm_id = %s)" %(df['ts_updated'].values[0], df['tsm_id'].values[0])
        if get_db_dataframe(query).values[0][0] == 1:
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
        if get_db_dataframe(query).values[0][0] == 1:
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
        if get_db_dataframe(query).values[0][0] == 1:
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
        if get_db_dataframe(query).values[0][0] == 1:
            inDB = True
        else:
            inDB = False

    if (len(df2) == 0 or not same_alert) and not inDB:
        push_db_dataframe(df, table_name, index=False)
        
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
        execute_query(query)

def memcached():
    mc = memcache.Client(['127.0.0.1:11211'],debug=0)
    sc = mc.get("server_config")
    return sc