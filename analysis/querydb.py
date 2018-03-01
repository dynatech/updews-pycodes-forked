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
    
    if ((df_accel.version[0] == 2) and (type_num == 111)):
        if (tsm_name== 'nagsa'):
            df['mval1-n'] =(((8000000/(df.mval1))-(8000000/(df.mval2)))*4)/10
        else:
            df['mval1-n'] =(((20000000/(df.mval1))-(20000000/(df.mval2)))*4)/10     
        
        df = df.drop('mval1', axis=1, inplace=False)
        df = df.drop('mval2', axis=1, inplace=False)
        df['mval1'] = df['mval1-n']
        df = df.drop('mval1-n', axis=1, inplace=False)
    
    #df = df.replace("-inf", "NAN")         
#    df = df.drop('mval2', axis=1, inplace=False)

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
    
def get_single_lgdpm(tsm_name, no_init_val, offsetstart, analysis=True):
    lgdpm = get_raw_accel_data(tsm_name=tsm_name, from_time=offsetstart-timedelta(3),
                               to_time=offsetstart, analysis=analysis)
    lgdpm = lgdpm[lgdpm.node_id.isin(no_init_val)]

    return lgdpm

#create_alert_status
#    creates table named 'alert_status' which contains alert valid/invalid status
def create_alert_status():
    query = "CREATE TABLE `alert_status` ("
    query += "  `stat_id` INT(7) UNSIGNED NOT NULL AUTO_INCREMENT,"
    query += "  `ts_last_retrigger` TIMESTAMP NULL,"
    query += "  `trigger_id` INT(10) UNSIGNED NULL,"
    query += "  `ts_set` TIMESTAMP NULL,"
    query += "  `ts_ack` TIMESTAMP NULL,"
    query += "  `alert_status` TINYINT(1) NULL"
    query += "      COMMENT 'alert_status:\n-1 invalid\n0 validating\n1 valid',"
    query += "  `remarks` VARCHAR(450) NULL,"
    query += "  `user_id` SMALLINT(6) UNSIGNED NULL,"
    query += "  PRIMARY KEY (`stat_id`),"
    query += "  INDEX `fk_alert_status_operational_triggers1_idx` (`trigger_id` ASC),"
    query += "  CONSTRAINT `fk_alert_status_operational_triggers1`"
    query += "    FOREIGN KEY (`trigger_id`)"
    query += "    REFERENCES `operational_triggers` (`trigger_id`)"
    query += "    ON DELETE NO ACTION"
    query += "    ON UPDATE CASCADE,"
    query += "  INDEX `fk_alert_status_users1_idx` (`user_id` ASC),"
    query += "  CONSTRAINT `fk_alert_status_users1`"
    query += "    FOREIGN KEY (`user_id`)"
    query += "    REFERENCES `users` (`user_id`)"
    query += "    ON DELETE NO ACTION"
    query += "    ON UPDATE CASCADE,"
    query += "  UNIQUE INDEX `uq_alert_status`"
    query += "    (`ts_last_retrigger` ASC, `trigger_id` ASC))"

    
    execute_query(query)

#create_tsm_alerts
#    creates table named 'tsm_alerts' which contains alerts for all tsm
def create_tsm_alerts():    
    query = "CREATE TABLE `tsm_alerts` ("
    query += "  `ta_id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,"
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
    query += "  `trigger_id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,"
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
        #Create a operational_triggers table if it doesn't exist yet
        elif table_name == 'operational_triggers':
            create_operational_triggers()
        else:
            print_out('unrecognized table : ' + table_name)
            return
    
    if table_name == 'operational_triggers':
        # checks trigger source
        query =  "SELECT * FROM "
        query += "  operational_trigger_symbols AS op "
        query += "INNER JOIN "
        query += "  trigger_hierarchies AS trig "
        query += "ON op.source_id = trig.source_id "
        all_trig = get_db_dataframe(query)
        trigger_source = all_trig[all_trig.trigger_sym_id == \
                    df['trigger_sym_id'].values[0]]['trigger_source'].values[0]

        # does not write nd subsurface alerts
        if trigger_source == 'subsurface':
            alert_level = all_trig[all_trig.trigger_sym_id == \
                    df['trigger_sym_id'].values[0]]['alert_level'].values[0]
            if alert_level == -1:
                return
        # if ts does not exist, writes alert; else: updates alert level
        elif trigger_source == 'surficial':

            query =  "SELECT trigger_id, trig.trigger_sym_id FROM "
            query += "  (SELECT trigger_sym_id, alert_level, alert_symbol, "
            query += "  op.source_id, trigger_source FROM "
            query += "    operational_trigger_symbols AS op "
            query += "  INNER JOIN "
            query += "    (SELECT * FROM trigger_hierarchies "
            query += "    WHERE trigger_source = '%s' " %trigger_source
            query += "    ) AS trig "
            query += "  ON op.source_id = trig.source_id "
            query += "  ) AS sym "
            query += "INNER JOIN "
            query += "  (SELECT * FROM operational_triggers "
            query += "  WHERE site_id = %s " %df['site_id'].values[0]
            query += "  AND ts = '%s' " %df['ts'].values[0]
            query += "  ) AS trig "
            query += "ON trig.trigger_sym_id = sym.trigger_sym_id"
            surficial = get_db_dataframe(query)

            if len(surficial) == 0:
                push_db_dataframe(df, table_name, index=False)
            else:
                trigger_id = surficial['trigger_id'].values[0]
                trigger_sym_id = df['trigger_sym_id'].values[0]
                if trigger_sym_id != surficial['trigger_sym_id'].values[0]:
                    query =  "UPDATE %s " %table_name
                    query += "SET trigger_sym_id = '%s' " %trigger_sym_id
                    query += "WHERE trigger_id = %s" %trigger_id
                    execute_query(query)
            
            return
                
        query =  "SELECT * FROM "
        query += "  (SELECT trigger_sym_id, alert_level, alert_symbol, "
        query += "    op.source_id, trigger_source FROM "
        query += "      operational_trigger_symbols AS op "
        query += "    INNER JOIN "
        query += "      (SELECT * FROM trigger_hierarchies "
        query += "      WHERE trigger_source = '%s' " %trigger_source
        query += "      ) AS trig "
        query += "    ON op.source_id = trig.source_id "
        query += "    ) AS sym "
        query += "INNER JOIN "
        query += "  ( "
    
    else:
        query = ""

    if table_name == 'tsm_alerts':
        where_id = 'tsm_id'
    else:
        where_id = 'site_id'
        
    ts_updated = pd.to_datetime(df['ts_updated'].values[0])-timedelta(hours=0.5)
    
    # previous alert
    query += "  SELECT * FROM %s " %table_name
    query += "  WHERE %s = %s " %(where_id, df[where_id].values[0])
    query += "  AND ((ts <= '%s' " %df['ts_updated'].values[0]
    query += "    AND ts_updated >= '%s') " %df['ts_updated'].values[0]
    query += "  OR (ts_updated <= '%s' " %df['ts_updated'].values[0]
    query += "    AND ts_updated >= '%s')) " %ts_updated

    if table_name == 'operational_triggers':
        
        query += "  ) AS trig "
        query += "ON trig.trigger_sym_id = sym.trigger_sym_id "

    query += "ORDER BY ts DESC LIMIT 1"

    df2 = get_db_dataframe(query)

    if table_name == 'public_alerts':
        query =  "SELECT * FROM %s " %table_name
        query += "WHERE site_id = %s " %df['site_id'].values[0]
        query += "AND ts = '%s' " %df['ts'].values[0]
        query += "AND pub_sym_id = %s" %df['pub_sym_id'].values[0]

        df2 = df2.append(get_db_dataframe(query))

    # writes alert if no alerts within the past 30mins
    if len(df2) == 0:
        push_db_dataframe(df, table_name, index=False)
    # does not update ts_updated if ts in written ts to ts_updated range
    elif pd.to_datetime(df2['ts_updated'].values[0]) >= \
                  pd.to_datetime(df['ts_updated'].values[0]):
        pass
    # if diff prev alert, writes to db; else: updates ts_updated
    else:
        if table_name == 'tsm_alerts':
            alert_comp = 'alert_level'
            pk_id = 'ta_id'
        elif table_name == 'public_alerts':
            alert_comp = 'pub_sym_id'
            pk_id = 'public_id'
        else:
            alert_comp = 'trigger_sym_id'
            pk_id = 'trigger_id'

        same_alert = df2[alert_comp].values[0] == df[alert_comp].values[0]
        
        try:
            same_alert = same_alert[0]
        except:
            pass
        
        if not same_alert:
            push_db_dataframe(df, table_name, index=False)
        else:
            query =  "UPDATE %s " %table_name
            query += "SET ts_updated = '%s' " %df['ts_updated'].values[0]
            query += "WHERE %s = %s" %(pk_id, df2[pk_id].values[0])
            execute_query(query)

def memcached():
    mc = memcache.Client(['127.0.0.1:11211'],debug=0)
    sc = mc.get("server_config")
    return sc