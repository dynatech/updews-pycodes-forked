from datetime import datetime, timedelta
import memcache
import pandas.io.sql as psql
import pandas as pd
import platform
import volatile.memory as memory 
from sqlalchemy import create_engine
import re

curOS = platform.system()

if curOS == "Windows":
    import MySQLdb as mysqlDriver
elif curOS == "Linux":
    import pymysql as mysqlDriver

import dynadb.db as db
import gsm.smsparser2.smsclass as sms
import volatile.memory as mem


def print_out(line):
    """Prints line.
    
    """
    sc = mem.server_config()
    if sc['print']['print_stdout']:
        print line


def does_table_exist(table_name, hostdb='local'):
    """Checks if table exists in database.
    
    Args:
        table_name (str): Name of table to be checked.
        hostdb (str): Host of database to be checked. Defaults to local.

    Returns:
        bool: True if table exists otherwise, False.
    
    """

    query = "SHOW TABLES LIKE '%s'" %table_name
    df = db.df_read(query)

    if len(df) > 0:
        return True
    else:
        return False


def get_latest_ts(table_name):
    try:
        query = "SELECT max(ts) FROM %s" %table_name
        ts = db.df_read(query).values[0][0]
        return pd.to_datetime(ts)
    except:
        print_out("Error in getting maximum timestamp")
        return ''
        

def get_alert_level(site_id, end):
    """Retrieves alert level.
    
    Args:
        tsm_id (int): ID of site to retrieve alert level from.
        end (bool): Timestamp of alert level to be retrieved.

    Returns:
        dataframe: Dataframe containing alert_level.
    
    """

    query =  "SELECT alert_level FROM "
    query += "  (SELECT * FROM public_alerts "
    query += "  WHERE site_id = %s " %site_id
    query += "  AND ts <= '%s' " %end
    query += "  AND ts_updated >= '%s' " %(end - timedelta(hours=0.5))
    query += "  ) AS a "
    query += "INNER JOIN "
    query += "  (SELECT pub_sym_id, alert_level FROM public_alert_symbols "
    query += "  ) AS s "
    query += "USING(pub_sym_id)"

    df = db.df_read(query)
    
    return df


########################### RAINFALL-RELATED QUERIES ###########################


def create_rainfall_gauges():    
    """Creates rainfall_gauges table; record of available rain gauges for
    rainfall alert analysis.

    """
    
    query = "CREATE TABLE `rainfall_gauges` ("
    query += "  `rain_id` SMALLINT(5) UNSIGNED NOT NULL AUTO_INCREMENT,"
    query += "  `gauge_name` VARCHAR(5) NOT NULL,"
    query += "  `data_source` VARCHAR(8) NOT NULL,"
    query += "  `latitude` DECIMAL(9,6) UNSIGNED NOT NULL,"
    query += "  `longitude` DECIMAL(9,6) UNSIGNED NOT NULL,"
    query += "  `date_activated` DATE NOT NULL,"
    query += "  `date_deactivated` DATE NULL,"
    query += "  PRIMARY KEY (`rain_id`),"
    query += "  UNIQUE INDEX `gauge_name_UNIQUE` (`gauge_name` ASC))"

    db.write(query)


def create_rainfall_priorities():
    """Creates rainfall_priorities table; record of distance of nearby 
    rain gauges to sites for rainfall alert analysis.

    """

    query = "CREATE TABLE `rainfall_priorities` ("
    query += "  `priority_id` SMALLINT(5) UNSIGNED NOT NULL AUTO_INCREMENT,"
    query += "  `rain_id` SMALLINT(5) UNSIGNED NOT NULL,"
    query += "  `site_id` TINYINT(3) UNSIGNED NOT NULL,"
    query += "  `distance` DECIMAL(5,2) UNSIGNED NOT NULL,"
    query += "  PRIMARY KEY (`priority_id`),"
    query += "  INDEX `fk_rainfall_priorities_sites1_idx` (`site_id` ASC),"
    query += "  INDEX `fk_rainfall_priorities_rain_gauges1_idx` (`rain_id` ASC),"
    query += "  UNIQUE INDEX `uq_rainfall_priorities` (`site_id` ASC, `rain_id` ASC),"
    query += "  CONSTRAINT `fk_rainfall_priorities_sites1`"
    query += "    FOREIGN KEY (`site_id`)"
    query += "    REFERENCES `sites` (`site_id`)"
    query += "    ON DELETE CASCADE"
    query += "    ON UPDATE CASCADE,"
    query += "  CONSTRAINT `fk_rainfall_priorities_rain_gauges1`"
    query += "    FOREIGN KEY (`rain_id`)"
    query += "    REFERENCES `rainfall_gauges` (`rain_id`)"
    query += "    ON DELETE CASCADE"
    query += "    ON UPDATE CASCADE)"
    
    db.write(query)


def create_NOAH_table(gauge_name):
    """Create table for gauge_name.
    
    """
    
    query = "CREATE TABLE `%s` (" %gauge_name
    query += "  `data_id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,"
    query += "  `ts` TIMESTAMP NOT NULL,"
    query += "  `rain` DECIMAL(4,1) NOT NULL,"
    query += "  `temperature` DECIMAL(3,1) NULL DEFAULT NULL,"
    query += "  `humidity` DECIMAL(3,1) NULL DEFAULT NULL,"
    query += "  `battery1` DECIMAL(4,3) NULL DEFAULT NULL,"
    query += "  `battery2` DECIMAL(4,3) NULL DEFAULT NULL,"
    query += "  `csq` TINYINT(3) NULL DEFAULT NULL,"
    query += "  PRIMARY KEY (`data_id`),"
    query += "  UNIQUE INDEX `ts_UNIQUE` (`ts` ASC))"
    query += " ENGINE = InnoDB"
    query += " DEFAULT CHARACTER SET = utf8;"

    print_out("Creating table: %s..." % gauge_name)

    db.write(query)


def get_raw_rain_data(gauge_name, from_time='2010-01-01', to_time=""):
    """Retrieves rain gauge data from the database.
    
    Args:
        gauge_name (str): Name of rain gauge to collect data from.
        from_time (str): Start of data to be collected.
        to_time (str): End of data to be collected. Optional.

    Returns:
        dataframe: Rainfall data of gauge_name from from_time [to to_time].
    
    """

    query = "SELECT ts, rain FROM %s " %gauge_name
    query += "WHERE ts > '%s'" %from_time
    
    if to_time:
        query += "AND ts < '%s'" %to_time

    query += "ORDER BY ts"

    df = db.df_read(query)
    df['ts'] = pd.to_datetime(df['ts'])
    
    return df


def does_alert_exists(site_id, end, alert):
    """Retrieves alert level.
    
    Args:
        tsm_id (int): ID of site to retrieve alert level from.
        end (bool): Timestamp of alert level to be retrieved.

    Returns:
        dataframe: Dataframe containing alert_level.
    
    """

    query = "SELECT EXISTS(SELECT * FROM rainfall_alerts"
    query += " WHERE ts = '%s' AND site_id = %s" %(end, site_id)
    query += " AND rain_alert = '%s')" %alert

    df = db.df_read(query)
    
    return df


########################## SUBSURFACE-RELATED QUERIES ##########################


def query_pattern(template_id="", dictionary=""):
    """
    - Query pattern.

    Args:
        template_id (str): Args.
        dictionary (dict):
    Returns:
        query (string) : Returns the query with details in dictionary

    """
    if template_id == 'raw_accel':
        string = ("SELECT ts,'[tsm_name]' as 'tsm_name',times.node_id, xval, "
                    " yval, zval, batt, times.accel_number, accel_id, "
                    " in_use from (select *, if(type_num in (32,11) or "
                    " type_num is NULL, 1,if(type_num in (33,12),2,0)) "
                    " as 'accel_number' from tilt_[tsm_name] WHERE "
                    " ts >= '[from_time]' AND ts <= '[to_time]' [node_id]) "
                    " times  [node_id_query]) nodes on "
                    " times.node_id = nodes.node_id and "
                    "times.accel_number=nodes.accel_number")
                    
    elif template_id == 'soms_raw':
        string = ("select * from senslopedb.soms_[tsm_name] where " 
                  " ts > '[from_time]' [to_time] [node_id] [type_num]") 
    else:
         raise ValueError("template_id doesn't exists")
         return
         
    for item in sorted(dictionary.keys()):
        string = re.sub(r'\[' + item + '\]', dictionary[item], string)
    return string


def get_tsm_id(tsm_details="", tsm_name="", to_time=""):
    """
    - Get tsm id.

    Args:
        tsm_details (str): Sensor Details.
        tsm_name (str) : Sensor Name
        to_time (datetime) : To time.

    Returns:
        tsm_id : tilt sensor id.

    Raises:
        ValueError : Input tsm_name error

    """

    if tsm_details.tsm_id[tsm_details.tsm_name==tsm_name].count()>1:
        
        tsm_id = (tsm_details.tsm_id[(tsm_details.tsm_name==tsm_name) & 
                                     ((tsm_details.date_deactivated>=to_time) 
                                     | (tsm_details.date_deactivated.isnull()))
                                    ])
    else:
        tsm_id = tsm_details.tsm_id[tsm_details.tsm_name==tsm_name]
    
    if tsm_id.empty:
        raise ValueError("Input tsm_name error")
    else:
        return tsm_id.iloc[0]


def filter_raw_accel(accel_info="",query="",df=""):

    """
    - Filter raw accel dataframe data.

    Args:
        accel_info (obj): Compiled details of raw accel.
        query (str) : Query statement of raw accel data.
        df (dataframe) : Raw accel dataframe

    Returns:
        df (dataframe) : filtered dataframe.
        query (str) : Query statement of raw accel data.
   
    """

    accelerometers = memory.get('DF_ACCELEROMETERS')  
        
    if re.search("analysis", accel_info['output_type']):
        df = df[df.in_use==1]
        df = df.drop(['accel_number','in_use'],axis=1)
    elif re.search("query", accel_info['output_type']):
        return query
     
    if re.search("voltf", accel_info['output_type']):
     if len(accel_info['tsm_name'])==5:
        df = df.merge(accelerometers,how='inner', on='accel_id')
        df = df[(df.batt>=df.voltage_min) & (df.batt<=df.voltage_max)]
        df = df.drop(['voltage_min','voltage_max'],axis=1)
         
    return df

def check_timestamp(from_time="",to_time=""):

    """
    - Check timestamp format.

    Args:
        from_time (date, datetime): From time.
        to_time (date, datetime): To Time
    Returns:
        Returns valid date and datetime format.

    Raises:
        ValueError : Input from_time error
        ValueError : Input to_time error
        ValueError : Input from_time and to_time error

    """

    if from_time=="":
        from_time= pd.to_datetime("2010-01-01")
    else:
        try:
            from_time=pd.to_datetime(from_time)
        except ValueError:
            raise ValueError("Input from_time error")
    
    if to_time=="":
        to_time= pd.to_datetime(datetime.now())
    else:
        try:
            to_time=pd.to_datetime(to_time)
        except ValueError:
            raise ValueError("Input to_time error")
            
    time_diff = from_time - to_time
    if str(time_diff.days).find("-") == 0:
        return {'from_time':from_time, 'to_time':to_time}
    else:
        raise ValueError("Input from_time and to_time error")
    
    
def get_raw_accel_data_2(tsm_id="", tsm_name="", from_time="", 
                         to_time= "", accel_number="", node_id="", 
                         output_type=""):
                             
    """
    - Retrieves raw accel data.
    
    Args:
        tsm_id (int): ID of tsm sensor to retrieve data from. 
                      Optional if with tsm_name.
        tsm_name (str): name of tsm sensor to retrieve data from. 
                        Optional if with tsm_id.
        from_time (datetime): Start timestamp of data to be retrieved. Optional.
        to_time (datetime): End timestamp of data to be retrieved. Optional.
        accel_number (int): ID of accel to be retrieved. Optional.
        node_id (int): ID of node to be retrieved. Optional.
        output_type (str): Whether to return dataframe or filtered dataframe. 
                          Defaults to dataframe.

    Returns:
        dataframe/str: Dataframe containing accel data / 
                       query used in retrieving data.
    
    Raises:
        ValueError : Input tsm_id error
        ValueError : Error node_id
        ValueError : Error accel_number

    """
    tsm_details = memory.get('DF_TSM_SENSORS')
    tsm_details.date_deactivated = pd.to_datetime(tsm_details.date_deactivated)
    
    ref_ts = check_timestamp(from_time, to_time)

    if tsm_id != '':
        try:
            tsm_name = tsm_details.tsm_name[tsm_details.tsm_id==tsm_id].iloc[0]
        except IndexError:
            raise ValueError("Input tsm_id error")
    else:
        tsm_id = get_tsm_id(tsm_details, tsm_name, to_time)

    if node_id != '':
        if ((node_id>tsm_details.number_of_segments
             [tsm_details.tsm_id==tsm_id].iloc[0]) or (node_id<1)):
            raise ValueError('Error node_id')
        else:
            node_id =' AND node_id = %d' %node_id

    node_id_query = " inner join (SELECT * FROM senslopedb.accelerometers"
    node_id_query += " where tsm_id=%d" %tsm_id

    if accel_number in (1,2):
        if len(tsm_name)==5:
            node_id_query += " and accel_number = %d" %accel_number
    elif accel_number == '':
        pass
    else:
        raise ValueError('Error accel_number')

    variables = {'tsm_name':tsm_name,
                'from_time':str(ref_ts['from_time'])[:-3],
                'to_time':str(ref_ts['to_time'])[:-3],
                'node_id':node_id,
                'tsm_id':str(tsm_id),
                'node_id_query':node_id_query
                }            
    query = query_pattern('raw_accel',variables)

    df =  db.df_read(str(query))
    df.columns = ['ts','tsm_name','node_id','x','y','z'
                      ,'batt','accel_number','accel_id','in_use']
    df.ts = pd.to_datetime(df.ts)
    
    if output_type == "":
        return df
    else:
        accel_info = {"tsm_id":tsm_id, "tsm_name":tsm_name, 
                      "from_time":from_time, "to_time":to_time, 
                      "accel_number":accel_number, "node_id":node_id, 
                      "output_type":output_type}
        return filter_raw_accel(accel_info,query,df)      

    
################################################################################


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
    sc = mem.server_config()
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


#Check if table exists
#   Returns true if table exists


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
    
    memc.set('tsm', db.df_read(query_tsm))
    memc.set('accel', db.df_read(query_accel))
    
    print_out("Updated memcached with MySQL data")

    
def get_soms_raw(tsm_name = "", from_time = "", to_time = "", type_num="", node_id = ""):

    if not tsm_name:
        raise ValueError('invalid tsm_name')
    
    query_accel = "SELECT version FROM senslopedb.tsm_sensors where tsm_name = '%s'" %tsm_name  
    df_accel =  db.df_read(query_accel) 
    query = "select * from senslopedb.soms_%s" %tsm_name
    
    if not from_time:
        from_time = "2010-01-01"
    
        
    query += " where ts > '%s'" %from_time
    
    if to_time:
        query += " and ts < '%s'" %to_time
    
    
    if node_id:
        query += " and node_id = '%s'" %node_id
    
    if type_num:
        query += " and type_num = '%s'" %type_num
        

        
    df =  db.df_read(query)
    
    
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

def ref_get_soms_raw(tsm_name="", from_time="", to_time="", type_num="", node_id=""):
   tsm_details=memory.get("DF_TSM_SENSORS")
   #For blank tsm_name
   if not tsm_name:
       raise ValueError('enter valid tsm_name')
   if not node_id:
       node_id = 0
   
   #For invalid node_id    
   check_num_seg=tsm_details[tsm_details.tsm_name == tsm_name].reset_index().number_of_segments[0]

   if (int(node_id) > int(check_num_seg)):
       raise ValueError('Invalid node id. Exceeded number of nodes')
   
   #For invalid type_num
   check_type_num=tsm_details[tsm_details.tsm_name == tsm_name].reset_index().version[0]
   v3_types = [110,113,10,13]
   v2_types = [21,26,112,111]
   if (check_type_num ==3):
       if int(type_num) not in v3_types:
           raise ValueError('Invalid msgid for version 3 soms sensor. Valid values are 110,113,10,13')
   elif (check_type_num == 2):
       if int(type_num) not in v2_types:
           raise ValueError('Invalid msgid for version 2 soms sensor. Valid values are 111,112,21,26')
   else:
       pass
   
   query_accel = "SELECT version FROM senslopedb.tsm_sensors where tsm_name = '%s'" %tsm_name  
   df_accel =  get_db_dataframe(query_accel) 

   if not from_time:
       from_time = "2010-01-01"
   if to_time:
       to_time = " and ts < '%s'" %to_time
   
   if node_id:
       node_id = " and node_id = {}" .format(node_id)
       
   if type_num:
       type_num = " and type_num = {}" .format(type_num)
       
   query = query_pattern(template_id='soms_raw',
                       dictionary= {"tsm_name" : tsm_name, 
                                    "from_time": from_time, 
                                    "to_time": to_time, 
                                    "node_id": node_id, 
                                    "type_num": type_num})      
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
        
   df = df.drop('mval2', axis=1, inplace=False)
   df['tsm_name'] = tsm_name
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
            df = db.df_read(query)
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
            df = db.df_read(query)
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
        df = db.df_read(query)
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

    
    db.write(query)

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
    
    db.write(query)

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
    
    db.write(query)

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
    
    db.write(query)

#alert_to_db
#    writes to alert tables
#    Inputs:
#        df- dataframe to be written in table_name
#        table_name- str; name of table in database ('tsm_alerts' or 'operational_triggers')
def alert_to_db(df, table_name):
    """Summary of cumulative rainfall, threshold, alert and rain gauge used in
    analysis of rainfall.
    
    Args:
        df (dataframe): Dataframe to be written to database.
        table_name (str): Name of table df to be written to.
    
    """

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
        all_trig = db.df_read(query)
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
            surficial = db.df_read(query)

            if len(surficial) == 0:
                data_table = sms.DataTable(table_name, df)
                db.df_write(data_table)
            else:
                trigger_id = surficial['trigger_id'].values[0]
                trigger_sym_id = df['trigger_sym_id'].values[0]
                if trigger_sym_id != surficial['trigger_sym_id'].values[0]:
                    query =  "UPDATE %s " %table_name
                    query += "SET trigger_sym_id = '%s' " %trigger_sym_id
                    query += "WHERE trigger_id = %s" %trigger_id
                    db.write(query)
            
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

    df2 = db.df_read(query)

    if table_name == 'public_alerts':
        query =  "SELECT * FROM %s " %table_name
        query += "WHERE site_id = %s " %df['site_id'].values[0]
        query += "AND ts = '%s' " %df['ts'].values[0]
        query += "AND pub_sym_id = %s" %df['pub_sym_id'].values[0]

        df2 = df2.append(db.df_read(query))

    # writes alert if no alerts within the past 30mins
    if len(df2) == 0:
        data_table = sms.DataTable(table_name, df)
        db.df_write(data_table)
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
            data_table = sms.DataTable(table_name, df)
            db.df_write(data_table)
        else:
            query =  "UPDATE %s " %table_name
            query += "SET ts_updated = '%s' " %df['ts_updated'].values[0]
            query += "WHERE %s = %s" %(pk_id, df2[pk_id].values[0])
            db.write(query)

#        data_table = sms.DataTable('rainfall_gauges', deactivated_gauges)
#        db.df_write(data_table)


def memcached():
    # mc = memcache.Client(['127.0.0.1:11211'],debug=0)
    return mem.server_config()