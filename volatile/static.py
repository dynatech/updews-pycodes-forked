import memory
import dynadb.db as dbio
from datetime import datetime as dt
import pandas.io.sql as psql
import pandas as pd

def get_db_dataframe(query):
    try:
        db, cur = dbio.connect()
        df = psql.read_sql(query, db)
        # df.columns = ['ts','id','x','y','z','m']
        # change ts column to datetime
        # df.ts = pd.to_datetime(df.ts)

        db.close()
        return df
    except KeyboardInterrupt:
        print "Exception detected in accessing database"
        sys.exit()
    except psql.DatabaseError:
    	print "Error getting query %s" % (query)
    	return None

def set_mysql_tables(mc):
	tables = ['sites','tsm_sensors','loggers','accelerometers']

	print 'Setting dataframe tables to memory'
	for key in tables:
		print "%s," % (key),
		df = get_db_dataframe("select * from %s;" % key)

		if df is None:
			continue

		mc.set('df_'+key,df)

		# special configuration
		if key == 'sites':
			mc.set(key+'_dict',df.set_index('site_code').to_dict())

	print ' ... done'

def get_mobiles(table, host = None, args = None):
    """
        **Description:**
          -The get mobile sim nums is a function that get the number of the loggers or users in the database.
         
        :param table: loggers or users table.
        :param host: host name of the number and  **Default** to **None**
        :type table: str
        :type host: str 
        :returns:  **mobile number** (*int*) - mobile number of user or logger
    """
    mc = memory.get_handle()

    if host is None:
        raise ValueError("No host value given for mobile number")

    if args:
        is_reset_variables = args.reset_variables
    else:
        is_reset_variables = False

    if table == 'loggers':

        logger_mobile_sim_nums = mc.get('logger_mobile_sim_nums')
        if logger_mobile_sim_nums and not is_reset_variables:
            return logger_mobile_sim_nums

        print "Force reset logger mobiles in memory"

        query = ("SELECT t1.mobile_id, t1.sim_num, t1.gsm_id "
            "FROM logger_mobile AS t1 "
            "LEFT OUTER JOIN logger_mobile AS t2 "
            "ON t1.sim_num = t2.sim_num "
            "AND (t1.date_activated < t2.date_activated "
            "OR (t1.date_activated = t2.date_activated "
            "AND t1.mobile_id < t2.mobile_id)) "
            "WHERE t2.sim_num IS NULL and t1.sim_num is not null")

        nums = dbio.read(query, 'get_mobile_sim_nums', host)

        logger_mobile_sim_nums = {sim_num: mobile_id for (mobile_id, sim_num, 
            gsm_id) in nums}
        mc.set("logger_mobile_sim_nums", logger_mobile_sim_nums)

        logger_mobile_def_gsm_id = {mobile_id: gsm_id for (mobile_id, sim_num, 
            gsm_id) in nums}
        mc.set("logger_mobile_def_gsm_id", logger_mobile_def_gsm_id)

    elif table == 'users':

        user_mobile_sim_nums = mc.get('user_mobile_sim_nums')
        if user_mobile_sim_nums and not is_reset_variables:
            return user_mobile_sim_nums

        print "Force reset user mobiles in memory"
        
        query = "select mobile_id, sim_num, gsm_id from user_mobile"

        nums = dbio.read(query, 'get_mobile_sim_nums', host)

        user_mobile_sim_nums = {sim_num: mobile_id for (mobile_id, sim_num, 
            gsm_id) in nums}
        mc.set("user_mobile_sim_nums",user_mobile_sim_nums)

        user_mobile_def_gsm_id = {mobile_id: gsm_id for (mobile_id, sim_num, 
            gsm_id) in nums}
        mc.set("user_mobile_def_gsm_id", user_mobile_def_gsm_id)

    else:
        print 'Error: table', table
        sys.exit()

    return nums

def get_surficial_markers(host = None, from_memory = True):
    mc = memory.get_handle()
    sc = memory.server_config()

    if from_memory:
        return mc.get("surficial_markers")

    if not host:
        print "Host defaults to datadb"
        host = sc["resource"]["datadb"]

    query = ("select m2.marker_id, m3.marker_name, m4.site_id from "
        "(select max(history_id) as history_id, "
        "marker_id from marker_history as m1 "
        "group by m1.marker_id "
        ") as m2 "
        "inner join marker_names as m3 "
        "on m2.history_id = m3.history_id "
        "inner join markers as m4 "
        "on m2.marker_id = m4.marker_id ")

    engine = dbio.df_engine(host)
    surficial_markers = pd.read_sql(query, engine)
    mc.set("surficial_markers", surficial_markers)

    return surficial_markers

def get_surficial_parser_reply_messages():
    query = "select * from surficial_parser_reply_messages"
    
    df = get_db_dataframe(query)
    df = df.set_index("msg_id")

    return df


def main(args):

    print dt.today().strftime('%Y-%m-%d %H:%M:%S')	
    mc = memory.get_handle()
    sc = memory.server_config()

    print "Reset alergenexec",
    mc.set("alertgenexec", False)
    print "done"

    print "Set static tables to memory",
    try:
    	set_mysql_tables(mc)
    except KeyError:
    	print ">> KeyError"
    print "done"

    print "Set mobile numbers to memory",
    mobiles_host = sc["resource"]["mobile_nums_db"]
    get_mobiles("loggers", mobiles_host, args)
    get_mobiles("users", mobiles_host, args)
    print "done"

    print "Set surficial_markers to memory", 
    get_surficial_markers(from_memory = False)
    print "done"

    print "Set surficial_parser_reply_messages",
    df = get_surficial_parser_reply_messages()
    mc.set("surficial_parser_reply_messages", df)
    print "done"
	
if __name__ == "__main__":
    main()