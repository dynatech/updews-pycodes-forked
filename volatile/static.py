import memory
import dynadb.db as dbio
from datetime import datetime as dt
import pandas.io.sql as psql

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

def get_mobiles(table, host = None):
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

    if table == 'loggers':

        logger_mobile_sim_nums = mc.get('logger_mobile_sim_nums')
        if logger_mobile_sim_nums:
            return logger_mobile_sim_nums

        query = ("SELECT t1.mobile_id,t1.sim_num "
            "FROM logger_mobile AS t1 "
            "LEFT OUTER JOIN logger_mobile AS t2 "
            "ON t1.sim_num = t2.sim_num "
            "AND (t1.date_activated < t2.date_activated "
            "OR (t1.date_activated = t2.date_activated "
            "AND t1.mobile_id < t2.mobile_id)) "
            "WHERE t2.sim_num IS NULL and t1.sim_num is not null")

        nums = dbio.read(query,'get_mobile_sim_nums', host)
        nums = {key: value for (value, key) in nums}

        logger_mobile_sim_nums = nums
        mc.set("logger_mobile_sim_nums",logger_mobile_sim_nums)

    elif table == 'users':

        user_mobile_sim_nums = mc.get('user_mobile_sim_nums')
        if user_mobile_sim_nums:
            return user_mobile_sim_nums
        
        query = "select mobile_id,sim_num from user_mobile"

        nums = dbio.read(query,'get_mobile_sim_nums', host)
        nums = {key: value for (value, key) in nums}

        user_mobile_sim_nums = nums
        mc.set("user_mobile_sim_nums",user_mobile_sim_nums)

    else:
        print 'Error: table', table
        sys.exit()

    return nums

def main():

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
	get_mobiles("loggers", mobiles_host)
	get_mobiles("users", mobiles_host)
	print "done"
	
if __name__ == "__main__":
    main()