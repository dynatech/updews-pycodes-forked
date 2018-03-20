import serverdbio as dbio
import gsmserver as server
import pandas as pd
import pandas.io.sql as psql
import memcache
import cfgfileio as cfg
from datetime import datetime as dt

#get_db_dataframe(query): queries a specific sensor data table and returns it as
#    a python dataframe format
#    Parameters:
#        query: str
#            mysql like query code
#    Returns:
#        df: dataframe object
#            dataframe object of the result set
def get_db_dataframe(query):
    try:
        db, cur = dbio.db_connect()
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


def set_logger_mobiles():
	server.get_mobile_sim_nums('loggers')
	server.get_mobile_sim_nums('users')

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

def set_server_cfg(mc):
	print 'Setting config file to memory ...',
	server_config = cfg.config()
	mc.set('server_config',server_config)
	print 'done'
	

def main():
	print dt.today().strftime('%Y-%m-%d %H:%M:%S')	
	print "Connecting to memcache client ...", 
	mc = memcache.Client(['127.0.0.1:11211'],debug=0)
	print 'done'

	print 'Setting server configuration',
	c = cfg.dewsl_server_config()
	mc.set("server_config",c.config)
	print 'done'

	print 'Reset alergenexec',
	mc.set("alertgenexec", False)
	print 'done'

	# set_server_cfg(mc)
	try:
		set_mysql_tables(mc)
	except KeyError:
		print ">> KeyError"
	
if __name__ == "__main__":
    main()