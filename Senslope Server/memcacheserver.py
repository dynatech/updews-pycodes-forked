import senslopedbio as dbio
import senslopeServer as server
import pandas as pd
import pandas.io.sql as psql
import memcache
import cfgfileio as cfg
from datetime import datetime as dt

#GetDBDataFrame(query): queries a specific sensor data table and returns it as
#    a python dataframe format
#    Parameters:
#        query: str
#            mysql like query code
#    Returns:
#        df: dataframe object
#            dataframe object of the result set
def GetDBDataFrame(query):
    try:
        db, cur = dbio.SenslopeDBConnect()
        df = psql.read_sql(query, db)
        # df.columns = ['ts','id','x','y','z','m']
        # change ts column to datetime
        # df.ts = pd.to_datetime(df.ts)

        db.close()
        return df
    except KeyboardInterrupt:
        PrintOut("Exception detected in accessing database")


def setLoggerMobiles():
	server.getMobileSimNums('loggers')
	server.getMobileSimNums('users')

def setMysqlTables(mc):
	tables = ['sites','tsm_sensors','loggers','accelerometers']

	print 'Setting dataframe tables to memory'
	for key in tables:
		print "%s," % (key),
		df = GetDBDataFrame("select * from %s;" % key)
		mc.set('df_'+key,df)

		# special configuration
		if key == 'sites':
			mc.set(key+'_dict',df.set_index('site_code').to_dict())

	print ' ... done'

def setServerConfig(mc):
	print 'Setting config file to memory ...',
	server_config = cfg.config()
	mc.set('server_config',server_config)
	print 'done'
	

def main():
	print dt.today().strftime('%Y-%m-%d %H:%M:%S')	
	print "Connecting to memcache client ...", 
	mc = memcache.Client(['127.0.0.1:11211'],debug=0)
	print 'done'

	setServerConfig(mc)
	setMysqlTables(mc)
	
if __name__ == "__main__":
    main()