from datetime import datetime
import numpy as np
import os
import pandas as pd
import requests
import sys

#include the path of "Analysis" folder for the python scripts searching
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
    sys.path.insert(1,path)
del path   

import querydb as qdb

def create_rainfall_gauges():    
    
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

    qdb.execute_query(query)
    
def senslope_rain_gauges():
    query = "SELECT l.logger_name, l.latitude, l.longitude, l.date_activated, l.date_deactivated"
    query += " FROM loggers as l left join logger_models as m"
    query += " on l.model_id = m.model_id where has_rain = 1"
    query += " and not (latitude is null or longitude is null)"
    query += " order by logger_name"
    df = qdb.get_db_dataframe(query)
    df['data_source'] = 'senslope'
    df = df.rename(columns = {'logger_name': 'gauge_name'})
    return df

def to_mysql(df):
    gauge_name = df['gauge_name'].values[0]
    query = "SELECT EXISTS(SELECT * FROM rainfall_gauges"
    query += " WHERE gauge_name = '%s')" %gauge_name
    if qdb.get_db_dataframe(query).values[0][0] == 0:
        qdb.push_db_dataframe(df, 'rainfall_gauges', index=False)
    else:
        query = "SELECT * FROM rainfall_gauges WHERE gauge_name = '%s'" %gauge_name
        rain_id = qdb.get_db_dataframe(query)['rain_id'].values[0]
        query = "UPDATE rainfall_gauges SET latitude = %s, longitude = %s," %(df['latitude'].values[0], df['longitude'].values[0])
        query += " date_activated = '%s'" %df['date_activated'].values[0]
        try:
            if not np.isnan(df['date_deactivated'].values[0]):
                query += ", date_deactivated = '%s'" %df['date_deactivated'].values[0]
        except:
            pass
        query += " WHERE rain_id = %s" %rain_id
        qdb.execute_query(query)

def main():
    start = datetime.now()
    qdb.print_out(start)

    if qdb.does_table_exist('rainfall_gauges') == False:
        #Create a rainfall_gauges table if it doesn't exist yet
        create_rainfall_gauges()
        senslope = senslope_rain_gauges()
        
        rain_id = senslope.groupby('gauge_name')
        rain_id.apply(to_mysql)

    r = requests.get('http://weather.asti.dost.gov.ph/web-api/index.php/api/devices', auth=('phivolcs.ggrdd', 'PhiVolcs0117'))    
    noah = pd.DataFrame(r.json())
    noah = noah[noah['sensor_name'].str.contains('rain', case = False)]
    noah = noah.dropna()
    noah['dev_id'] = noah['dev_id'].apply(lambda x: int(x))
    noah['longitude'] = noah['longitude'].apply(lambda x: np.round(float(x),6))
    noah['latitude'] = noah['latitude'].apply(lambda x: np.round(float(x),6))
    noah = noah.loc[(noah.longitude != 0) & (noah.latitude != 0)]
    noah = noah.rename(columns = {'dev_id': 'gauge_name', 'date_installed': 'date_activated'})
    noah['data_source'] = 'noah'
    noah['date_deactivated'] = np.nan
    noah = noah[['gauge_name', 'data_source', 'longitude', 'latitude', 'date_activated', 'date_deactivated']]
    
    rain_id = noah.groupby('gauge_name')
    rain_id.apply(to_mysql)
    
    qdb.print_out('runtime = %s' %(datetime.now() - start))

################################################################################
if __name__ == "__main__":
    main()