from datetime import datetime
import numpy as np
import os
import pandas as pd
import sys

#include the path of "Analysis" folder for the python scripts searching
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
    sys.path.insert(1,path)
del path   

import querydb as qdb

def all_site_coord():
    query = "select site_id, latitude, longitude from loggers"
    df = qdb.get_db_dataframe(query)
    df = df.dropna()
    df = df.drop_duplicates('site_id')
    df = df.sort_values('site_id')
    return df
    
def all_rg_coord():
    query =  "SELECT * FROM rainfall_gauges "
    query += "WHERE (date_deactivated >= '%s' " %(datetime.now())
    query += "OR date_deactivated IS NULL)"
    df = qdb.get_db_dataframe(query)
    return df

def create_rainfall_priorities():
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
    
    qdb.execute_query(query)

def to_mysql(df):
    site_id = df['site_id'].values[0]
    rain_id = df['rain_id'].values[0]
    query = "SELECT EXISTS(SELECT * FROM rainfall_priorities"
    query += " WHERE site_id = %s AND rain_id = %s)" %(site_id, rain_id)
    if qdb.get_db_dataframe(query).values[0][0] == 0:
        qdb.push_db_dataframe(df[['rain_id', 'site_id', 'distance']], 'rainfall_priorities', index=False)
    else:
        distance = df['distance'].values[0]
        query = "SELECT * FROM %s WHERE site_id = %s and rain_id = %s" %('rainfall_priorities', site_id, rain_id)
        priority_id = qdb.get_db_dataframe(query)['priority_id'].values[0]
        query = "UPDATE %s SET distance = %s WHERE priority_id = %s" %('rainfall_priorities', distance, priority_id)
        qdb.execute_query(query)

def get_distance(site_coord, rg_coord):
    site_id = site_coord['site_id'].values[0]
    site_lat = site_coord['latitude'].values[0]
    site_lon = site_coord['longitude'].values[0]

    rg_coord['dlat'] = rg_coord['latitude'].apply(lambda x: float(x) - site_lat)
    rg_coord['dlon'] = rg_coord['longitude'].apply(lambda x: float(x) - site_lon)
    rg_coord['dlat'] = np.radians(rg_coord.dlat)
    rg_coord['dlon'] = np.radians(rg_coord.dlon)

    rg_coord['a1'] = rg_coord['dlat'].apply(lambda x: np.sin(x/2)**2)
    rg_coord['a3'] = rg_coord['latitude'].apply(lambda x: np.cos(np.radians(float(x))))
    rg_coord['a4'] = rg_coord['dlon'].apply(lambda x: np.sin(x/2)**2)
    
    rg_coord['a'] = rg_coord['a1'] + (np.cos(np.radians(site_lat)) * rg_coord['a3'] * rg_coord['a4'])
    rg_coord['c']= 2 * np.arctan2(np.sqrt(rg_coord.a),np.sqrt(1-rg_coord.a))
    rg_coord['distance']= 6371 * rg_coord.c
    rg_coord = rg_coord.sort_values('distance', ascending = True)
    
    nearest_rg = rg_coord[['rain_id', 'gauge_name', 'distance']]
    nearest_rg = nearest_rg[0:4]
    nearest_rg['site_id'] = site_id
    
    return nearest_rg

def main(end=datetime.now()):
    start = datetime.now()
    qdb.print_out(start)
    
    end = pd.to_datetime(end)

    coord = all_site_coord()
    rg_coord = all_rg_coord(end)  
    site_coord = coord.groupby('site_id')
    nearest_rg = site_coord.apply(get_distance, rg_coord=rg_coord)
    nearest_rg['distance'] = np.round(nearest_rg.distance,2)
    
    if qdb.does_table_exist('rainfall_priorities') == False:
        #Create a NOAH table if it doesn't exist yet
        create_rainfall_priorities()

    nearest_rg = nearest_rg.reset_index(drop=True)
    nearest_rg['priority_id'] = range(len(nearest_rg))
    site_nearest_rg = nearest_rg.groupby('priority_id')
    site_nearest_rg.apply(to_mysql)
    
    qdb.print_out('runtime = %s' %(datetime.now() - start))

    return nearest_rg
    
if __name__ == "__main__":
    main()