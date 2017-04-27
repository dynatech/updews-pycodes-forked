from datetime import datetime
import numpy as np
import os
from sqlalchemy import create_engine
import sys

#include the path of "Analysis" folder for the python scripts searching
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
    sys.path.insert(1,path)
del path   

import querydb as q

def SiteCoord():
    query = "select site_id, latitude, longitude from loggers"
    df = q.GetDBDataFrame(query)
    df = df.dropna()
    df = df.drop_duplicates('site_id')
    df = df.sort_values('site_id')
    return df
    
def AllRGCoord():
    query = "SELECT * FROM rainfall_gauges where gauge_name not like '%s'" %'mes%'
    df = q.GetDBDataFrame(query)
    return df

def create_rainfall_priorities():
    db, cur = q.SenslopeDBConnect(q.Namedb)
    
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
    
    cur.execute(query)
    db.commit()
    db.close()

def to_MySQL(df, engine):
#    gauge_name = df['gauge_name'].values[0]
    site_id = df['site_id'].values[0]
    try:
        df[['rain_id', 'site_id', 'distance']].to_sql(name = 'rainfall_priorities', con = engine, if_exists = 'append', schema = q.Namedb, index = False)
#        print site_id, '-', gauge_name, ': success'
    except:
        rain_id = df['rain_id'].values[0]
        distance = df['distance'].values[0]
        query = "SELECT * FROM %s WHERE site_id = %s and rain_id = %s" %('rainfall_priorities', site_id, rain_id)
        priority_id = q.GetDBDataFrame(query)['priority_id'].values[0]
        db, cur = q.SenslopeDBConnect(q.Namedb)
        query = "UPDATE %s SET distance = %s WHERE priority_id = %s" %('rainfall_priorities', distance, priority_id)
        cur.execute(query)
        db.commit()
        db.close()
#        print site_id, '-', gauge_name, ': updated'

def Distance(site_coord, rg_coord):
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

def main():
    coord = SiteCoord()
    rg_coord = AllRGCoord()
    site_coord = coord.groupby('site_id')
    nearest_rg = site_coord.apply(Distance, rg_coord=rg_coord)
    nearest_rg['distance'] = np.round(nearest_rg.distance,2)
    
    if q.DoesTableExist('rainfall_priorities') == False:
        #Create a NOAH table if it doesn't exist yet
        create_rainfall_priorities()

    engine = create_engine('mysql://'+q.Userdb+':'+q.Passdb+'@'+q.Hostdb+':3306/'+q.Namedb)
    nearest_rg = nearest_rg.reset_index(drop=True)
    nearest_rg['priority_id'] = range(len(nearest_rg))
    site_nearest_rg = nearest_rg.groupby('priority_id')
    site_nearest_rg.apply(to_MySQL, engine=engine)

    return nearest_rg
    
if __name__ == "__main__":
    start = datetime.now()
    
    main()

    print 'runtime =', datetime.now() - start