from datetime import datetime
import numpy as np
import os
import pandas as pd
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import dynadb.db as dynadb
import gsm.smsparser2.smsclass as sms


def get_radius(km):
    return float(np.rad2deg(km/6371.))    

def get_crit_dist(mag):
    return (29.027 * (mag**2)) - (251.89*mag) + 547.97

def get_distance_to_eq(df, eq_lat, eq_lon):
    lon = df['longitude'].values[0]
    lat = df['latitude'].values[0]
    
    dlon = eq_lon - lon
    dlat = eq_lat - lat
    dlon = np.radians(dlon)
    dlat = np.radians(dlat)
    
    a = (np.sin(dlat/2))**2 + ( np.cos(np.radians(eq_lat)) * np.cos(np.radians(lat)) * (np.sin(dlon/2))**2 )
    c = 2 * np.arctan2(np.sqrt(a),np.sqrt(1-a))
    d = 6371 * c
    df.loc[:, 'distance'] = d    
    return df

def get_unprocessed():
    query = "select * from earthquake_events where processed = 0"
    df = dynadb.df_read(query=query, resource="common_data")
    df = df.set_index('eq_id')
    return df

def get_sites():
    query = ("SELECT s.site_id, site_code, latitude, longitude FROM "
        "loggers as l left join sites as s on s.site_id = l.site_id ")
    df = dynadb.df_read(query=query, resource="sensor_data")
    df = df.drop_duplicates('site_id',keep='last').dropna()
    return df
    
def get_alert_symbol():
    query =  "SELECT trigger_sym_id FROM "
    query += "  (SELECT * FROM operational_trigger_symbols "
    query += "  WHERE alert_level = 1 "
    query += "  ) AS op "
    query += "INNER JOIN "
    query += "  (SELECT source_id FROM trigger_hierarchies "
    query += "  WHERE trigger_source = 'earthquake' "
    query += "  ) AS trig "
    query += "USING (source_id)"
    df = dynadb.df_read(query=query, resource="sensor_data")
    return df['trigger_sym_id'].values[0]

def create_table():
    query = ''
    query += 'CREATE TABLE `senslopedb`.`earthquake_alerts` ('
    query += '`ea_id` SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,'
    query += '`eq_id` INT(10) UNSIGNED NOT NULL,'
    query += '`site_id` TINYINT(3) UNSIGNED NOT NULL,'
    query += '`distance` DECIMAL(5,3) NULL,'
    query += 'PRIMARY KEY (`ea_id`),'
    query += 'UNIQUE INDEX `uq_earthquake_alerts` (`eq_id` ASC,`site_id` ASC),'
    query += 'INDEX `fk_earthquake_alerts_sites_idx` (`site_id` ASC),'
    query += 'INDEX `fk_earthquake_alerts_earthquake_events_idx` (`eq_id` ASC),'
    query += 'CONSTRAINT `fk_earthquake_alerts_sites`'
    query += '  FOREIGN KEY (`site_id`)'
    query += '  REFERENCES `senslopedb`.`sites` (`site_id`)'
    query += '  ON DELETE NO ACTION'
    query += '  ON UPDATE CASCADE,'
    query += 'CONSTRAINT `fk_earthquake_alerts_earthquake_events`'
    query += '  FOREIGN KEY (`eq_id`)'
    query += '  REFERENCES `senslopedb`.`earthquake_events` (`eq_id`)'
    query += ' ON DELETE NO ACTION'
    query += ' ON UPDATE CASCADE);'
    return query




    
############################ MAIN ############################

def main():

    eq_events = get_unprocessed()
    sym = get_alert_symbol()
    sites = get_sites()
    dfg = sites.groupby('site_id')
    eq_a = pd.DataFrame(columns=['site_id','eq_id','distance'])
    EVENTS_TABLE = 'earthquake_events'

    for i in eq_events.index:
        cur = eq_events.loc[i]
        
        mag = cur.magnitude
        eq_lat = cur.latitude
        eq_lon = cur.longitude
        ts = cur.ts
           
        critdist = get_crit_dist(mag)
    
        if False in np.isfinite([mag,eq_lat,eq_lon]): #has NaN value in mag, lat, or lon 
            query = "UPDATE %s SET processed = -1 where eq_id = %s" % (EVENTS_TABLE, i)
            dynadb.write(query=query, resource="sensor_data")
            continue
         
        if mag < 4:
            print ("> Magnitude too small: %d" % (mag))
            query = "UPDATE %s SET processed = 1 where eq_id = %s" % (EVENTS_TABLE,i)
            dynadb.write(query=query, resource="sensor_data")
            continue
        else:
            print ("> Magnitude reached threshold: %d" % (mag))

        # magnitude is big enough to consider
        sites = dfg.apply(get_distance_to_eq,eq_lat=eq_lat,eq_lon=eq_lon)

        crits = sites.loc[sites.distance <= critdist, :]

        if len(crits) == 0: 
            print ("> No affected sites. ")
            query = "UPDATE %s SET processed = 1, critical_distance = %s where eq_id = %s" % (EVENTS_TABLE,critdist,i)
            dynadb.write(query=query, resource="sensor_data")
            continue
        else:
            #merong may trigger
            print (">> Possible sites affected: %d" % (len(crits.site_id.values)))

        crits.loc[:, 'ts']  = ts
        crits.loc[:, 'source'] = 'earthquake'
        crits.loc[:, 'trigger_sym_id'] = sym
        crits.loc[:, 'ts_updated'] = ts       
        crits.loc[:, 'eq_id'] = i

        eq_a = crits.loc[:, ['eq_id','site_id','distance']]
        op_trig = crits.loc[:, ['ts','site_id','trigger_sym_id','ts_updated']]

        # write to tables
        data_table = sms.DataTable("operational_triggers", op_trig)
        dynadb.df_write(data_table)
        data_table = sms.DataTable("earthquake_alerts", eq_a)
        dynadb.df_write(data_table)
        
        query = "UPDATE %s SET processed = 1, critical_distance = %s where eq_id = %s " % (EVENTS_TABLE,critdist,i)
        dynadb.write(query=query, resource="sensor_data")

        print (">> Alert iniated.\n")
        
                    
if __name__ == "__main__":
    main()
    print (datetime.now())
