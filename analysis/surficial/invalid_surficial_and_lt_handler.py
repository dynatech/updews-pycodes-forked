from datetime import datetime, timedelta
import pandas as pd
import analysis.querydb as qdb
from publicalerts import round_data_ts

def insert_l2_operational_trigger(ts, site_id):
    query = "INSERT INTO operational_triggers "
    query += "(ts, site_id, trigger_sym_id, ts_updated) "
    query += "VALUES ('%s', %s, 8, '%s')" %(ts, site_id, ts)
    result = qdb.execute_query(query)
    
    return result

def update_ts_last_site_public_alert(site_id, ts_updated):
    query = "UPDATE public_alerts "
    query += "SET "
    query += " ts_updated = '%s' " %(ts_updated)
    query += "WHERE "
    query += "  site_id = %s " %(site_id)
    query += "ORDER BY ts_updated DESC "
    query += "LIMIT 1"
    result = qdb.execute_query(query)
    
    return result

def delete_invalid_public_alert_entry(site_id, public_ts_start):
#    query = "SELECT "
#    query += "  * "
    query = "DELETE "
    query += "FROM public_alerts "
    query += "WHERE "
    query += "  ts = '%s' " %(public_ts_start)
    query += "  AND site_id = %s " %(site_id)
#    query += "  AND pub_sym_id = %s " %(pub_sym_id)
    result = qdb.execute_query(query)
    
    return result

def get_valid_cotriggers(site_id, public_ts_start):
    query =  "SELECT "
    query += "  ot.trigger_id, ot.ts, ot.site_id, "
    query += "  ot.ts_updated, ot.trigger_sym_id, ots.alert_symbol, "
    query += "  ots.alert_level "
    query += "FROM operational_triggers AS ot "
    query += "JOIN operational_trigger_symbols AS ots"
    query += "  ON ot.trigger_sym_id = ots.trigger_sym_id "
    query += "JOIN alert_status AS als"
    query += "  ON ot.trigger_id = als.trigger_id "
    query += "WHERE "
    query += "  ts = '%s' " %(public_ts_start) 
    query += "  AND ot.site_id = %s " %(site_id)
    query += "  AND als.alert_status = 1 " #change this to 0 for validating
    query += "ORDER BY ts DESC"
    result = qdb.get_db_dataframe(query)
    
    return result

def get_surficial_trigger(start_ts, end_ts):
    query =  "SELECT "
    query += "  ot.trigger_id, ot.ts, ot.site_id, als.alert_status, "
    query += "  ot.ts_updated, ot.trigger_sym_id, ots.alert_symbol, "
    query += "  ots.alert_level, pas.pub_sym_id, sites.site_code "
    query += "FROM operational_triggers AS ot "
    query += "JOIN operational_trigger_symbols AS ots"
    query += "  ON ot.trigger_sym_id = ots.trigger_sym_id "
    query += "JOIN public_alert_symbols AS pas"
    query += "  ON ots.alert_level = pas.alert_level "
    query += "JOIN alert_status AS als"
    query += "  ON ot.trigger_id = als.trigger_id "
    query += "JOIN sites "
    query += "  ON ot.site_id = sites.site_id "
    query += "WHERE "
    query += "  (ts >= '%s' AND ts_updated <= '%s') " %(start_ts, end_ts) 
    query += "  AND source_id = 2 "
    #query += "  AND als.alert_status = -1 " #change this to 0 for validating
    query += "ORDER BY ts DESC"  
    result = qdb.get_db_dataframe(query)
        
    return result

def main(end_ts=datetime.now()):
    start_time = datetime.now()
    qdb.print_out(start_time)
    
    start_ts = pd.to_datetime(end_ts) - timedelta(1)

    surficial_triggers = get_surficial_trigger(start_ts, end_ts)
    
    if len(surficial_triggers) == 0:
        qdb.print_out("=================");
        qdb.print_out("No surficial trigger (lt, l2, l3) to process")
    
    for index, surficial in surficial_triggers.iterrows():
        ts_updated = surficial['ts_updated']
        public_ts_start = round_data_ts(ts_updated)
        alert_level = surficial['alert_level']
        alert_symbol = surficial['alert_symbol']
        alert_status = surficial['alert_status']
        site_id = surficial['site_id']
        site_code = surficial['site_code']
        
        if (alert_symbol == 'lt'):
            if (alert_status == 1):
                qdb.print_out("=================");
                qdb.print_out("Found valid lt surficial trigger for " + \
                              "%s at %s" %(site_code.upper(), ts_updated))
                qdb.print_out(" > Added l2 trigger on operational triggers")
                insert_l2_operational_trigger(ts_updated, site_id)
        
        # Process only l2 and l3 with alert status of -1 (invalid)
        elif (alert_status == -1): 
            valid_cotriggers = get_valid_cotriggers(site_id, public_ts_start)            
            dont_delete = False
            # Check if it has co-triggers on start of event
            # tho highly unlikely
            if len(valid_cotriggers) != 0:
                for index, valid in valid_cotriggers.iterrows():
                    # Don't delete public alert entry if there
                    # is a co-trigger that's equal or 
                    # greater of alert level
                    if (valid['alert_level'] >= alert_level):
                        qdb.print_out("=================");
                        qdb.print_out("%s has valid co-trigger: deleting will NOT commence" %(site_code.upper()))
                        dont_delete = True
                        break
    
            if dont_delete == False:
                qdb.print_out("=================");
                qdb.print_out("Deleting public alert for site " + \
                              "%s (%s) at %s" %(site_code.upper(), site_id, public_ts_start))
                public_alert = delete_invalid_public_alert_entry(site_id, public_ts_start)
                
                # update ts_updated of latest entry for that site to current time
                # using round_data_ts(datetime.now())
                update_ts_last_site_public_alert(site_id, round_data_ts(datetime.now()))
    
    qdb.print_out("=================");
    qdb.print_out('runtime = %s' %(datetime.now() - start_time))
    
################################################################################
    
if __name__ == "__main__":
    df = main()
    #df = main("2018-09-19 17:00:00")
