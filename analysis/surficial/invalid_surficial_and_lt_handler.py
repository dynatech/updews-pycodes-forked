from datetime import datetime, timedelta
import os
import pandas as pd
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from analysis.publicalerts import round_data_ts
import analysis.querydb as qdb


def main(end_ts=datetime.now()):
    
    start_ts = pd.to_datetime(end_ts) - timedelta(2)
    print(start_ts)
    surficial_triggers = qdb.get_surficial_trigger(start_ts, end_ts)
    
    if len(surficial_triggers) == 0:
        qdb.print_out("No surficial trigger to process")
    
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
                qdb.print_out("Found valid lt surficial trigger for " + \
                              "%s at %s" %(site_code.upper(), ts_updated))
                trigger_sym_id = qdb.get_trigger_sym_id(2, 'surficial')
                df = pd.DataFrame({'ts': [ts_updated], 'site_id': [site_id],
                                   'trigger_sym_id': [trigger_sym_id],
                                   'ts_updated': [ts_updated]})
                qdb.alert_to_db(df, 'operational_triggers', lt_overwrite=False)
                qdb.print_out(" > Added l2 trigger on operational triggers")
        
        # Process only l2 and l3 with alert status of -1 (invalid)
        elif (alert_status == -1): 
            valid_cotriggers = qdb.get_valid_cotriggers(site_id, public_ts_start)            
            dont_delete = False
            # Check if it has co-triggers on start of event
            # tho highly unlikely
            if len(valid_cotriggers) != 0:
                for index, valid in valid_cotriggers.iterrows():
                    # Don't delete public alert entry if there
                    # is a co-trigger that's equal or 
                    # greater of alert level
                    if (valid['alert_level'] >= alert_level):
                        qdb.print_out("%s has valid co-trigger: deleting will NOT commence" %(site_code.upper()))
                        dont_delete = True
                        break
    
            if dont_delete == False:
                qdb.delete_public_alert(site_id, public_ts_start)
                qdb.print_out("Deleted {} public alert of {}".format(public_ts_start, site_code))

    
###############################################################################
    
if __name__ == "__main__":
    start_time = datetime.now()
    qdb.print_out(start_time)

    main()
    
    # test: valit lt of INA
    # pd.to_datetime('2020-05-17 15:00')
    
    qdb.print_out('runtime = %s' %(datetime.now() - start_time))
