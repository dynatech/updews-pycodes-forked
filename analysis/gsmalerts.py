from datetime import datetime, timedelta
import pandas as pd

import querydb as qdb
import publicalerts as pub

def site_alerts(curr_trig, ts, release_data_ts):
    site_id = curr_trig['site_id'].values[0]
    
    query = "SELECT site_id, stat.trigger_id, trigger_source, alert_level FROM"
    query += "  (SELECT * FROM alert_status"
    query += "  WHERE ts_last_retrigger >= '%s') as stat" %(ts - timedelta(1))
    query += "    LEFT JOIN"
    query += "  (SELECT trigger_id, site_id, trigger_source, alert_level FROM"
    query += "    (SELECT * FROM operational_triggers"
    query += "     WHERE site_id = %s) as op" %site_id
    query += "      INNER JOIN"
    query += "    operational_trigger_symbols as sym) as sub"
    query += "  ON stat.trigger_id = sub.trigger_id"
    sent_alert = qdb.get_db_dataframe(query)        

    site_curr_trig = curr_trig[~curr_trig.trigger_id.isin(sent_alert.trigger_id)]
    site_curr_trig = site_curr_trig.sort_values('alert_level', ascending=False)
    site_curr_trig = site_curr_trig.drop_duplicates('trigger_source')

    if len(site_curr_trig) == 0:
        return

    if len(sent_alert) == 0:
        pass
    elif max(site_curr_trig.alert_level) <= max(sent_alert.alert_level):
        if max(sent_alert.alert_level) > 1 or \
                    (max(site_curr_trig.alert_level) == 1 and \
                    'surficial' not in site_curr_trig['trigger_source'].values):
            qdb.print_out('no higher trigger')
            return
        site_curr_trig = site_curr_trig[site_curr_trig.trigger_source == 'surficial']
    else:
        site_curr_trig = site_curr_trig[site_curr_trig.alert_level >
                max(sent_alert.alert_level)]
        
    alert_status = site_curr_trig[['ts_updated', 'trigger_id']]                
    alert_status = alert_status.rename(columns = {'ts_updated': 
            'ts_last_retrigger'})
    qdb.push_db_dataframe(alert_status, 'alert_status', index=False)

def main():
    start_time = datetime.now()
    qdb.print_out(start_time)
    
    ts = pub.data_ts(start_time)
    release_data_ts = pub.release_time(ts) - timedelta(hours=0.5)
    
    try:
        query = "SELECT trigger_id, ts, site_id, trigger_source, "
        query += "alert_level, ts_updated FROM"
        query += "  (SELECT * FROM operational_triggers"
        query += "  WHERE ts <= '%s'" %ts
        query += "  AND ts_updated >= '%s') AS op" %(ts - timedelta(1))
        query += "    INNER JOIN"
        query += "  (SELECT * FROM operational_trigger_symbols"
        query += "  WHERE alert_level > 0) AS sym"
        query += "    ON op.trigger_sym_id = sym.trigger_sym_id "
        query += "ORDER BY ts_updated DESC"
        curr_trig = qdb.get_db_dataframe(query)
    except:
        curr_trig = pd.DataFrame()
        qdb.create_operational_triggers()
        
    if len(curr_trig) == 0:
        qdb.print_out('no new trigger')
        return
        
    if not qdb.does_table_exist('alert_status'):
        qdb.create_alert_status()

    site_curr_trig = curr_trig.groupby('site_id', as_index=False)
    site_curr_trig.apply(site_alerts, ts=ts, release_data_ts=release_data_ts)

################################################################################

if __name__ == "__main__":
    main()