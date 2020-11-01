from datetime import datetime, timedelta, time
import os
import pandas as pd
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import dynadb.db as db
import querydb as qdb
import gsm.gsmserver_dewsl3.sms_data as sms


def release_time(date_time):
    """Rounds time to 4/8/12 AM/PM.

    Args:
        date_time (datetime): Timestamp to be rounded off. 04:00 to 07:30 is
        rounded off to 8:00, 08:00 to 11:30 to 12:00, etc.

    Returns:
        datetime: Timestamp with time rounded off to 4/8/12 AM/PM.

    """

    time_hour = int(date_time.strftime('%H'))

    quotient = int(time_hour / 4)

    if quotient == 5:
        date_time = datetime.combine(date_time.date()+timedelta(1), time(0,0))
    else:
        date_time = datetime.combine(date_time.date(), time((quotient+1)*4,0))
            
    return date_time

def round_data_ts(date_time):
    """Rounds time to HH:00 or HH:30.

    Args:
        date_time (datetime): Timestamp to be rounded off. Rounds to HH:00
        if before HH:30, else rounds to HH:30.

    Returns:
        datetime: Timestamp with time rounded off to HH:00 or HH:30.

    """

    hour = date_time.hour
    minute = date_time.minute

    if minute < 30:
        minute = 0
    else:
        minute = 30

    date_time = datetime.combine(date_time.date(), time(hour, minute))
    
    return date_time

def site_alerts(curr_trig, ts, release_data_ts, connection):
    df = curr_trig.drop_duplicates(['site_id', 'trigger_source', 'alert_level'])
    site_id = df['site_id'].values[0]
    
    query = "SELECT trigger_id, MAX(ts_last_retrigger) ts_last_retrigger FROM alert_status"
    query += " WHERE trigger_id IN (%s)" %(','.join(map(lambda x: str(x), \
                                         set(df['trigger_id'].values))))
    query += " GROUP BY trigger_id"
    written = db.df_read(query, connection=connection)

    site_curr_trig = pd.merge(df, written, how='left')
    site_curr_trig = site_curr_trig.loc[(site_curr_trig.ts_last_retrigger+timedelta(1) < site_curr_trig.ts_updated) | (site_curr_trig.ts_last_retrigger.isnull()), :]
    
    if len(site_curr_trig) == 0:
        qdb.print_out('no new trigger for site_id %s' %site_id)
        return
        
    alert_status = site_curr_trig[['ts_updated', 'trigger_id']]                
    alert_status = alert_status.rename(columns = {'ts_updated': 
            'ts_last_retrigger'})
    alert_status['ts_set'] = datetime.now()
    data_table = sms.DataTable('alert_status', alert_status)
    db.df_write(data_table, connection=connection)

def main(connection='analysis'):
    start_time = datetime.now()
    qdb.print_out(start_time)
    
    ts = round_data_ts(start_time)
    release_data_ts = release_time(ts) - timedelta(hours=0.5)
    
    if qdb.does_table_exist('operational_triggers') == False:
        qdb.create_operational_triggers()
    
    query = "SELECT trigger_id, ts, site_id, trigger_source, "
    query += "alert_level, ts_updated FROM "
    query += "  (SELECT * FROM operational_triggers "
    query += "  WHERE ts <= '%s' " %ts
    query += "  AND ts_updated >= '%s' " %(ts - timedelta(2))
    query += "  ) AS op "
    query += "INNER JOIN " 
    query += "  (SELECT trigger_sym_id, alert_level, trigger_source FROM "
    query += "    (SELECT * FROM operational_trigger_symbols "
    query += "    WHERE alert_level > 0 "
    query += "    ) AS trig_sym "
    query += "  INNER JOIN "
    query += "    (SELECT * FROM trigger_hierarchies WHERE trigger_source not in ('moms', 'on demand')) AS trig "
    query += "  USING (source_id) "
    query += "  ) AS sym "
    query += "USING (trigger_sym_id) "
    query += "ORDER BY ts_updated DESC"
    curr_trig = db.df_read(query, connection=connection)
    
    if len(curr_trig) == 0:
        qdb.print_out('no new trigger')
        return
        
    if not qdb.does_table_exist('alert_status'):
        qdb.create_alert_status()

    site_curr_trig = curr_trig.groupby('site_id', as_index=False)
    site_curr_trig.apply(site_alerts, ts=ts, release_data_ts=release_data_ts, connection=connection)

################################################################################

if __name__ == "__main__":
    main()
