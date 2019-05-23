from datetime import timedelta
import os
import pandas as pd
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import querydb as qdb

def create_uptime():

    query =  "CREATE TABLE `uptime` ( "
    query += "  `ts` TIMESTAMP, "
    query += "  `site_count` TINYINT(2) UNSIGNED NOT NULL, "
    query += "  `ts_updated` TIMESTAMP NULL, "
    query += "  PRIMARY KEY (`ts`))"
                                     
    qdb.execute_query(query)
    
def alerts_df():

    output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
    sc = qdb.memcached()
    file_path = output_path+sc['fileio']['output_path']
    
    df = pd.read_json(file_path + 'PublicAlertRefDB.json')
    alerts = pd.DataFrame(df['alerts'].values[0])
    alerts['internal_alert'] = alerts['internal_alert'].apply(lambda x: x.lower().replace('a0', ''))
    alerts['up'] =  ~(alerts['internal_alert'].str.contains('nd')|alerts['internal_alert'].str.contains('0'))
    alerts = alerts[['ts', 'site_id', 'internal_alert', 'up']]
    
    return alerts

def to_db(df):
    if not qdb.does_table_exist('uptime'):
        create_uptime()
    
    ts = pd.to_datetime(df['ts'].values[0])
    
    query =  "SELECT * FROM uptime "
    query += "WHERE (ts <= '%s' " %ts
    query += "  AND ts_updated >= '%s') " %ts
    query += "OR (ts_updated >= '%s' " %(ts - timedelta(hours=0.5))
    query += "  AND ts_updated <= '%s') " %ts
    query += "ORDER BY ts DESC LIMIT 1"
    prev_uptime = qdb.get_db_dataframe(query)

    if len(prev_uptime) == 0 or prev_uptime['site_count'].values[0] != df['site_count'].values[0]:
        qdb.push_db_dataframe(df, 'uptime', index=False)
    elif pd.to_datetime(prev_uptime['ts_updated'].values[0]) < df['ts_updated'].values[0]:
        query =  "UPDATE uptime "
        query += "SET ts_updated = '%s' " %pd.to_datetime(df['ts_updated'].values[0])
        query += "WHERE ts = %s" %prev_uptime['ts'].values[0]
        qdb.execute_query(query)

def main():
    
    alerts = alerts_df()
    up_count = len(alerts[alerts.up == True])
    ts = pd.to_datetime(max(alerts['ts'].values))
    df = pd.DataFrame({'ts': [ts], 'site_count': [up_count], 'ts_updated': [ts]})
    to_db(df)

###############################################################################

if __name__ == "__main__":
    main()
