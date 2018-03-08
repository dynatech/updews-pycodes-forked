# -*- coding: utf-8 -*-
"""
Created on Wed Dec 06 09:19:03 2017

@author: Meryll
"""

from datetime import timedelta
import pandas as pd

import filepath
import querySenslopeDb as qdb

def create_uptime():

    query =  "CREATE TABLE `uptime` ( "
    query += "  `ts` TIMESTAMP, "
    query += "  `site_count` TINYINT(2) UNSIGNED NOT NULL, "
    query += "  `ts_updated` TIMESTAMP NULL, "
    query += "  PRIMARY KEY (`ts`))"
                                     
    qdb.ExecuteQuery(query)
    
def alerts_df():
    
    file_path = filepath.output_file_path('all', 'public')['monitoring_output']
    df = pd.read_json(file_path + 'PublicAlert.json')
    alerts = pd.DataFrame(df['alerts'].values[0])
    alerts['internal_alert'] = alerts['internal_alert'].apply(lambda x: x.lower().replace('a0', ''))
    alerts['up'] =  ~(alerts['internal_alert'].str.contains('nd')|alerts['internal_alert'].str.contains('0'))
    alerts = alerts[['timestamp', 'site', 'internal_alert', 'up']]
    
    return alerts

def to_db(df):
    
    if not qdb.DoesTableExist('uptime'):
        create_uptime()
    
    ts = pd.to_datetime(df['ts'].values[0])
    
    query =  "SELECT * FROM uptime "
    query += "WHERE (ts <= '%s' " %ts
    query += "  AND ts_updated >= '%s') " %ts
    query += "OR (ts_updated >= '%s' " %(ts - timedelta(hours=0.5))
    query += "  AND ts_updated <= '%s') " %ts
    query += "ORDER BY ts DESC LIMIT 1"
    prev_uptime = qdb.GetDBDataFrame(query)

    if len(prev_uptime) == 0 or prev_uptime['site_count'].values[0] != df['site_count'].values[0]:
        qdb.PushDBDataFrame(df, 'uptime', index=False)
    elif pd.to_datetime(prev_uptime['ts_updated'].values[0]) < df['ts_updated'].values[0]:
        query =  "UPDATE uptime "
        query += "SET updateTS = '%s' " %df['ts_updated'].values[0]
        query += "WHERE ts = '%s'" %prev_uptime['ts'].values[0]
        qdb.ExecuteQuery(query)

def main():
    
    alerts = alerts_df()
    up_count = len(alerts[alerts.up == True])
    ts = pd.to_datetime(max(alerts['timestamp'].values))
    df = pd.DataFrame({'ts': [ts], 'site_count': [up_count], 'ts_updated': [ts]})
    to_db(df) 

###############################################################################

if __name__ == "__main__":
    main()