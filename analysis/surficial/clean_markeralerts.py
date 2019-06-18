#### Import essential libraries
import os
import pandas as pd
import sys

#### Import local codes
sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import analysis.querydb as qdb
import dynadb.db as db
import gsm.smsparser2.smsclass as sms


def main():
    query = 'SELECT * FROM markers'
    markers = db.df_read(query)
    query = "SELECT * FROM marker_observations"
    mo = db.df_read(query)
    query = "SELECT * FROM marker_data"
    md = db.df_read(query)
    query = "SELECT ma_id, ts, marker_id FROM marker_alerts"
    ma = db.df_read(query)
    
    marker_alerts = pd.merge(ma, markers, on='marker_id', validate='m:1')
    marker_alerts = pd.merge(marker_alerts, mo, on=['site_id', 'ts'],
                             validate='m:1')
    marker_alerts = pd.merge(marker_alerts, md, on=['mo_id', 'marker_id'],
                             validate='m:1')
    marker_alerts = marker_alerts.drop_duplicates(['ts', 'marker_id'],
                                                  keep='last')
    
    # delete marker_alerts not in marker_observations and duplicated marker_alerts
    ma_id = set(ma['ma_id']) - set(marker_alerts['ma_id'])
    if len(ma_id) != 0:
        query = 'DELETE FROM marker_alerts WHERE ma_id in %s' %str(tuple(ma_id))
        qdb.execute_query(query)
    
    try:
        query = 'ALTER TABLE marker_alerts ADD UNIQUE INDEX uq_marker_alerts (marker_id ASC, ts ASC)'
        qdb.execute_query(query)
    except:
        pass
    
    try:
        query =  "ALTER TABLE marker_alerts "
        query += "ADD UNIQUE INDEX uq_marker_alerts1 (data_id ASC); "
        qdb.execute_query(query)
    except:
        pass
    
    try:
        query =  "ALTER TABLE marker_alerts "
        query += "ADD CONSTRAINT fk_marker_data "
        query += "  FOREIGN KEY (data_id) "
        query += "  REFERENCES marker_data (data_id) "
        query += "  ON DELETE CASCADE "
        query += "  ON UPDATE CASCADE; "
        qdb.execute_query(query)
    except:
        pass
    
    data_table = sms.DataTable('marker_alerts',
                               marker_alerts[['ts', 'marker_id', 'data_id']])
    db.df_write(data_table)
    

if __name__ == "__main__":
    main()