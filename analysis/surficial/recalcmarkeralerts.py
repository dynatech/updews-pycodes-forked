# -*- coding: utf-8 -*-

from datetime import datetime
import os
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import dynadb.db as db
import markeralerts as ma


def get_surf(connection='analysis'):
    query  = "SELECT site_id, ts FROM "
    query += "  (SELECT * FROM marker_alerts "
    query += "  WHERE processed = 0 "
    query += "  ) ma "
    query += "INNER JOIN "
    query += "  markers "
    query += "USING (marker_id) "
    query += "GROUP BY ts, site_id "
    query += "ORDER BY ts"
    df = db.df_read(query, connection=connection)
    return df


def proc_surf(df):
    site_id = df.site_id.values[0]
    for ts in df.ts:
        ma.generate_surficial_alert(site_id=site_id, ts=ts)
        

def main():
    surf = get_surf()
    site_surf = surf.groupby('site_id', as_index=False)
    site_surf.apply(proc_surf)
    

###############################################################################
if __name__ == '__main__':
    start = datetime.now()
    
    main()
    
    print ('runtime =', datetime.now()-start)
