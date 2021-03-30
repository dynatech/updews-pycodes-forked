from datetime import datetime, timedelta
import os
import pandas as pd
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import lib
import dynadb.db as db
import ops.ewisms_meal as ewisms


def get_bulletin_sent(start, end):
    query  = "SELECT timestamp, site_code, narrative FROM narratives "
    query += "INNER JOIN sites USING (site_id) "
    query += "WHERE TIMESTAMP BETWEEN '{}' AND '{}' "
    query += "AND narrative REGEXP 'EWI BULLETIN'"
    query = query.format(start, end)
    bulletin_sent = db.df_read(query, connection='common')
    return bulletin_sent


def main(time_now=datetime.now()):

    curr_release = ewisms.release_time(time_now) - timedelta(hours=4)
    
    start = curr_release - timedelta(3)
    end = curr_release + timedelta(hours=4)
    
    mysql = True
    print(curr_release)
    ewi_sched = lib.get_monitored_sites(curr_release, start, end, mysql=mysql)
    ewi_sched = ewi_sched.loc[ewi_sched.mon_type == 'event', :]
    
    if len(ewi_sched) != 0:
        bulletin_sent = get_bulletin_sent(curr_release, end)
        bulletin_sent = pd.merge(ewi_sched, bulletin_sent, how='left', on='site_code')
    
        df = bulletin_sent.loc[bulletin_sent.timestamp.isnull(), ['site_code']]
        
        lib.send_unsent_notif(df, curr_release)
    

if __name__ == '__main__':
    main()