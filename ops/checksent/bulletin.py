from datetime import datetime, timedelta
import os
import pandas as pd
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import lib
import dynadb.db as db
import ops.ipr.lib as iprlib


def get_bulletin_sent(start, end):
    query  = "SELECT event_id, timestamp, site_code, narrative FROM narratives "
    query += "INNER JOIN sites USING (site_id) "
    query += "WHERE TIMESTAMP BETWEEN '{}' AND '{}' "
    query += "AND narrative REGEXP 'EWI BULLETIN'"
    query = query.format(start, end)
    bulletin_sent = db.df_read(query, connection='common')
    return bulletin_sent


def get_eq_bulletin():
    query  = "SELECT DISTINCT event_id FROM monitoring_events "
    query += "INNER JOIN monitoring_event_alerts USING (event_id) "
    query += "INNER JOIN monitoring_releases USING (event_alert_id) "
    query += "WHERE trigger_list regexp 'E'"
    eq_bulletin = db.df_read(query, connection='website')
    return eq_bulletin['event_id'].values

def main(time_now=datetime.now()):

    curr_release = iprlib.release_time(time_now) - timedelta(hours=4)
    
    start = curr_release - timedelta(3)
    end = curr_release + timedelta(hours=4)
    
    mysql = True
    
    ewi_sched = lib.get_monitored_sites(curr_release, start, end, mysql=mysql)
    ewi_sched = ewi_sched.loc[ewi_sched.mon_type == 'event', :]

    if len(ewi_sched) != 0:
        df = pd.DataFrame(columns = ['site_code', 'event_id', 'recipient'])
        
        eq_bulletin = get_eq_bulletin()
        eq_mail = ['jeffrey.perez@phivolcs.dost.gov.ph', 'moncada.fatima@gmail.com']
        
        bulletin_sent = get_bulletin_sent(curr_release, end)
        bulletin_sent = pd.merge(ewi_sched, bulletin_sent, how='left', on='site_code')

        df = df.append(bulletin_sent.loc[bulletin_sent.timestamp.isnull(), ['site_code', 'event_id']], ignore_index=True)
        eq_df = bulletin_sent.loc[(bulletin_sent.event_id.isin(eq_bulletin)) & ~(bulletin_sent.narrative.apply(lambda x: any(map(x.__contains__, eq_mail)))), ['site_code']]
        if len(df) != 0:
            df.loc[:, 'recipient'] = 'RUS, ASD'
            df.loc[df.event_id.isin(eq_bulletin), 'recipient'] += ', '.join([''] + eq_mail)
        df = df.append(eq_df, ignore_index=True)
        recipients = ', '.join(eq_mail)
        df = df.fillna(recipients).loc[:, ['site_code', 'recipient']]
        
        lib.send_unsent_notif(df, 'EWI Bulletin', curr_release)
    

if __name__ == '__main__':
    main()