# -*- coding: utf-8 -*-
"""
Created on Tue Feb 18 07:46:51 2020

@author: Meryll
"""

from datetime import datetime, time, timedelta
import pandas as pd

import dynadb.db as db
import gsm.smsparser2.smsclass as sms
import volatile.memory as mem


def release_time(date_time):
    """Rounds time to 8 AM/PM.

    Args:
        date_time (datetime): Timestamp to be rounded off.

    Returns:
        datetime: Timestamp with time rounded off to 8 AM/PM.

    """

    hour = date_time.hour

    if hour <= 8:
        date_time = datetime.combine(date_time.date(), time(8,0))
    else:
        date_time = datetime.combine(date_time.date(), time(20,0))
            
    return date_time

def get_mobile():
    conn = mem.get('DICT_DB_CONNECTIONS')
    query  = "SELECT mobile_id, nickname, gsm_id FROM "
    query += "  (SELECT user_id, nickname FROM {}.users ".format(conn['common']['schema'])
    query += "  WHERE status = 1) u "
    query += "INNER JOIN "
    query += "  (SELECT user_id, team_code FROM "
    query += "  {}.user_team_members ".format(conn['common']['schema'])
    query += "  INNER JOIN {}.user_teams ".format(conn['common']['schema'])
    query += "  USING (team_id) "
    query += "  WHERE team_code IN ('admin', 'CT', 'MT') "
    query += "  ) team "
    query += "USING (user_id) "
    query += "INNER JOIN {}.user_mobiles ".format(conn['gsm_pi']['schema'])
    query += "USING (user_id) "
    query += "INNER JOIN {}.mobile_numbers ".format(conn['gsm_pi']['schema'])
    query += "USING (mobile_id)"
    df = db.df_read(query, resource='sms_analysis')
    return df

def send_reminder(ts = datetime.now()):
    shift_ts = release_time(ts)+timedelta(0.5)
    
    query = """SELECT * FROM monshiftsched
            WHERE ts = '{}'""".format(shift_ts)
    df = db.df_read(query)
    df = df.rename(columns={'iompmt': 'MT', 'iompct': 'CT'})
    
    sched = (shift_ts-timedelta(hours=0.25)).strftime("%B %d, %Y %I:%M%p")
    greeting = ts.strftime("%p")
    if greeting == 'AM':
        greeting = 'morning'
    else:
        greeting = 'evening'
    
    IOMP_dict = df.loc[:, ['MT', 'CT']].to_dict(orient='records')[0]
    IOMP_num = get_mobile()
    for IOMP, name in IOMP_dict.items():
        sms_msg = ("Monitoring shift reminder.\n"
                "Good {} {}, you are assigned to be the IOMP-{} "
                "for {}").format(greeting, name, IOMP, sched)
        print(sms_msg, '\n')
        outbox = pd.DataFrame({'sms_msg': [sms_msg], 'source': ['central']})
        mobile_id = IOMP_num.loc[IOMP_num.nickname == name, 'mobile_id'].values
        gsm_id = IOMP_num.loc[IOMP_num.nickname == name, 'gsm_id'].values
        if len(mobile_id) != 0 and len(gsm_id) != 0:
            data_table = sms.DataTable('smsoutbox_users', outbox)
            outbox_id = db.df_write(data_table, resource='sms_data', last_insert=True)[0][0]
            status = pd.DataFrame({'outbox_id': [outbox_id]*len(mobile_id), 'mobile_id': mobile_id,
                                   'gsm_id': gsm_id})
            data_table = sms.DataTable('smsoutbox_user_status', status)
            db.df_write(data_table, resource='sms_data')
        else:
            print("No mobile number")
        
if __name__ == "__main__":
    send_reminder()
