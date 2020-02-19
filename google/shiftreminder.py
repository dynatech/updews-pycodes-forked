# -*- coding: utf-8 -*-
"""
Created on Tue Feb 18 07:46:51 2020

@author: Meryll
"""

from datetime import datetime, time, timedelta
import pandas as pd

import dynadb.db as db
import gsm.smsparser2.smsclass as sms


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
    query =  "SELECT mobile_id, nickname, gsm_id FROM commons_db.users "
    query += "inner join commons_db.user_accounts ua "
    query += "on ua.user_fk_id = users.user_id "
    query += "inner join commons_db.user_team_members "
    query += "using (user_id) "
    query += "inner join commons_db.user_teams "
    query += "using (team_id) "
    query += "inner join comms_db.user_mobile "
    query += "using (user_id) "
    query += "where status = 1 "
    query += "and team_code in ('admin', 'CT', 'MT') "
    query += "order by last_name"
    df = db.df_read(query)
    return df

def send_reminder(ts = datetime.now()):
    shift_ts = release_time(ts)+timedelta(0.5)
    
    query = """SELECT * FROM monshiftsched
            WHERE ts = '{}'""".format(shift_ts)
    df = db.df_read(query)
    df = df.rename(columns={'iompmt': 'MT', 'iompct': 'CT'})
    
    sched = shift_ts.strftime("%B %d, %Y %I:%M%p")
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
        try:
            mobile_id = IOMP_num.loc[IOMP_num.nickname == name, 'mobile_id'].values
            gsm_id = IOMP_num.loc[IOMP_num.nickname == name, 'gsm_id'].values
        except:
            print("No mobile number")
            continue
        data_table = sms.DataTable('smsoutbox_users', outbox)
        outbox_id = db.df_write(data_table, resource='sms_data', last_insert=True)[0][0]
        status = pd.DataFrame({'outbox_id': [outbox_id]*len(mobile_id), 'mobile_id': mobile_id,
                               'gsm_id': gsm_id})
        data_table = sms.DataTable('smsoutbox_user_status', status)
        db.df_write(data_table, resource='sms_data')
        
if __name__ == "__main__":
    send_reminder()
