from datetime import datetime, timedelta
import os
import pandas as pd
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import dynadb.db as db
import gsm.smsparser2.smsclass as sms
import ops.ewisms_meal as ewisms


def get_monitored_sites(curr_release, start, end, mysql=True):
    ewi_sched = pd.DataFrame()
    event = ewisms.get_events(start, end, mysql=mysql)
    after_ins_sites = sorted(event.loc[(event.ts_start >= curr_release-timedelta(hours=4)), 'site_code'])
    ewi_sched = ewi_sched.append(pd.DataFrame({'site_code': after_ins_sites, 'mon_type': ['event']*len(after_ins_sites), 'ins': [1]*len(after_ins_sites)}))
    event_sites = sorted(set(event.loc[event.validity >= curr_release, 'site_code']) - set(after_ins_sites))
    ewi_sched = ewi_sched.append(pd.DataFrame({'site_code': event_sites, 'mon_type': ['event']*len(event_sites)}))
    if curr_release.hour == 12:
        extended_sites = sorted(event.loc[(event.validity <= curr_release-timedelta(hours=4)) & (event.validity < pd.to_datetime(curr_release.date())), 'site_code'])
        routine = ewisms.routine_sched(mysql=mysql)
        month = curr_release.strftime('%B').lower()
        iso_week_day = curr_release.weekday()+1
        routine_sites = sorted(set(routine.loc[(routine.iso_week_day == iso_week_day) & (routine.season_type == routine[month]), 'site_code']) - set(event_sites+after_ins_sites+extended_sites))
    else:
        routine_sites = []
        extended_sites = []
    ewi_sched = ewi_sched.append(pd.DataFrame({'site_code': routine_sites, 'mon_type': ['routine']*len(routine_sites)}))
    ewi_sched = ewi_sched.append(pd.DataFrame({'site_code': extended_sites, 'mon_type': ['extended']*len(extended_sites)}))
    return ewi_sched


def unsent_ewisms(df):
    set_org_name = set(['lewc', 'blgu', 'mlgu', 'plgu'])
    if df.mon_type.values[0] != 'event':
        set_org_name -= set(['plgu'])
    unsent = sorted(set_org_name-set(df.org_name))
    unsent_df = pd.DataFrame()
    if len(unsent) != 0:
        unsent_df = pd.DataFrame({'site_code': [df.site_code.values[0]], 'ofc': [', '.join(unsent)]})
    return unsent_df


def get_recipient(curr_release, unsent=True):    
    query = "SELECT * FROM monshiftsched "
    query += "WHERE ts < '{}' ".format(curr_release)
    query += "ORDER BY ts DESC LIMIT 1"
    IOMP = db.df_read(query, connection='analysis')
        
    query =  "SELECT * FROM users "
    query += "WHERE first_name = 'Community' "
    if unsent:
        query += "OR (user_id IN (select user_fk_id user_id from user_accounts)  "
        query += "  AND nickname in {}) ".format(tuple(IOMP.loc[:, ['iompmt', 'iompct']].values[0]))
    users = db.df_read(query, connection='common')
    if len(users) == 1:
        user_id_list = '('+str(users.user_id.values[0])+')'
    else:
        user_id_list = tuple(users.user_id)
        
    query =  "SELECT mobile_id, gsm_id, status FROM "
    query += "  (SELECT * from user_mobiles "
    query += "  WHERE user_id IN {}) um".format(user_id_list)
    query += "INNER JOIN mobile_numbers USING (mobile_id)"
    user_mobiles = db.df_read(query, connection='gsm_pi')
    
    return user_mobiles.loc[user_mobiles.status == 1, ['mobile_id', 'gsm_id']]


def send_unsent_notif(df, curr_release):
    ts = curr_release.strftime('%I%p %B %d, %Y')
    if len(df) != 0:
        unsent_ewi = '\n'.join(list(map(lambda x: ': '.join(x), df.values)))
        sms_msg = 'Unsent EWI SMS (' + ts + '):\n\n' + unsent_ewi
        smsoutbox_user_status = get_recipient(curr_release)
    else:
        sms_msg = 'Sent all EWI SMS (' + ts + ')'
        smsoutbox_user_status = get_recipient(curr_release, unsent=False)
    smsoutbox_users = pd.DataFrame({'sms_msg': [sms_msg], 'source': ['central']})
    data_table = sms.DataTable('smsoutbox_users', smsoutbox_users)
    outbox_id = db.df_write(data_table, connection='gsm_pi', last_insert=True)[0][0]

    smsoutbox_user_status.loc[:, 'outbox_id'] = outbox_id
    data_table = sms.DataTable('smsoutbox_user_status', smsoutbox_user_status)
    db.df_write(data_table, connection='gsm_pi')


def main():

    time_now = datetime.now()
    curr_release = ewisms.release_time(time_now) - timedelta(hours=4)
    
    start = curr_release - timedelta(3)
    end = curr_release + timedelta(hours=4)
    
    mysql = True
    
    ewi_sched = get_monitored_sites(curr_release, start, end, mysql=mysql)
    
    if len(ewi_sched) != 0:
        ewisms_sent = ewisms.ewi_sent(start=curr_release, end=end, mysql=mysql)    
        ewisms_sent = pd.merge(ewi_sched, ewisms_sent.reset_index(), how='left', on='site_code')
    
        site_ewisms = ewisms_sent.groupby('site_code', as_index=False)
        df = site_ewisms.apply(unsent_ewisms).reset_index(drop=True)
        
        send_unsent_notif(df, curr_release)
    
    return df



if __name__ == '__main__':
    df = main()