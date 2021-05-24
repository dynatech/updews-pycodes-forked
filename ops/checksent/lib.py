from datetime import timedelta
import os
import pandas as pd
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import dynadb.db as db
import gsm.smsparser2.smsclass as sms
import ops.ipr.ewisms_meal as ewisms


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


def send_unsent_notif(df, notif_type, curr_release, validation=False):
    ts = curr_release.strftime('%I%p %B %d, %Y')
    if len(df) != 0:
        site_notif = '\n'.join(list(map(lambda x: ': '.join(x), df.values)))
        if validation:
            sms_msg = 'Validate measurements with displacement of 1cm and more:\n\n' + site_notif
        else:
            sms_msg = 'Unsent ' + notif_type + ' (' + ts + '):\n\n' + site_notif
        smsoutbox_user_status = get_recipient(curr_release)
    else:
        if notif_type == 'gndmeas':
            return
        sms_msg = 'Sent all ' + notif_type + ' (' + ts + ')'
        smsoutbox_user_status = get_recipient(curr_release, unsent=False)
    smsoutbox_users = pd.DataFrame({'sms_msg': [sms_msg], 'source': ['central']})
    data_table = sms.DataTable('smsoutbox_users', smsoutbox_users)
    outbox_id = db.df_write(data_table, connection='gsm_pi', last_insert=True)[0][0]

    smsoutbox_user_status.loc[:, 'outbox_id'] = outbox_id
    data_table = sms.DataTable('smsoutbox_user_status', smsoutbox_user_status)
    db.df_write(data_table, connection='gsm_pi')