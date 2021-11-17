from datetime import datetime
import os
import pandas as pd
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import dynadb.db as db
import gsm.smsparser2.smsclass as sms
import ops.checksent.olivianotif as olivia

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


def send_notif(ts=datetime.now()):
    notif = olivia.main(ts)
    sms_msg = '\n'.join(list(filter(lambda x: x.startswith('Un'), notif.split('\nNo '))))
    if sms_msg != '':
        smsoutbox_user_status = get_recipient(ts)
        smsoutbox_users = pd.DataFrame({'sms_msg': [sms_msg], 'source': ['central']})
        data_table = sms.DataTable('smsoutbox_users', smsoutbox_users)
        outbox_id = db.df_write(data_table, connection='gsm_pi', last_insert=True)[0][0]
    
        smsoutbox_user_status.loc[:, 'outbox_id'] = outbox_id
        data_table = sms.DataTable('smsoutbox_user_status', smsoutbox_user_status)
        db.df_write(data_table, connection='gsm_pi')
        
        
if __name__ == '__main__':
    send_notif()
