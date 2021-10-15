from datetime import timedelta
import os
import pandas as pd
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import ops.lib.lib as lib
import dynadb.db as db
import volatile.memory as mem


output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
deduction = 0.1
incremental = 3

def get_ewi_recipients(mysql=True, to_csv=False):
    if mysql:
        conn = mem.get('DICT_DB_CONNECTIONS')
        query = "SELECT mobile_id, sim_num, user_id, fullname, site_id, org_name, alert_level FROM "
        query += "    {gsm_pi}.mobile_numbers "
        query += "  LEFT JOIN "
        query += "    {gsm_pi}.user_mobiles "
        query += "  USING (mobile_id) "
        query += "  LEFT JOIN "
        query += "    (select user_id, CONCAT(first_name, ' ', last_name) AS fullname, status AS user_status, ewi_recipient from {common}.users) users "
        query += "  USING (user_id) "
        query += "LEFT JOIN "
        query += "  (SELECT user_id, site_id, site_code, org_name, primary_contact FROM "
        query += "    {common}.user_organizations "
        query += "  INNER JOIN "
        query += "    {common}.sites "
        query += "  USING (site_id) "
        query += "  ) AS site_org "
        query += "USING (user_id) "
        query += "LEFT JOIN {gsm_pi}.user_ewi_restrictions USING (user_id) "
        query += "where user_id not in (SELECT user_fk_id user_id FROM {common}.user_accounts) "
        query += "and site_code is not null "
        query += "and ewi_recipient = 1 "
        query += "and user_status = 1 and status = 1 "
        query += "order by site_id, org_name, fullname, sim_num"
        query = query.format(common=conn['common']['schema'], gsm_pi=conn['gsm_pi']['schema'])
        df = db.df_read(query, resource='sms_analysis')
        #ewi recipient of higher alert level only
        df.loc[:, 'alert_level'] += 1
        #ewi recipient of event only
        df.loc[df.alert_level.isnull() & (~df.org_name.isin(['lewc', 'blgu', 'mlgu'])), 'alert_level'] = 1
        #recipient of all ewi
        df.loc[:, 'alert_level'] = df.alert_level.fillna(0)
        if to_csv:
            df.to_csv(output_path+'/input_output/ewi_recipient.csv', index=False)
    else:
        df = pd.read_csv(output_path+'/input_output/ewi_recipient.csv')
    return df

def ewi_sent(start, end, mysql=True, to_csv=False):
    if mysql:
        conn = mem.get('DICT_DB_CONNECTIONS')
        query =  "SELECT outbox_id, ts_written, ts_sent, site_id, user_id, mobile_id, send_status, sms_msg FROM "
        query += "	(SELECT outbox_id, ts_written, ts_sent, mobile_id, sim_num, "
        query += "	CONCAT(first_name, ' ', last_name) AS fullname, sms_msg, "
        query += "	send_status, user_id FROM "
        query += "		{gsm_pi}.smsoutbox_users "
        query += "	INNER JOIN "
        query += "		{gsm_pi}.smsoutbox_user_status "
        query += "	USING (outbox_id) "
        query += "	INNER JOIN "
        query += "		(SELECT * FROM  "
        query += "			{gsm_pi}.user_mobiles "
        query += "		INNER JOIN "
        query += "			{gsm_pi}.mobile_numbers "
        query += "		USING (mobile_id) "
        query += "		) mobile "
        query += "	USING (mobile_id) "
        query += "	INNER JOIN "
        query += "		{common}.users "
        query += "	USING (user_id) "
        query += "	) as msg "
        query += "LEFT JOIN "
        query += "	(SELECT * FROM "
        query += "		{common}.user_organizations AS org "
        query += "	INNER JOIN "
        query += "		{common}.sites "
        query += "	USING (site_id) "
        query += "	) AS site_org "
        query += "USING (user_id) "
        query += "WHERE sms_msg regexp 'ang alert level' "
        query += "AND ts_written between '{start}' and '{end}' "
        query += "AND send_status = 5"
        query = query.format(start=start, end=end, common=conn['common']['schema'], gsm_pi=conn['gsm_pi']['schema'])
        df = db.df_read(query, resource='sms_analysis')
        df.loc[:, 'sms_msg'] = df.sms_msg.str.lower().str.replace('city', '').str.replace('.', '')
        if to_csv:
            df.to_csv(output_path+'/input_output/sent.csv', index=False)
    else:
        df = pd.read_csv(output_path+'/input_output/sent.csv')
    site_names = lib.get_site_names().loc[:, ['site_id', 'name']]
    df = pd.merge(df, site_names, on='site_id', how='left')
    df = df.loc[df.apply(lambda row: row['name'] in row.sms_msg, axis=1), :]
    return df

def check_sent(release, sent):
    data_ts = pd.to_datetime(release['data_ts'].values[0])
    release_sent = sent.loc[(sent.ts_written >= data_ts) & (sent.ts_written <= data_ts+timedelta(hours=4)), :]
    sent_sched = pd.merge(release, release_sent, how='left', on=['site_id', 'user_id', 'mobile_id'])
    sent_sched = sent_sched.drop_duplicates(['site_id', 'user_id', 'mobile_id', 'outbox_id'])
    return sent_sched

def ewi_sms_sched(start, end, mysql=True, to_csv=False):
    ewi_sms_sched = lib.release_sched(start, end, mysql=mysql, to_csv=to_csv)
    recipient = get_ewi_recipients(mysql=mysql, to_csv=to_csv)    
    sched = pd.merge(ewi_sms_sched, recipient, how='left', on='site_id')
    if len(sched) != 0:
        sent = ewi_sent(start, end+timedelta(hours=4), mysql=mysql, to_csv=to_csv)
        per_ts = sched.groupby(['data_ts'], as_index=False)
        sent_sched = per_ts.apply(check_sent, sent=sent).reset_index(drop=True)
        #remove special cases and nonrecipient of extended/routine
        sent_sched = sent_sched.loc[(sent_sched.pub_sym_id - 1 >= sent_sched.alert_level), :]
    else:
        sent_sched = pd.DataFrame()
    return sent_sched