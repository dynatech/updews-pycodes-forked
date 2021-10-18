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
        query = "SELECT mobile_id, sim_num, user_id, rain_site_id, rain_site_code, fullname, province, all_sites, mm_values, percentage FROM "
        query += "{common}.rain_info_recipients "
        query += "	LEFT JOIN "
        query += "(SELECT user_id, CONCAT(first_name, ' ', last_name) AS fullname, status AS user_status, ewi_recipient "
        query += "FROM {common}.users "
        query += ") users  "
        query += "	USING (user_id) "
        query += "	LEFT JOIN "
        query += "{gsm_pi}.user_mobiles  "
        query += "	USING (user_id) "
        query += "	LEFT JOIN "
        query += "{gsm_pi}.mobile_numbers "
        query += "	USING (mobile_id) "
        query += "	LEFT JOIN "
        query += "(SELECT user_id, site_id AS rain_site_id, site_code AS rain_site_code, province FROM  "
        query += "	{common}.user_organizations "
        query += "		INNER JOIN  "
        query += "	{common}.sites  "
        query += "		USING (site_id) "
        query += ") AS site_org  "
        query += "	USING (user_id) "
        query += "	LEFT JOIN "
        query += "{gsm_pi}.user_ewi_restrictions  "
        query += "	USING (user_id) "
        query += "WHERE user_id NOT IN ( "
        query += "	SELECT user_fk_id user_id "
        query += "    FROM {common}.user_accounts) "
        query += "AND ewi_recipient = 1 "
        query += "AND user_status = 1 "
        query += "AND status = 1 "
        query += "ORDER BY fullname, sim_num"
        query = query.format(common=conn['common']['schema'], gsm_pi=conn['gsm_pi']['schema'])
        df = db.df_read(query, resource='sms_analysis')
        if to_csv:
            df.to_csv(output_path+'/input_output/ewi_recipient.csv', index=False)
    else:
        df = pd.read_csv(output_path+'/input_output/ewi_recipient.csv')
    return df

def ewi_sent(start, end, mysql=True, to_csv=False):
    if mysql:
        conn = mem.get('DICT_DB_CONNECTIONS')
        query = "SELECT ts_written, ts_sent, mobile_id, sms_msg, tag_id FROM "
        query += "  (SELECT outbox_id, ts_written, ts_sent, mobile_id, sms_msg FROM  "
        query += "    {gsm_pi}.smsoutbox_users "
        query += "  INNER JOIN  "
        query += "    {gsm_pi}.smsoutbox_user_status  "
        query += "  USING (outbox_id) "
        query += "  ) AS msg "
        query += "LEFT JOIN  "
        query += "  (SELECT outbox_id, tag_id FROM {gsm_pi}.smsoutbox_user_tags  "
        query += "  WHERE ts BETWEEN '{start}' AND '{end}' "
        query += "  AND tag_id = 21 "
        query += "  ORDER BY outbox_id DESC LIMIT 5000 "
        query += "  ) user_tags  "
        query += "USING (outbox_id) "
        query += "WHERE sms_msg REGEXP 'Rainfall info for' "
        query += "AND ts_written BETWEEN '{start}' AND '{end}'"
        query = query.format(start=start, end=end, common=conn['common']['schema'], gsm_pi=conn['gsm_pi']['schema'])
        df = db.df_read(query, resource='sms_analysis')
        df.loc[:, 'sms_msg'] = df.sms_msg.str.lower().str.replace('city', '').str.replace('.', '')
        if to_csv:
            df.to_csv(output_path+'/input_output/sent.csv', index=False)
    else:
        df = pd.read_csv(output_path+'/input_output/sent.csv')
    return df

def check_sent(release, sent):
    data_ts = pd.to_datetime(release['data_ts'].values[0])
    release_sent = sent.loc[(sent.ts_written >= data_ts) & (sent.ts_written <= data_ts+timedelta(hours=4)), :]
    release.loc[:, 'ts_written'] = release.apply(lambda row: release_sent.loc[(release_sent.sms_msg.str.contains(row['name'])) & (release_sent.mobile_id==row['mobile_id']), 'ts_written'], axis=1).min(axis=1)
    release.loc[:, 'ts_sent'] = release.apply(lambda row: release_sent.loc[(release_sent.sms_msg.str.contains(row['name'])) & (release_sent.mobile_id==row['mobile_id']), 'ts_sent'], axis=1).min(axis=1)
    release.loc[:, 'tagged'] = release.apply(lambda row: int(21 in release_sent.loc[(release_sent.sms_msg.str.contains(row['name'])) & (release_sent.mobile_id==row['mobile_id']), 'tag_id'].values), axis=1)
    release.loc[:, 'written_mm'] = release.apply(lambda row: int((row['mm_values'] == 1) & (len(release_sent.loc[(release_sent.sms_msg.str.contains(row['name'])) & (release_sent.mobile_id==row['mobile_id']) & (release_sent.sms_msg.str.contains(r'(?=.*mm)(?=.*threshold)',regex=True)), :]) != 0)), axis=1)
    release.loc[:, 'written_percent'] = release.apply(lambda row: int((row['mm_values'] == 1) & (len(release_sent.loc[(release_sent.sms_msg.str.contains(row['name'])) & (release_sent.mobile_id==row['mobile_id']) & (release_sent.sms_msg.str.contains('%')), :]) != 0)), axis=1)
    release.loc[(release.mm_values == 1) & (release.written_mm == 0), 'unwritten_info'] = 'mm '
    release.loc[(release.percentage == 1) & (release.written_percent == 0), 'unwritten_info'] += 'percentage'
    release.loc[~release.unwritten_info.isnull(), 'unwritten_info'] = release.loc[~release.unwritten_info.isnull(), 'unwritten_info'].apply(lambda x: '(' + x.strip().replace(' ', ' and ') + ')')
    return release

def ewi_sched(start, end, mysql=True, to_csv=False):
    rain_sched = lib.release_sched(start, end, mysql=mysql, to_csv=to_csv)
    rain_sched = rain_sched.loc[rain_sched.event == 1, :]
    site_names = lib.get_site_names().loc[:, ['site_id', 'province']]
    rain_sched = pd.merge(rain_sched, site_names, on='site_id')
    
    recipient = get_ewi_recipients(mysql=mysql, to_csv=to_csv)    
    rain_sched = pd.merge(rain_sched, recipient, on='province')
    
    if len(rain_sched) != 0:
        rain_sched = rain_sched.loc[rain_sched.apply(lambda row: (row.all_sites==1) | (row.site_id==row.rain_site_id), axis=1), :]
        rain_sched = rain_sched.drop_duplicates(['data_ts', 'rain_site_id', 'sim_num'])
        site_names = lib.get_site_names().loc[:, ['site_id', 'name']]
        rain_sched = pd.merge(rain_sched, site_names, left_on='rain_site_id', right_on='site_id')
    
        sent = ewi_sent(start, end+timedelta(hours=4), mysql=mysql, to_csv=to_csv)
        per_ts = rain_sched.groupby(['data_ts'], as_index=False)
        sent_sched = per_ts.apply(check_sent, sent=sent).reset_index(drop=True)
    else:
        sent_sched = pd.DataFrame()
    return sent_sched