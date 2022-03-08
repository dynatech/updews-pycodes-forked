from datetime import timedelta
import os
import pandas as pd
import re
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import dynadb.db as db
import volatile.memory as mem


output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
conn = mem.get('DICT_DB_CONNECTIONS')


def get_sms_recipients(mysql=True, to_csv=False):
    if mysql:
        query = "SELECT mobile_id, sim_num, user_id, fullname, site_id, org_name, alert_level, start_ewi_recipient FROM "
        query += "    {gsm_pi}.mobile_numbers "
        query += "  LEFT JOIN "
        query += "    {gsm_pi}.user_mobiles "
        query += "  USING (mobile_id) "
        query += "  LEFT JOIN "
        query += "    (select user_id, CONCAT(first_name, ' ', last_name) AS fullname, status AS user_status, ewi_recipient, start_ewi_recipient from {common}.users) users "
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

def get_sms_sent(start, end, site_names, mysql=True, to_csv=False):
    if mysql:
        query =  "SELECT outbox_id, ts_written, ts_sent, site_id, user_id, mobile_id, sms_msg FROM "
        query += "	(SELECT outbox_id, ts_written, ts_sent, mobile_id, sim_num, "
        query += "	CONCAT(first_name, ' ', last_name) AS fullname, sms_msg, "
        query += "	send_status, user_id FROM "
        query += "		{gsm_pi}.smsoutbox_users "
        query += "	INNER JOIN "
        query += "		(SELECT * FROM {gsm_pi}.smsoutbox_user_status "
        # pisd trial messages for training
        query += "      WHERE stat_id NOT IN (1072245,1072246,1067662,1065358,1064091) "
        query += "      ) AS sms_stat "
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
        query += "AND user_id NOT IN (31, 631, 948, 976) "
        query = query.format(start=start, end=end, common=conn['common']['schema'], gsm_pi=conn['gsm_pi']['schema'])
        df = db.df_read(query, resource='sms_analysis')
        df.loc[:, 'sms_msg'] = df.sms_msg.str.lower().str.replace('city', '').str.replace('.', '')
        df = pd.merge(df, site_names.loc[:, ['site_id', 'name']], on='site_id', how='left')
        df = df.loc[~df.name.isnull(), :]
        if len(df) != 0:
            df = df.loc[df.apply(lambda row: len(re.findall(row['name'], row.sms_msg))!=0, axis=1), :]
        if to_csv:
            df.to_csv(output_path+'/input_output/sent.csv', index=False)
    else:
        df = pd.read_csv(output_path+'/input_output/sent.csv')
    return df


def get_bulletin_recipients(mysql=True, to_csv=False):
    if mysql:
        query = "SELECT fullname, site_id, email FROM "
        query += "    {common}.user_emails "
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
        query += "and site_code is not null and org_name='phivolcs'"
        query += "and ewi_recipient = 1 and user_status = 1 "
        query += "order by site_id, fullname"
        query = query.format(common=conn['common']['schema'], gsm_pi=conn['gsm_pi']['schema'])
        df = db.df_read(query, resource='sms_analysis')
        if to_csv:
            df.to_csv(output_path+'/input_output/ewi_recipient.csv', index=False)
    else:
        df = pd.read_csv(output_path+'/input_output/ewi_recipient.csv')
    return df

def get_bulletin_sent(start, end, mysql=True, to_csv=False):
    if mysql:
        query  = "SELECT timestamp, site_id, site_code, narrative FROM narratives "
        query += "INNER JOIN sites USING (site_id) "
        query += "WHERE TIMESTAMP BETWEEN '{}' AND '{}' "
        query += "AND narrative REGEXP 'EWI BULLETIN'"
        query = query.format(start, end)
        df = db.df_read(query, connection='common')
        if to_csv:
            df.to_csv(output_path+'/input_output/sent.csv', index=False)
    else:
        df = pd.read_csv(output_path+'/input_output/sent.csv')
    return df


def get_rain_recipients(mysql=True, to_csv=False):
    if mysql:
        conn = mem.get('DICT_DB_CONNECTIONS')
        query = "SELECT mobile_id, sim_num, user_id, rain_site_id, rain_site_code, fullname, province, all_sites, mm_values, percentage, date_start FROM "
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

def get_rain_sent(start, end, mysql=True, to_csv=False):
    if mysql:
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

def get_web_releases(start, end, mysql=True, to_csv=False):
    if mysql:
        query  = "SELECT site_code, data_ts, release_time "
        query += "FROM commons_db.sites "
        query += "INNER JOIN ewi_db.monitoring_events USING (site_id) "
        query += "INNER JOIN ewi_db.monitoring_event_alerts USING (event_id) "
        query += "LEFT JOIN ewi_db.monitoring_releases USING (event_alert_id)"
        query += "WHERE data_ts BETWEEN '{start}' AND '{end}' "
        query += "ORDER BY site_code, data_ts desc"
        query = query.format(start=start, end=end)
        df = db.df_read(query=query, resource="ops")
        if to_csv:
            df.to_csv(output_path + 'webreleases.csv', index=False)
    else:
        df = pd.read_csv(output_path + 'webreleases.csv')
    df.loc[:, 'data_ts'] = pd.to_datetime(df.data_ts)
    df.loc[:, 'ts_release'] = df.loc[: , ['data_ts', 'release_time']].apply(lambda row: pd.to_datetime(str(row.data_ts.date()) + ' ' + str(row.release_time).replace('0 days ', '')), axis=1)
    df.loc[df.data_ts > df.ts_release, 'ts_release'] = df.loc[df.data_ts > df.ts_release, 'ts_release'] - timedelta(1)
    return df