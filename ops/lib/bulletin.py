from datetime import timedelta
import numpy as np
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

def ewi_sent(start, end, mysql=True, to_csv=False):
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

def check_sent(release, sent, queued):
    data_ts = pd.to_datetime(release['data_ts'].values[0])
    site_id = release['site_id'].values[0]
    end = lib.release_time(data_ts)
    onset = True
    if end - data_ts <= timedelta(minutes=30):
        ts_name = end.time().strftime('%I%p')
        end += timedelta(hours=4)
        onset = False
    release_queued = queued.loc[(queued.site_id == site_id) & (queued.timestamp >= data_ts) & (queued.timestamp <= end), :]
    if len(release_queued) != 0:
        release.loc[:, 'ts_queued'] = min(release_queued)
    else:
        release.loc[:, 'ts_queued'] = np.nan
    release_sent = sent.loc[(sent.site_id == site_id) & (sent.timestamp >= data_ts) & (sent.timestamp <= end), :]
    if not onset:
        release_sent = release_sent.loc[release_sent.narrative.str.contains(ts_name), :]
    release.loc[:, 'ts_sent'] = release.apply(lambda row: release_sent.loc[release_sent.narrative.str.contains(row.email), 'timestamp'], axis=1).min(axis=1)    
    return release
        
def ewi_sched(start, end, mysql=True, to_csv=False):
    sched = lib.release_sched(start, end, mysql=mysql, to_csv=to_csv)
    sched = sched.loc[sched.event == 1, :]
    recipient = get_ewi_recipients(mysql=mysql, to_csv=to_csv)
    bulletin_sched = pd.merge(sched, recipient, how='inner', on='site_id')
    bulletin_sched = bulletin_sched.append(sched.assign(fullname='Arturo S. Daag', email='asdaag48@gmail.com'), ignore_index=True, sort=False)
    bulletin_sched = bulletin_sched.append(sched.assign(fullname='Renato U. Solidum, Jr.', email='RUS'), ignore_index=True, sort=False)
    bulletin_sched = bulletin_sched.append(sched.loc[sched.EQ == 1, :].assign(fullname='Jeffrey Perez', email='jeffrey.perez@phivolcs.dost.gov.ph'), ignore_index=True, sort=False)
    if len(bulletin_sched) != 0:
        sent_queued = ewi_sent(start, end+timedelta(hours=4), mysql=mysql, to_csv=to_csv)
        sent = sent_queued.loc[sent_queued.narrative.str.contains('M EWI BULLETIN to '), :]
        queued = sent_queued.loc[sent_queued.narrative.str.contains('Added EWI bulletin to the sending queue'), :]
        per_release = bulletin_sched.groupby(['site_id', 'data_ts'], as_index=False)
        sent_sched = per_release.apply(check_sent, sent=sent, queued=queued).reset_index(drop=True)
        sent_sched.loc[sent_sched.ts_queued.isnull(), 'ts_queued'] = sent_sched.loc[sent_sched.ts_queued.isnull(), 'ts_sent']
    else:
        sent_sched = pd.DataFrame()
    return sent_sched