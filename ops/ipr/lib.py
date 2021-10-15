from datetime import datetime, timedelta, time
import numpy as np
import pandas as pd

import dynadb.db as db
import volatile.memory as mem


def release_time(date_time):
    """Rounds time to 4/8/12 AM/PM.

    Args:
        date_time (datetime): Timestamp to be rounded off. 04:00 to 07:30 is
        rounded off to 8:00, 08:00 to 11:30 to 12:00, etc.

    Returns:
        datetime: Timestamp with time rounded off to 4/8/12 AM/PM.

    """

    date_time = pd.to_datetime(date_time)

    time_hour = int(date_time.strftime('%H'))

    quotient = int(time_hour / 4)

    if quotient == 5:
        date_time = datetime.combine(date_time.date()+timedelta(1), time(0,0))
    else:
        date_time = datetime.combine(date_time.date(), time((quotient+1)*4,0))
            
    return date_time


def get_site_names():
    sites = mem.get('df_sites')
    special = ['hin', 'mca', 'msl', 'msu']
    sites.loc[~sites.site_code.isin(special), 'name'] = sites.loc[~sites.site_code.isin(special), ['barangay', 'municipality']].apply(lambda row: ', '.join(row.values).lower().replace('city', '').replace('.', '').strip(), axis=1)
    sites.loc[sites.site_code.isin(special[0:2]), 'name'] = sites.loc[sites.site_code.isin(special[0:2]), 'municipality'].apply(lambda x: x.lower())
    sites.loc[sites.site_code.isin(special[2:4]), 'name'] = sites.loc[sites.site_code.isin(special[2:4]), 'sitio'].apply(lambda x: x.lower())
    return sites


def get_sheet(key, sheet_name, drop_unnamed=True):
    url = 'https://docs.google.com/spreadsheets/d/{key}/gviz/tq?tqx=out:csv&sheet={sheet_name}'.format(
        key=key, sheet_name=sheet_name.replace(' ', '%20'))
    df = pd.read_csv(url, date_parser=pd.to_datetime, dayfirst=True)
    if drop_unnamed:
        df = df.drop([col for col in df.columns if col.startswith('Unnamed')], axis=1)
    return df


# personnel list
def get_personnel():
    key = "1UylXLwDv1W1ukT4YNoUGgHCHF-W8e3F8-pIg1E024ho"
    sheet_name = "personnel"
    df = get_sheet(key, sheet_name)
    df = df.rename(columns={'Fullname': 'Name'})
    return df


def get_narratives(start, end, mysql=False, category='EWI BULLETIN'):
    if mysql:
        query  = "SELECT site_code, timestamp, narrative FROM "
        query += "  (SELECT * FROM commons_db.narratives "
        query += "  WHERE timestamp > '{start}' "
        query += "  AND timestamp <= '{end}' "
        query += "  ) bulletin "
        query += "INNER JOIN commons_db.sites USING (site_id) "
        query += "ORDER BY timestamp"
        query = query.format(start=start+timedelta(hours=8), end=end+timedelta(hours=8))
        df = db.df_read(query=query, connection="common")
        df.to_csv('input/narrative.csv', index=False)
    else:
        df = pd.read_csv('input/narrative.csv')
    df.loc[:, 'timestamp'] = pd.to_datetime(df.timestamp)
    df = df.loc[df.narrative.str.contains(category), :]
    return df


def system_downtime(mysql=False):
    if mysql:
        query = 'SELECT * FROM system_down WHERE reported = 1'
        df = db.df_read(query=query, resource="sensor_data")
        df.to_csv('input/downtime.csv', index=False)
    else:
        df = pd.read_csv('input/downtime.csv')
    df.loc[:, ['start_ts', 'end_ts']] = df.loc[:, ['start_ts', 'end_ts']].apply(pd.to_datetime)
    return df


def remove_downtime(df, downtime):
    for start, end in downtime[['start_ts', 'end_ts']].values:
        df = df.loc[(df.ts_start < start+np.timedelta64(150, 'm')) | (df.ts_start > end+np.timedelta64(150, 'm')), :]
    return df


def ewi_recipients():
    conn = mem.get('DICT_DB_CONNECTIONS')
    query = "SELECT mobile_id, sim_num, status, user_id, fullname, user_status, ewi_recipient, site_code, org_name, primary_contact, alert_level FROM "
    query += "    {gsm_pi}.mobile_numbers "
    query += "  LEFT JOIN "
    query += "    {gsm_pi}.user_mobiles "
    query += "  USING (mobile_id) "
    query += "  LEFT JOIN "
    query += "    (select user_id, CONCAT(first_name, ' ', last_name) AS fullname, status AS user_status, ewi_recipient from {common}.users) users "
    query += "  USING (user_id) "
    query += "LEFT JOIN "
    query += "  (SELECT user_id, site_code, org_name, primary_contact FROM "
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
    query += "order by site_code, org_name, user_id, sim_num"
    query = query.format(common=conn['common']['schema'], gsm_pi=conn['gsm_pi']['schema'])
    df = db.df_read(query, resource='sms_analysis')
    return df