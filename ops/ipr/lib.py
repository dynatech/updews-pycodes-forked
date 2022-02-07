from datetime import timedelta
import numpy as np
import os
import pandas as pd
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import dynadb.db as db

output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..//input_output//'))

def get_sheet(key, sheet_name, drop_unnamed=True):
    url = 'https://docs.google.com/spreadsheets/d/{key}/gviz/tq?tqx=out:csv&sheet={sheet_name}'.format(
        key=key, sheet_name=sheet_name.replace(' ', '%20'))
    df = pd.read_csv(url, date_parser=pd.to_datetime, dayfirst=True)
    if drop_unnamed:
        df = df.drop([col for col in df.columns if col.startswith('Unnamed')], axis=1)
    return df


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
        df.to_csv(output_path + 'narrative.csv', index=False)
    else:
        df = pd.read_csv(output_path + 'narrative.csv')
    df.loc[:, 'timestamp'] = pd.to_datetime(df.timestamp)
    df = df.loc[df.narrative.str.contains(category), :]
    return df


def system_downtime(mysql=False):
    if mysql:
        query = 'SELECT * FROM system_down WHERE reported = 1'
        df = db.df_read(query=query, resource="sensor_data")
        df.to_csv(output_path + 'downtime.csv', index=False)
    else:
        df = pd.read_csv(output_path + 'downtime.csv')
    df.loc[:, ['start_ts', 'end_ts']] = df.loc[:, ['start_ts', 'end_ts']].apply(pd.to_datetime)
    return df


def remove_downtime(df, downtime, meas_reminder=False):
    if meas_reminder:
        mins = 150
    else:
        mins = 0
    for start, end in downtime[['start_ts', 'end_ts']].values:
        df = df.loc[(df.data_ts < start+np.timedelta64(mins, 'm')) | (df.data_ts > end+np.timedelta64(mins, 'm')), :]
    return df