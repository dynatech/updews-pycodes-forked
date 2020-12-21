# -*- coding: utf-8 -*-
"""
Created on Wed Jun 24 11:08:27 2020

@author: Meryll
"""

from datetime import datetime, timedelta
import numpy as np
import pandas as pd

import dynadb.db as db


def check_sending(shift_release, releases):
    site_code = shift_release.site_code.values[0]
    start = pd.to_datetime(shift_release.ts_start.values[0])
    sent = releases.loc[(releases.ts_start >= start-timedelta(hours=0.5)) & (releases.ts_start <= start+timedelta(hours=0.5)) & (releases.site_code == site_code), :]
    if len(sent) == 0:
        shift_release.loc[:, 'deduction'] = 1
    else:
        ts_release = pd.to_datetime(min(sent.ts_release))
        if ts_release < start:
            shift_release.loc[:, 'deduction'] = 0
        else:
            shift_release.loc[:, 'deduction'] = 0.1 * np.ceil((start - ts_release) / timedelta(minutes=10))
    return shift_release


def system_downtime(mysql=False):
    if mysql:
        query = 'SELECT * FROM system_down'
        df = db.df_read(query=query, resource="sensor_data")
    else:
        df = pd.read_csv('input/downtime.csv')
    df.loc[:, ['start_ts', 'end_ts']] = df.loc[:, ['start_ts', 'end_ts']].apply(pd.to_datetime)
    return df


def remove_downtime(df, downtime):
    for start, end in downtime[['start_ts', 'end_ts']].values:
        df = df.loc[(df.ts_start < start+np.timedelta64(150, 'm')) | (df.ts_start > end+np.timedelta64(150, 'm')), :]
    return df

def get_releases(start, end, mysql=False):
    if mysql:
        query  = "SELECT site_code, event_id, validity, pub_sym_id, data_ts as ts_start, release_time "
        query += "FROM commons_db.sites "
        query += "INNER JOIN ewi_db.monitoring_events USING (site_id) "
        query += "INNER JOIN ewi_db.monitoring_event_alerts USING (event_id) "
        query += "LEFT JOIN "
        query += "  (SELECT * FROM ewi_db.monitoring_releases "
        query += "  LEFT JOIN ewi_db.monitoring_triggers USING (release_id) "
        query += "  LEFT JOIN ewi_db.internal_alert_symbols USING (internal_sym_id) "
        query += "  ) AS trig "
        query += "USING (event_alert_id)"
        query += "WHERE data_ts BETWEEN '{start}' AND '{end}' "
        query += "ORDER BY site_code, data_ts desc"
        query = query.format(start=start, end=end)
        df = db.df_read(query=query, resource="ops")
    else:
        df = pd.read_csv('input/webreleases.csv')
    return df

def main(start, end, mysql=False):
    downtime = system_downtime(mysql=mysql)
    ewi_sched = pd.read_csv('output/sending_status.csv', parse_dates=['ts_start', 'ts_end'])
    ewi_sched = remove_downtime(ewi_sched, downtime)
    ewi_sched.loc[ewi_sched.raising == 1, 'ts_start'] = ewi_sched.loc[ewi_sched.raising == 1, 'ts_start'] + timedelta(minutes=55)
    
    releases = get_releases(start, end, mysql=mysql)
    releases.loc[:, ['ts_start', 'release_time']] = releases.loc[:, ['ts_start', 'release_time']].apply(pd.to_datetime)
    releases.loc[: , 'ts_start'] = releases.loc[: , 'ts_start'] + timedelta(minutes=30)
    releases.loc[:, 'ts_release'] = releases.loc[: , ['ts_start', 'release_time']].apply(lambda row: datetime.combine(row.ts_start.date(), row.release_time.time()), axis = 1)
    releases.loc[releases.ts_start.dt.time < releases.release_time.dt.time, 'ts_release'] = releases.loc[releases.ts_start.dt.time < releases.release_time.dt.time, 'ts_release'] - timedelta(1)
    
    monitoring_ipr = pd.read_excel('output/monitoring_ipr.xlsx', sheet_name=None)
    
    for name in monitoring_ipr.keys():
        indiv_ipr = monitoring_ipr[name]
    
        for ts in indiv_ipr.columns[5:]:
            ts = pd.to_datetime(ts)
            ts_end = ts + timedelta(0.5)
            shift_release = ewi_sched.loc[(ewi_sched.ts_start > ts) & (ewi_sched.ts_start <= ts_end), :]
            if len(shift_release) != 0:
                indiv_release = shift_release.groupby('index', as_index=False)
                shift_release = indiv_release.apply(check_sending, releases=releases).reset_index(drop=True)
                grade = np.round((len(shift_release) - sum(shift_release.deduction)) / len(shift_release), 2)
                indiv_ipr.loc[indiv_ipr.Output2 == 'EWI web release', str(ts)] = grade
        monitoring_ipr[name] = indiv_ipr
        
    writer = pd.ExcelWriter('output/monitoring_ipr.xlsx')
    for sheet_name, xlsxdf in monitoring_ipr.items():
        xlsxdf.to_excel(writer, sheet_name, index=False)
    writer.save()
