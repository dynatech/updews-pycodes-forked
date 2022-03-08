from datetime import datetime, timedelta, time
import numpy as np
import os
import pandas as pd
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import dynadb.db as db
import volatile.memory as mem


output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))


def round_data_ts(date_time):
    """Rounds time to HH:00 or HH:30.

    Args:
        date_time (datetime): Timestamp to be rounded off.

    Returns:
        datetime: Timestamp with time rounded off to HH:00 or HH:30. 
                  Rounds to HH:00 if before HH:30, else rounds to HH:30.

    """

    hour = date_time.hour
    minute = date_time.minute

    if minute < 30:
        minute = 0
    else:
        minute = 30
    date_time = datetime.combine(date_time.date(), time(hour, minute))

    return date_time

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

def get_routine_dates(start, end):
    ewi_sched = pd.DataFrame({'data_ts': pd.date_range(start=pd.to_datetime(start.date()), 
                                                       end=pd.to_datetime(end.date())+timedelta(1),
                                                       freq='1D', closed='left')})
    ewi_sched.loc[:, 'data_ts'] = pd.to_datetime(ewi_sched.loc[:, 'data_ts'])
    ewi_sched.loc[:, 'day'] = ewi_sched.data_ts.apply(lambda x: x.weekday()+1)
    ewi_sched.loc[:, 'month'] = ewi_sched.data_ts.apply(lambda x: x.strftime('%B').lower())
    ewi_sched = ewi_sched.loc[ewi_sched.day.isin([2,3,5]), :]
    return ewi_sched

def get_site_names():
    sites = mem.get('df_sites')
    special = ['hin', 'mca', 'msl', 'msu']
    sites.loc[~sites.site_code.isin(special), 'name'] = sites.loc[~sites.site_code.isin(special), ['barangay', 'municipality']].apply(lambda row: ', '.join(row.values).lower().replace('city', '').replace('.', '').strip(), axis=1)
    sites.loc[sites.site_code.isin(special[0:2]), 'name'] = sites.loc[sites.site_code.isin(special[0:2]), 'municipality'].apply(lambda x: x.lower())
    sites.loc[sites.site_code.isin(special[2:4]), 'name'] = sites.loc[sites.site_code.isin(special[2:4]), 'sitio'].apply(lambda x: x.lower().replace(' ', ' | '))
    return sites

def get_sites_no_markers(mysql=True, to_csv=False):
    if mysql:
        conn = mem.get('DICT_DB_CONNECTIONS')
        query = "select site_id, site_code from {common}.sites  "
        query += "where site_id not in  "
        query += "	(select site_id from {analysis}.site_markers  "
        query += "	where in_use = 1) "
        query += "and active = 1"
        query = query.format(common=conn['common']['schema'], analysis=conn['analysis']['schema'])
        df = db.df_read(query, resource='sensor_analysis')
        df = df.loc[df.site_code != 'msu', :]
        if to_csv:
            df.to_csv(output_path+'/input_output/site_no_marker.csv', index=False)
    else:
        df = pd.read_csv(output_path+'/input_output/site_no_marker.csv')
    return df

def event_with_eq(df, mysql=True):
    df.loc[:, 'EQ'] = int('E' in df.alert_symbol.values)
    return df

def event_with_moms(df, mysql=True):
    sites_no_markers = get_sites_no_markers(mysql=mysql)['site_id'].values
    df.loc[:, 'moms'] = int('m' in df.alert_symbol.values or 'M' in df.alert_symbol.values or any(site_id in df.site_id.values for site_id in sites_no_markers))
    return df

def get_event_releases(start, end, mysql=True, to_csv=False):
    """Gets events overlapping with timestamp range given.

    Args:
        start (datetime): Start of timestamp range.
        end (datetime): End of timestamp range.
        mysql (bool): Gets data from mysql if True, else gets data from csv. 
                      Optional. Default: False.
        drop (bool): Retains most recent event release if True. Optional. 
                     Default: True.

    Returns:
        DataFrame: Events overlapping with timestamp range given. 
                   Columns: site_id, site_code, event_id, validity, pub_sym_id, alert_symbol

    """

    if mysql:
        start = start-timedelta(3)
        end = end+timedelta(3)
        conn = mem.get('DICT_DB_CONNECTIONS')
        query =  "select site_id, site_code, event_id, validity, pub_sym_id, alert_symbol, data_ts, release_time "
        query += "from {common}.sites "
        query += "inner join {website}.monitoring_events using(site_id) "
        query += "left join {website}.monitoring_event_alerts using(event_id) "
        query += "left join "
        query += "	(SELECT * FROM {website}.monitoring_releases "
        query += "	left join {website}.monitoring_triggers using(release_id) "
        query += "	left join {website}.internal_alert_symbols using(internal_sym_id) "
        query += "    ) as trig "
        query += "using(event_alert_id) "
        query += "where ((ts_start >= '{start}' and ts_start <= '{end}') "
        query += "or (validity >= '{start}' and validity <= '{end}') "
        query += "or (ts_start <= '{start}' and validity >= '{end}')) "
        query += "and pub_sym_id != 1 "
        query += "and active = 1 "
        query += "order by event_id, data_ts"
        query = query.format(start=start, end=end, common=conn['common']['schema'], website=conn['website']['schema'])
        df = db.df_read(query, resource='ops')
        if len(df) != 0:
            df.loc[:, 'data_ts'] = df.data_ts.apply(lambda x: round_data_ts(pd.to_datetime(x)))
            df_grp = df.groupby('event_id', as_index=False)
            df = df_grp.apply(event_with_eq, mysql=mysql).reset_index(drop=True)
            df_grp = df.groupby(['event_id', 'pub_sym_id'], as_index=False)
            df = df_grp.apply(event_with_moms, mysql=mysql).reset_index(drop=True)
            if to_csv:
                df.to_csv(output_path+'/input_output/event_releases.csv', index=False)
    else:
        df = pd.read_csv(output_path+'/input_output/event_releases.csv')
    return df

def get_routine_sched(mysql=True, to_csv=False):
    if mysql:
        query  = "select * from "
        query += "	(select * from sites "
        query += "    where active = 1 "
        query += "    ) sites "
        query += "inner join "
        query += "	seasons seas "
        query += "on sites.season = seas.season_group_id "
        query += "inner join "
        query += "	routine_schedules "
        query += "using (sched_group_id)"
        df = db.df_read(query, connection='common')
        if to_csv:
            df.to_csv(output_path+'/input_output/routine.csv', index=False)
    else:
        df = pd.read_csv(output_path+'/input_output/routine.csv')
    return df

def get_event_sched(event, ewi_sched, site_no_markers):
    validity = pd.to_datetime(max(event.validity))
    site_id = event['site_id'].values[0]
    site_code = event['site_code'].values[0]
    EQ = event.loc[event.EQ == 1, 'data_ts']
    moms = event.loc[event.moms == 1, 'data_ts']
    #onset
    onset_sched = event.loc[:, ['site_id', 'site_code', 'data_ts', 'alert_symbol', 'pub_sym_id']]
    onset_sched.loc[:, 'raising'] = 1
    onset_sched.loc[:, 'permission'] = onset_sched.loc[:, ['alert_symbol', 'pub_sym_id']].apply(lambda row: int(((row.alert_symbol == 'D') | (row.pub_sym_id == 4))), axis=1)
    if site_id in site_no_markers.site_id.values:
        col_name = 'moms'
    else:
        col_name = 'gndmeas'
    #with gndmeas/obs reminder at 9:30AM if routine day and raised between 9:30AM to 12NN
    if len(ewi_sched) != 0:
        onset_sched.loc[:, col_name] = onset_sched.apply(lambda row: int((len(ewi_sched.loc[(ewi_sched.data_ts.dt.date == row.data_ts.date()) & (ewi_sched.site_code == row.site_code)]) != 0) & (row.data_ts.time() >= time(9,30)) & (row.data_ts.time() <= time(12,0))), axis=1)
    #with gndmeas/obs reminder if raising (already raised) alert between 5:30-8AM, 9:30AM-12NN, 1:30-4PM
    onset_sched.loc[onset_sched[1::].index, col_name] = onset_sched[1::].data_ts.apply(lambda x: int(((release_time(x) - x).total_seconds()/3600 <= 2.5) and release_time(x).hour <= 16 and release_time(x).hour >= 8))
    
    #lowering
    lowering_sched = event.loc[:, ['site_id', 'site_code', 'pub_sym_id']]
    lowering_sched.loc[:, 'data_ts'] = validity - timedelta(hours=0.5)
    lowering_sched.loc[:, 'lowering'] = 1
    lowering_sched.loc[:, 'permission'] = lowering_sched.pub_sym_id.apply(lambda x: int(x == 4))
    lowering_sched.loc[:, col_name] = lowering_sched.data_ts.apply(lambda x: int(release_time(x).hour <= 16 and release_time(x).hour >= 8))
    lowering_sched = lowering_sched.drop_duplicates('data_ts', keep='last')
    
    #4H release
    event_4H_start = min(event.data_ts)
    event_4H_start = release_time(event_4H_start)
    data_ts = pd.date_range(start=event_4H_start, end=validity-timedelta(hours=4), freq='4H')
    #no 4H release if onset 1H before supposed release
    onset_ts = set(event.loc[event.data_ts.apply(lambda x: x.time() in [time(3,0), time(3,30), time(7,0), time(7,30), time(11,0), time(11,30), time(15,0), time(15,30), time(19,0), time(19,30), time(23,0), time(23,30)]), 'data_ts'].apply(lambda x: release_time(x)))
    data_ts = sorted(set(data_ts) - onset_ts)
    data_ts = list(map(lambda x: x - timedelta(hours=0.5), data_ts))
    #4H release
    event_sched = pd.DataFrame({'data_ts': data_ts})
    event_sched.loc[:, 'site_id'] = site_id
    event_sched.loc[:, 'site_code'] = site_code
    #gndmeas/obs reminder for 8AM, 12NN, 4PM releases
    event_sched.loc[:, col_name] = event_sched.data_ts.apply(lambda x: int(release_time(x).hour <= 16 and release_time(x).hour >= 8))
    #no gndmeas/obs reminder next release time after onset
    next_release_ts = onset_sched.data_ts.apply(lambda x: release_time(x) - timedelta(hours=0.5))
    event_sched.loc[event_sched.data_ts.isin(next_release_ts), col_name] = 0
    event_sched = event_sched.append(onset_sched, ignore_index=True, sort=False).append(lowering_sched, ignore_index=True, sort=False)
    event_sched = event_sched.sort_values('data_ts')
    event_sched.loc[:, 'pub_sym_id'] = event_sched['pub_sym_id'].ffill()
    event_sched.loc[:, 'event'] = 1
    if len(EQ) != 0:
        event_sched.loc[event_sched.data_ts >= min(EQ), 'EQ'] = 1
    if col_name == 'moms':
        event_sched.loc[:, 'gndmeas'] = 0
    elif col_name == 'gndmeas' and len(moms) != 0:
        event_sched.loc[:, 'moms'] = event_sched.gndmeas
        event_sched.loc[event_sched.data_ts < min(moms), 'moms'] = 0
    else:
        event_sched.loc[:, 'moms'] = 0
    #extended
    extended_sched = pd.DataFrame({'data_ts': pd.date_range(start=pd.to_datetime(validity.date())+timedelta(hours=35.5), periods=3, freq='1D')})
    extended_sched.loc[:, 'site_code'] = site_code
    extended_sched.loc[:, 'site_id'] = site_id
    extended_sched.loc[:, col_name] = 1
    extended_sched.loc[:, 'extended'] = 1
    extended_sched.loc[:, 'pub_sym_id'] = 1
    event_sched = event_sched.append(extended_sched, ignore_index=True, sort=False)
    return event_sched

def release_sched(start, end, mysql=True, to_csv=False):
    if mysql:
        sites = get_site_names()
        site_no_markers = get_sites_no_markers(mysql=mysql, to_csv=to_csv)
        
        #routine ewi sched
        routine_sched = get_routine_sched(mysql=mysql, to_csv=to_csv)
        routine_ewi_sched = get_routine_dates(start, end)
        if len(routine_ewi_sched) != 0:
            routine_ewi_sched.loc[:, 'set_site_code'] = routine_ewi_sched.apply(lambda row: list(routine_sched[(routine_sched[row.month] == routine_sched.season_type) & (routine_sched.iso_week_day == row.day)].site_code), axis=1)
            routine_ewi_sched = routine_ewi_sched.loc[routine_ewi_sched.set_site_code.str.len() != 0, :]
            routine_ewi_sched.loc[:, 'data_ts'] = routine_ewi_sched.data_ts.apply(lambda x: x + timedelta(hours=11.5))
        if len(routine_ewi_sched) != 0:
            ewi_sched = pd.DataFrame({'data_ts': list(np.concatenate(routine_ewi_sched.apply(lambda row: [row.data_ts] * len(row.set_site_code), axis=1).values).flat), 'site_code': list(np.concatenate(routine_ewi_sched.set_site_code.values).flat)})
            ewi_sched = pd.merge(ewi_sched, sites.loc[sites.active==1, ['site_id', 'site_code']], on='site_code')
            ewi_sched.loc[ewi_sched.site_id.isin(site_no_markers.site_id), 'moms'] = 1
            ewi_sched.loc[~ewi_sched.site_id.isin(site_no_markers.site_id), 'gndmeas'] = 1
            ewi_sched.loc[:, 'event'] = 0
            ewi_sched = ewi_sched.assign(raising='', permission='', lowering='', pub_sym_id=1)
        else:
            ewi_sched = pd.DataFrame(columns=['data_ts', 'site_id', 'site_code', 'gndmeas', 'raising', 'permission', 'lowering', 'event', 'pub_sym_id', 'moms', 'extended', 'EQ'])

        #event ewi  sched
        event_releases = get_event_releases(start, end, mysql=mysql, to_csv=to_csv)
        event_releases = event_releases.drop_duplicates(['event_id', 'pub_sym_id'])
        event_grp = event_releases.groupby('event_id', as_index=False)
        event_sched = event_grp.apply(get_event_sched, ewi_sched=ewi_sched, site_no_markers=site_no_markers).reset_index(drop=True)
        
        #all ewi sched
        ewi_sched = event_sched.append(ewi_sched, ignore_index=True, sort=False)
        ewi_sched = ewi_sched.sort_values(['site_code', 'data_ts', 'pub_sym_id', 'extended'], ascending=[True, True, False, True]).drop_duplicates(['site_id', 'data_ts'], keep='first')
        #remove routine and extended of previous shifts for sites lowered before 12NN
        lowering_sched = event_sched.loc[(event_sched.lowering == 1) & (event_sched.data_ts.dt.hour < 8), ['site_id', 'data_ts']]
        lowering_sched.loc[:, 'data_ts'] = lowering_sched['data_ts'].apply(lambda x: pd.to_datetime(x.date()) + timedelta(hours=11.5))
        lowering_sched = pd.merge(lowering_sched, ewi_sched, on=['data_ts', 'site_id'])        
        ewi_sched = ewi_sched.append(lowering_sched, ignore_index=True, sort=False)
        ewi_sched = ewi_sched.drop_duplicates(['site_id', 'data_ts'], keep=False)
        
        if 'extended' not in ewi_sched.columns:
            ewi_sched.loc[:, 'extended'] = np.nan
        if 'EQ' not in ewi_sched.columns:
            ewi_sched.loc[:, 'EQ'] = np.nan
        ewi_sched = ewi_sched.loc[(ewi_sched.data_ts >= start-timedelta(hours=0.5)) & (ewi_sched.data_ts <= end-timedelta(hours=0.5)), ['data_ts', 'site_id', 'site_code', 'gndmeas', 'raising', 'permission', 'lowering', 'event', 'pub_sym_id', 'moms', 'extended', 'EQ']]
        ewi_sched.loc[:, 'pub_sym_id'] = ewi_sched['pub_sym_id'].fillna(1)
        if to_csv:
            ewi_sched.to_csv(output_path+'/input_output/ewi_sched.csv', index=False)
    else:
        ewi_sched = pd.read_csv(output_path+'/input_output/ewi_sched.csv')
    return ewi_sched