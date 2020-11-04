from datetime import datetime, timedelta, time
import numpy as np
import os
import pandas as pd
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import dynadb.db as db
import volatile.memory as mem

def timeline(year, quarter):
    if quarter == 4:
        end = "{}-01-01".format(year+1)
    else:
        end = "{}-{}-01".format(year, str(3*quarter + 1).zfill(2))
    end = pd.to_datetime(end)
    start = pd.to_datetime("{}-{}-01".format(year, str(3*quarter - 2).zfill(2)))
    return start, end


def quarter_dates(start, end):
    ewi_sched = pd.DataFrame({'ts': pd.date_range(start=start, end=end,
                                                 freq='1D', closed='left')})
    ewi_sched.loc[:, 'ts'] = pd.to_datetime(ewi_sched.loc[:, 'ts'])
    ewi_sched.loc[:, 'day'] = ewi_sched.ts.apply(lambda x: x.weekday()+1)
    ewi_sched.loc[:, 'month'] = ewi_sched.ts.apply(lambda x: x.strftime('%B').lower())
    return ewi_sched


def round_data_ts(date_time):
    """Rounds time to HH:00 or HH:30.

    Args:
        date_time (datetime): Timestamp to be rounded off. Rounds to HH:00
        if before HH:30, else rounds to HH:30.

    Returns:
        datetime: Timestamp with time rounded off to HH:00 or HH:30.

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

    time_hour = int(date_time.strftime('%H'))

    quotient = int(time_hour / 4)

    if quotient == 5:
        date_time = datetime.combine(date_time.date()+timedelta(1), time(0,0))
    else:
        date_time = datetime.combine(date_time.date(), time((quotient+1)*4,0))
            
    return date_time


def routine_sched():
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
    return df


def get_events(start, end):
    conn = mem.get('DICT_DB_CONNECTIONS')
    query =  "select site_code, event_id, validity, pub_sym_id, data_ts ts_start, release_time "
    query += "from {common}.sites "
    query += "inner join {website}.monitoring_events using(site_id) "
    query += "left join {website}.monitoring_event_alerts using(event_id) "
    query += "left join "
    query += "	(SELECT * FROM {website}.monitoring_releases "
    query += "	left join {website}.monitoring_triggers using(release_id) "
    query += "	left join {website}.internal_alert_symbols using(internal_sym_id) "
    query += "    ) as trig "
    query += "using(event_alert_id) "
    query += "where data_ts between '{start}' and '{end}' "
    query += "and pub_sym_id != 1 "
    query += "order by event_id, data_ts"
    query = query.format(start=start-timedelta(hours=0.5), end=end-timedelta(hours=0.5), common=conn['common']['schema'], website=conn['website']['schema'])
    df = db.df_read(query, resource='ops')
    df = df.drop_duplicates(['event_id', 'pub_sym_id'])
    df.loc[:, 'ts_start'] = df.ts_start.apply(lambda x: round_data_ts(pd.to_datetime(x)))
    return df
    

def ewi_sent(start, end):
    conn = mem.get('DICT_DB_CONNECTIONS')
    query =  "SELECT outbox_id, ts_written, ts_sent, site_code, org_name, "
    query += "fullname, sim_num, send_status, sms_msg FROM "
    query += "	(SELECT outbox_id, ts_written, ts_sent, sim_num, "
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
    return df


def event_releases(event, ewi_sched, list_org_name):
    validity = pd.to_datetime(max(event.validity))
    site_code = event['site_code'].values[0]
    #onset
    onset_sched = event.loc[:, ['site_code', 'ts_start']]
    onset_sched.loc[:, 'ts_end'] = onset_sched.ts_start.apply(lambda x: x + timedelta(minutes=55))
    onset_sched.loc[:, 'raising'] = 1
    #lowering
    lowering_sched = event.loc[:, ['site_code']]
    lowering_sched.loc[:, 'ts_start'] = validity
    lowering_sched.loc[:, 'ts_end'] = lowering_sched.ts_start.apply(lambda x: x + timedelta(minutes=5))
    lowering_sched.loc[:, 'lowering'] = 1
    #4H release
    event_4H_start = min(event.ts_start)
    if event_4H_start.time() in [time(3,0), time(3,30), time(7,0), time(7,30), time(11,0), time(11,30), time(15,0), time(15,30), time(19,0), time(19,30), time(23,0), time(23,30)]:
        event_4H_start += timedelta(hours=4)
    #no routine if onset at 11:00 or 11:30
    if (event_4H_start.time() == time(11,0) or event_4H_start.time() == time(11,30)) and event_4H_start.date() in set(ewi_sched.ts.apply(lambda x: x.date())):
        ewi_sched.loc[ewi_sched.ts == pd.to_datetime(event_4H_start.date()), 'set_site_code'] = ewi_sched.loc[ewi_sched.ts == pd.to_datetime(event_4H_start.date()), ['set_site_code']].apply(lambda x: x - set([site_code]))
    event_4H_start = release_time(event_4H_start)
    ts_start = pd.date_range(start=event_4H_start, end=validity-timedelta(hours=4), freq='4H')
    #no 4H release if onset 1H before supposed release
    onset_ts = set(event.loc[event.ts_start.apply(lambda x: x.time() in [time(3,0), time(3,30), time(7,0), time(7,30), time(11,0), time(11,30), time(15,0), time(15,30), time(19,0), time(19,30), time(23,0), time(23,30)]), 'ts_start'].apply(lambda x: release_time(x)))
    ts_start = sorted(set(ts_start) - onset_ts)
    #4H release
    event_sched = pd.DataFrame({'ts_start': ts_start})
    event_sched.loc[:, 'ts_end'] = event_sched.ts_start.apply(lambda x: x + timedelta(minutes=5))
    event_sched.loc[:, 'site_code'] = site_code
    event_sched = event_sched.append(onset_sched, ignore_index=True, sort=False).append(lowering_sched, ignore_index=True, sort=False)
    event_sched.loc[:, 'set_org_name'] = [set(list_org_name)] * len(event_sched)
    event_sched.loc[:, 'event'] = 1
    #extended
    extended_sched = pd.DataFrame({'ts_start': pd.date_range(start=pd.to_datetime(validity.date())+timedelta(1.5), periods=3, freq='1D')})
    extended_sched.loc[:, 'ts_end'] = extended_sched.ts_start.apply(lambda x: x + timedelta(minutes=5))
    extended_sched.loc[:, 'site_code'] = site_code
    extended_sched.loc[:, 'set_org_name'] = [set(list_org_name[0:3])] * len(extended_sched)
    event_sched = event_sched.append(extended_sched, ignore_index=True, sort=False)
    # no blgu in msl and msu
    event_sched.loc[event_sched.site_code.isin(['msl', 'msu']), 'set_org_name'] = event_sched.loc[event_sched.site_code.isin(['msl', 'msu']), 'set_org_name'].apply(lambda x: x - {'blgu'})
    #no routine if end of validity is same day before 12NN
    if validity.hour in [0, 4, 8] and validity.date() in set(ewi_sched.ts.apply(lambda x: x.date())):
        ewi_sched.loc[ewi_sched.ts == pd.to_datetime(validity.date()), 'set_site_code'] = ewi_sched.loc[ewi_sched.ts == pd.to_datetime(validity.date()), ['set_site_code']].apply(lambda x: x - set([site_code]))
    return event_sched


def ewi_releases(ewi_sched, non_plgu):
    adjusted_ewi_sched = ewi_sched.loc[ewi_sched.set_site_code.isnull(), :]
    routine = ewi_sched.loc[~ewi_sched.set_site_code.isnull(), :]
    #number of sites monitored; routine is counted as 1
    num_sites = len(ewi_sched)
    #sites under routine
    if len(routine) != 0:
        routine_sites = routine.set_site_code.values[0] - set(ewi_sched.site_code)
        adjusted_routine = pd.concat([routine]*len(routine_sites), ignore_index=True)
        adjusted_routine.loc[:, 'site_code'] = sorted(routine_sites)
        adjusted_ewi_sched = adjusted_ewi_sched.append(adjusted_routine, ignore_index=True, sort=False)
    if num_sites > 5:
        adjusted_ewi_sched.loc[(adjusted_ewi_sched.raising != 1) & (adjusted_ewi_sched.lowering != 1), 'ts_end'] += timedelta(minutes=num_sites-5)
    adjusted_ewi_sched.loc[(adjusted_ewi_sched.lowering == 1), 'ts_end'] += timedelta(minutes=15)
    return adjusted_ewi_sched


def actual_releases(ewi_sched, sent):
    site_code = ewi_sched['site_code'].values[0]
    set_org_name = ewi_sched['set_org_name'].values[0]
    # bar has no ewi recipient for blgu
    if site_code == 'bar':
        set_org_name -= {'blgu'}
    ts_start = ewi_sched['ts_start'].values[0]
    ts_end = pd.to_datetime(ewi_sched['ts_end'].values[0])
    temp_sent = sent.loc[(sent.ts_written >= ts_start) & (sent.ts_written <= ts_end+timedelta(minutes=100)) & (sent.site_code == site_code), :]
    ewi_sched.loc[:, 'unsent'] = [set_org_name - set(temp_sent.org_name)]
    if len(temp_sent) != 0:
        ewi_sched.loc[:, 'sent'] = min(temp_sent.ts_written)
    return ewi_sched


def ewi_stats(df, stat, quarter=False):
    sent = 100 * (1 - sum(df.tot_unsent)/sum(df.min_recipient))
    queud = 100 * (1 - len(df.loc[df.sent.isnull(), :]) / len(df))
    ts = pd.to_datetime(df.ts_start.values[0])
    if quarter:
        ts = '{year} Q{quarter}'.format(quarter=int(np.ceil(ts.month/3)), year=ts.year)
    else:
        ts = ts.strftime('%B %Y')
    temp = pd.DataFrame({'ts': [ts], 'sent': [sent], 'queud': [queud]})
    stat = stat.append(temp, ignore_index=True)
    return stat


def releases(start, end):
    list_org_name = ['lewc', 'blgu', 'mlgu', 'plgu']
    
    routine = routine_sched()
    ewi_sched = quarter_dates(start, end)
    ewi_sched.loc[:, 'set_site_code'] = ewi_sched.apply(lambda row: set(routine[(routine[row.month] == routine.season_type) & (routine.iso_week_day == row.day)].site_code), axis=1)
    ewi_sched = ewi_sched.loc[~(ewi_sched.set_site_code == set()), :]
    ewi_sched.loc[:, 'set_org_name'] = len(ewi_sched) * [set(list_org_name[0:3])]
    ewi_sched.loc[:, 'ts_start'] = ewi_sched.ts.apply(lambda x: x + timedelta(0.5))
    ewi_sched.loc[:, 'ts_end'] = ewi_sched.ts_start.apply(lambda x: x + timedelta(minutes=5))
    event = get_events(start, end)
    event_grp = event.groupby('event_id', as_index=False)
    event_sched = event_grp.apply(event_releases, ewi_sched=ewi_sched, 
                                  list_org_name=list_org_name).reset_index(drop=True)
    ewi_sched = ewi_sched.append(event_sched, ignore_index=True, sort=False)
    ewi_sched = ewi_sched.sort_values('set_org_name').drop_duplicates(['ts_start', 'site_code'], keep='last')
    ewi_sched_grp = ewi_sched.groupby('ts_start', as_index=False)
    non_plgu = set(list_org_name[0:3])
    all_ewi_sched = ewi_sched_grp.apply(ewi_releases, non_plgu=non_plgu).reset_index(drop=True)
    all_ewi_sched = all_ewi_sched.loc[:, ['ts_start', 'ts_end', 'set_org_name', 'site_code', 'raising', 'event']]
    all_ewi_sched = all_ewi_sched.loc[all_ewi_sched.site_code != 'umi', :]
    all_ewi_sched = all_ewi_sched.loc[(all_ewi_sched.ts_start >= start) & (all_ewi_sched.ts_start < end), :]
    
    return all_ewi_sched


def main(start, end):
    all_ewi_sched = releases(start, end)
    
    sent = ewi_sent(start, end)
    sent.loc[: ,'ts_written'] = sent.ts_written.apply(lambda x: pd.to_datetime(x))
    ewi_sched_grp = all_ewi_sched.reset_index().groupby('index', as_index=False)
    all_ewi_sched = ewi_sched_grp.apply(actual_releases, sent=sent).reset_index(drop=True)
    all_ewi_sched.loc[:, 'min_recipient'] = all_ewi_sched.set_org_name.apply(lambda x: len(x))
    all_ewi_sched.loc[~(all_ewi_sched.ts_end >= all_ewi_sched.sent), 'tot_unsent'] = all_ewi_sched.loc[~(all_ewi_sched.ts_end >= all_ewi_sched.sent), 'min_recipient']
    all_ewi_sched.loc[(all_ewi_sched.ts_end >= all_ewi_sched.sent), 'tot_unsent'] = all_ewi_sched.loc[(all_ewi_sched.ts_end >= all_ewi_sched.sent), 'unsent'].apply(lambda x: len(x))
    all_ewi_sched.loc[:, 'tot_unsent'] = all_ewi_sched.tot_unsent.fillna(0)
    
    stat = pd.DataFrame()
    all_ewi_sched.loc[:, 'month'] = all_ewi_sched.ts_start.dt.month
    stat = all_ewi_sched.groupby('month', as_index=False).apply(ewi_stats, stat=stat).reset_index(drop=True)
    all_ewi_sched.loc[:, 'quarter'] = np.ceil(all_ewi_sched.month/3)
    stat = all_ewi_sched.groupby('quarter', as_index=False).apply(ewi_stats, stat=stat, quarter=True).reset_index(drop=True)

    return stat

###############################################################################
if __name__ == "__main__":
    run_start = datetime.now()
    
    start = pd.to_datetime('2020-07-01')
    end = pd.to_datetime('2020-10-01')
    stat = main(start, end)
        
    runtime = datetime.now() - run_start
    print("runtime = {}".format(runtime))