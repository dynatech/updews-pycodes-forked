from datetime import datetime, timedelta, time
import os
import pandas as pd
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import dynadb.db as db
import lib
import volatile.memory as mem


output_path = os.path.dirname(os.path.realpath(__file__))

def timeline(year, quarter):
    if quarter == 4:
        end = "{}-01-01".format(year+1)
    else:
        end = "{}-{}-01".format(year, str(3*quarter + 1).zfill(2))
    end = pd.to_datetime(end)
    start = pd.to_datetime("{}-{}-01".format(year, str(3*quarter - 2).zfill(2)))
    return start, end


def quarter_dates(start, end):
    ewi_sched = pd.DataFrame({'data_ts': pd.date_range(start=start, end=end,
                                                 freq='1D', closed='left')})
    ewi_sched.loc[:, 'data_ts'] = pd.to_datetime(ewi_sched.loc[:, 'data_ts'])
    ewi_sched.loc[:, 'day'] = ewi_sched.data_ts.apply(lambda x: x.weekday()+1)
    ewi_sched.loc[:, 'month'] = ewi_sched.data_ts.apply(lambda x: x.strftime('%B').lower())
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


def routine_sched(mysql=False):
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
        df.to_csv(output_path+'/input/routine.csv', index=False)
    else:
        df = pd.read_csv(output_path+'/input/routine.csv')
    return df


def check_eq_moms(df):
    df.loc[:, 'eq'] = int('E' in df.alert_symbol.values)
    df.loc[:, 'moms'] = int('m' in df.alert_symbol.values or 'M' in df.alert_symbol.values or any(site_code in df.site_code.values for site_code in ['msl', 'pug', 'tue', 'umi']))
    return df


def get_events(start, end, mysql=False, drop=True):
    if mysql:
        conn = mem.get('DICT_DB_CONNECTIONS')
        query =  "select site_code, event_id, validity, pub_sym_id, alert_symbol, data_ts, release_time "
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
        query += "order by event_id, data_ts"
        query = query.format(start=start, end=end, common=conn['common']['schema'], website=conn['website']['schema'])
        df = db.df_read(query, resource='ops')
        df.loc[:, 'ts_start'] = df.data_ts.apply(lambda x: round_data_ts(pd.to_datetime(x))+timedelta(hours=0.5))
        df.to_csv(output_path+'/input/event.csv', index=False)
    else:
        df = pd.read_csv(output_path+'/input/event.csv')
    df_grp = df.groupby('event_id', as_index=False)
    df = df_grp.apply(check_eq_moms).reset_index(drop=True)
    if drop:
        df = df.drop_duplicates(['event_id', 'pub_sym_id'])
    df.loc[:, 'ts_start'] = pd.to_datetime(df.ts_start)
    df.loc[:, 'data_ts'] = pd.to_datetime(df.data_ts)
    return df
    

def ewi_sent(start, end, mysql=False):
    if mysql:
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
        df.loc[:, 'sms_msg'] = df.sms_msg.str.lower().str.replace('city', '').str.replace('.', '')
        df.to_csv(output_path+'/input/sent.csv', index=False)
    else:
        df = pd.read_csv(output_path+'/input/sent.csv')
    return df


def event_releases(event, ewi_sched, list_org_name):
    
    validity = pd.to_datetime(max(event.validity))
    site_code = event['site_code'].values[0]
    moms = event['moms'].values[0]
    eq = event['eq'].values[0]
    #onset
    onset_sched = event.loc[:, ['site_code', 'data_ts', 'ts_start', 'alert_symbol', 'pub_sym_id']]
    onset_sched.loc[:, 'ts_end'] = onset_sched.ts_start.apply(lambda x: x + timedelta(minutes=55))
    onset_sched.loc[:, 'raising'] = 1
    onset_sched.loc[:, 'permission'] = onset_sched.loc[:, ['alert_symbol', 'pub_sym_id']].apply(lambda row: int(((row.alert_symbol == 'D') | (row.pub_sym_id == 4))), axis=1)
    onset_sched.loc[:, 'gndmeas'] = onset_sched.data_ts.apply(lambda x: int(((lib.release_time(x) - x).total_seconds()/3600 < 2.5) and (lib.release_time(x) in pd.to_datetime(ewi_sched.ts_start.values)) and lib.release_time(x).hour <= 16 and lib.release_time(x).hour >= 8))
    #lowering
    lowering_sched = event.loc[:, ['site_code', 'alert_symbol', 'pub_sym_id']]
    lowering_sched.loc[:, 'ts_start'] = validity
    lowering_sched.loc[:, 'data_ts'] = validity - timedelta(hours=0.5)
    lowering_sched.loc[:, 'ts_end'] = lowering_sched.ts_start.apply(lambda x: x + timedelta(minutes=10))
    lowering_sched.loc[:, 'lowering'] = 1
    lowering_sched.loc[:, 'gndmeas'] = lowering_sched.data_ts.apply(lambda x: int(lib.release_time(x).hour <= 16 and lib.release_time(x).hour >= 8))
    lowering_sched.loc[:, 'permission'] = lowering_sched.loc[:, ['alert_symbol', 'pub_sym_id']].apply(lambda row: int((row.pub_sym_id == 4)), axis=1)
    #4H release
    event_4H_start = min(event.data_ts)
    if event_4H_start.time() in [time(3,0), time(3,30), time(7,0), time(7,30), time(11,0), time(11,30), time(15,0), time(15,30), time(19,0), time(19,30), time(23,0), time(23,30)]:
        event_4H_start += timedelta(hours=4)
    #no routine if onset at 11:00 or 11:30
    if (event_4H_start.time() == time(11,0) or event_4H_start.time() == time(11,30)) and event_4H_start.date() in set(ewi_sched.data_ts.apply(lambda x: x.date())):
        ewi_sched.loc[ewi_sched.data_ts == pd.to_datetime(event_4H_start.date())+timedelta(hours=11.5), 'set_site_code'] = ewi_sched.loc[ewi_sched.data_ts == pd.to_datetime(event_4H_start.date())+timedelta(hours=11.5), ['set_site_code']].apply(lambda x: x - set([site_code]))
    event_4H_start = lib.release_time(event_4H_start)
    ts_start = pd.date_range(start=event_4H_start, end=validity-timedelta(hours=4), freq='4H')
    #no 4H release if onset 1H before supposed release
    onset_ts = set(event.loc[event.data_ts.apply(lambda x: x.time() in [time(3,0), time(3,30), time(7,0), time(7,30), time(11,0), time(11,30), time(15,0), time(15,30), time(19,0), time(19,30), time(23,0), time(23,30)]), 'ts_start'].apply(lambda x: lib.release_time(x)))
    ts_start = sorted(set(ts_start) - onset_ts)
    #4H release
    event_sched = pd.DataFrame({'ts_start': ts_start})
    event_sched.loc[:, 'data_ts'] = event_sched.loc[:, 'ts_start'].apply(lambda x: x - timedelta(minutes=30))
    event_sched.loc[:, 'ts_end'] = event_sched.ts_start.apply(lambda x: x + timedelta(minutes=10))
    event_sched.loc[:, 'site_code'] = site_code
    event_sched.loc[:, 'gndmeas'] = event_sched.data_ts.apply(lambda x: int(lib.release_time(x).hour <= 16 and lib.release_time(x).hour >= 8))
    event_sched = event_sched.append(onset_sched, ignore_index=True, sort=False).append(lowering_sched, ignore_index=True, sort=False)
    event_sched.loc[:, 'set_org_name'] = [set(list_org_name)] * len(event_sched)
    event_sched.loc[:, 'event'] = 1
    #no gndmeas next release time after onset
    next_release_ts = onset_sched.data_ts.apply(lambda x: lib.release_time(x))
    event_sched.loc[event_sched.ts_start.isin(next_release_ts), 'gndmeas'] = 0
    #extended
    extended_sched = pd.DataFrame({'ts_start': pd.date_range(start=pd.to_datetime(validity.date())+timedelta(1.5), periods=3, freq='1D')})
    extended_sched.loc[:, 'data_ts'] = extended_sched.ts_start.apply(lambda x: x - timedelta(minutes=30))
    extended_sched.loc[:, 'ts_end'] = extended_sched.ts_start.apply(lambda x: x + timedelta(minutes=10))
    extended_sched.loc[:, 'site_code'] = site_code
    extended_sched.loc[:, 'set_org_name'] = [set(list_org_name[0:3])] * len(extended_sched)
    extended_sched.loc[:, 'gndmeas'] = 1
    event_sched = event_sched.append(extended_sched, ignore_index=True, sort=False)
    #no routine if end of validity is same day before 12NN
    if validity.hour in [0, 4, 8] and validity.date() in set(ewi_sched.ts_start.apply(lambda x: x.date())):
        ewi_sched.loc[ewi_sched.data_ts == pd.to_datetime(validity.date())+timedelta(hours=11.5), 'set_site_code'] = ewi_sched.loc[ewi_sched.data_ts == pd.to_datetime(validity.date())+timedelta(hours=11.5), ['set_site_code']].apply(lambda x: x - set([site_code]))
    event_sched.loc[:, 'moms'] = moms
    event_sched.loc[:, 'eq'] = eq
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


def actual_releases(ewi_sched, sent, site_names):
    site_code = ewi_sched['site_code'].values[0]
    site_name = site_names.loc[site_names.site_code == site_code, 'name'].values[0]
    set_org_name = ewi_sched['set_org_name'].values[0]
    ts_start = pd.to_datetime(ewi_sched['ts_start'].values[0])
    ts_end = pd.to_datetime(ewi_sched['ts_end'].values[0])
    temp_queued = sent.loc[(sent.ts_written >= ts_start-timedelta(minutes=30)) & (sent.ts_written <= ts_end+timedelta(minutes=100)) & (sent.site_code == site_code) & (sent.sms_msg.str.contains(site_name)), :]
    ewi_sched.loc[:, 'unsent'] = [set_org_name - set(temp_queued.org_name)]
    if len(temp_queued) != 0:
        ewi_sched.loc[:, 'queued'] = min(temp_queued.ts_written)
        ewi_sched.loc[:, 'actual_sent'] = min(temp_queued.ts_sent)
    return ewi_sched


def releases(start, end, mysql):
    list_org_name = ['lewc', 'blgu', 'mlgu', 'plgu']
    routine = routine_sched(mysql=mysql)
    ewi_sched = quarter_dates(start, end)
    ewi_sched.loc[:, 'set_site_code'] = ewi_sched.apply(lambda row: set(routine[(routine[row.month] == routine.season_type) & (routine.iso_week_day == row.day)].site_code), axis=1)
    ewi_sched = ewi_sched.loc[~(ewi_sched.set_site_code == set()), :]
    ewi_sched.loc[:, 'set_org_name'] = len(ewi_sched) * [set(list_org_name[0:3])]
    ewi_sched.loc[:, 'data_ts'] = ewi_sched.data_ts.apply(lambda x: x + timedelta(hours=11.5))
    ewi_sched.loc[:, 'ts_start'] = ewi_sched.data_ts.apply(lambda x: x + timedelta(hours=0.5))
    ewi_sched.loc[:, 'ts_end'] = ewi_sched.ts_start.apply(lambda x: x + timedelta(minutes=10))
    ewi_sched.loc[:, 'gndmeas'] = 1
    event = get_events(start, end, mysql=mysql)
    event_grp = event.groupby('event_id', as_index=False)
    event_sched = event_grp.apply(event_releases, ewi_sched=ewi_sched, 
                                  list_org_name=list_org_name).reset_index(drop=True)
    ewi_sched = ewi_sched.append(event_sched, ignore_index=True, sort=False)
    ewi_sched = ewi_sched.sort_values('set_org_name').drop_duplicates(['ts_start', 'site_code'], keep='last')
    ewi_sched_grp = ewi_sched.groupby('ts_start', as_index=False)
    non_plgu = set(list_org_name[0:3])
    all_ewi_sched = ewi_sched_grp.apply(ewi_releases, non_plgu=non_plgu).reset_index(drop=True)
    all_ewi_sched = all_ewi_sched.loc[:, ['data_ts', 'ts_start', 'ts_end', 'set_org_name', 'site_code', 'raising', 'event', 'lowering', 'permission', 'eq', 'moms', 'gndmeas']]
    all_ewi_sched = all_ewi_sched.loc[~all_ewi_sched.site_code.isin(['phi', 'umi']), :]
    all_ewi_sched = all_ewi_sched.loc[(all_ewi_sched.ts_start >= start) & (all_ewi_sched.ts_start < end), :]

    return all_ewi_sched

def main(year='', quarter='', start='', end='', mysql=False, write_csv=True):
    if start == '' and end == '':
        start, end = timeline(year, quarter)
    
    site_names = lib.get_site_names()
    all_ewi_sched = releases(start, end, mysql)
    
    sent = ewi_sent(start, end, mysql=mysql)
    sent.loc[: ,'ts_written'] = sent.ts_written.apply(lambda x: pd.to_datetime(x))
    # no blgu in bar, msl and msu
    all_ewi_sched.loc[all_ewi_sched.site_code.isin(['bar', 'msl', 'msu']), 'set_org_name'] = all_ewi_sched.loc[all_ewi_sched.site_code.isin(['bar', 'msl', 'msu']), 'set_org_name'].apply(lambda x: x - {'blgu'})
    all_ewi_sched.loc[all_ewi_sched.site_code.isin(['lte', 'msl', 'msu']), 'set_org_name'] = all_ewi_sched.loc[all_ewi_sched.site_code.isin(['lte', 'msl', 'msu']), 'set_org_name'].apply(lambda x: x - {'lewc'})
    ewi_sched_grp = all_ewi_sched.reset_index().groupby('index', as_index=False)
    all_ewi_sched = ewi_sched_grp.apply(actual_releases, sent=sent, site_names=site_names).reset_index(drop=True)

    all_ewi_sched.loc[:, 'min_recipient'] = all_ewi_sched.set_org_name.apply(lambda x: len(x))
    all_ewi_sched.loc[~(all_ewi_sched.ts_end >= all_ewi_sched.queued)|(all_ewi_sched.queued.isnull()), 'tot_unsent'] = all_ewi_sched.loc[~(all_ewi_sched.ts_end >= all_ewi_sched.queued), 'min_recipient']
    all_ewi_sched.loc[(all_ewi_sched.ts_end >= all_ewi_sched.queued), 'tot_unsent'] = all_ewi_sched.loc[(all_ewi_sched.ts_end >= all_ewi_sched.queued), 'unsent'].apply(lambda x: len(x))
    all_ewi_sched.loc[:, 'tot_unsent'] = all_ewi_sched.tot_unsent.fillna(0)

    if write_csv:
        all_ewi_sched.to_csv(output_path+'/output/sending_status.csv', index=False)
    
    print("{}% ewi queud for sending".format(100 * (1 - sum(all_ewi_sched.tot_unsent)/sum(all_ewi_sched.min_recipient))))

    return all_ewi_sched

###############################################################################
if __name__ == "__main__":
    run_start = datetime.now()
    
    start = pd.to_datetime('2020-12-01')
    end = pd.to_datetime('2021-06-01 08:00')
    all_ewi_sched = main(start=start, end=end, mysql=False)
        
    runtime = datetime.now() - run_start
    print("runtime = {}".format(runtime))