from datetime import timedelta
import numpy as np
import pandas as pd


def check_sent(release, sent):
    data_ts = pd.to_datetime(release['data_ts'].values[0])
    release_sent = sent.loc[(sent.ts_written >= data_ts) & (sent.ts_written <= data_ts+timedelta(hours=4)), :]
    release.loc[:, 'ts_written'] = pd.to_datetime(release.apply(lambda row: release_sent.loc[(release_sent.sms_msg.str.contains(row['name'])) & (release_sent.mobile_id==row['mobile_id']), 'ts_written'], axis=1).min(axis=1))
    release.loc[:, 'ts_sent'] = pd.to_datetime(release.apply(lambda row: release_sent.loc[(release_sent.sms_msg.str.contains(row['name'])) & (release_sent.mobile_id==row['mobile_id']), 'ts_sent'], axis=1).min(axis=1))
    release.loc[:, 'written_mm'] = release.apply(lambda row: int((row['mm_values'] == 1) & (len(release_sent.loc[(release_sent.sms_msg.str.contains(row['name'])) & (release_sent.mobile_id==row['mobile_id']) & (release_sent.sms_msg.str.contains(r'(?=.*mm)(?=.*threshold)',regex=True)), :]) != 0)), axis=1)
    release.loc[:, 'written_percent'] = release.apply(lambda row: int((row['mm_values'] == 1) & (len(release_sent.loc[(release_sent.sms_msg.str.contains(row['name'])) & (release_sent.mobile_id==row['mobile_id']) & (release_sent.sms_msg.str.contains('%')), :]) != 0)), axis=1)
    release.loc[(release.mm_values == 1) & (release.written_mm == 0), 'unwritten_info'] = 'mm '
    release.loc[(release.percentage == 1) & (release.written_percent == 0), 'unwritten_info'] += 'percentage'
    release.loc[~release.unwritten_info.isnull(), 'unwritten_info'] = release.loc[~release.unwritten_info.isnull(), 'unwritten_info'].apply(lambda x: '(' + x.strip().replace(' ', ' and ') + ')')
    return release

def check_tagged(release, sent):
    data_ts = pd.to_datetime(release['data_ts'].values[0])
    name = release['name'].values[0]
    mobile_id = set(release['mobile_id'])
    release_sent = sent.loc[(sent.ts_written >= data_ts) & (sent.ts_written <= data_ts+timedelta(hours=4)), :]
    if any(release.site_id == release.rain_site_id):
        tagged = 21 in release_sent.loc[(release_sent.sms_msg.str.contains(name)) & (release_sent.mobile_id.isin(mobile_id)), 'tag_id'].values
        if not tagged:
            release.loc[release.site_id == release.rain_site_id, 'tagged'] = '- no tag'
    return release

def ewi_sched(sched, recipients, sent, site_names):
    sched = sched.loc[sched.event == 1, :]
    sched = pd.merge(sched, site_names.loc[:, ['site_id', 'province']], on='site_id')
    
    sched = pd.merge(sched, recipients, on='province')
    sched = sched.loc[sched.data_ts >= sched.date_start, :]
    
    if len(sched) != 0:
        sched = sched.loc[sched.site_id==sched.rain_site_id, :].append(sched.loc[sched.all_sites==1, :], ignore_index=True, sort=False)
        sched = sched.drop_duplicates(['data_ts', 'rain_site_id', 'sim_num'])
        site_names = site_names.loc[:, ['site_id', 'name']].rename(columns={'site_id': 'rain_site_id'})
        sched = pd.merge(sched, site_names, on='rain_site_id')
    
        per_ts = sched.groupby(['data_ts'], as_index=False)
        sent_sched = per_ts.apply(check_sent, sent=sent).reset_index(drop=True)
        release_per_site = sent_sched.groupby(['rain_site_id', 'data_ts'], as_index=False)
        sent_sched = release_per_site.apply(check_tagged, sent=sent).reset_index(drop=True)
        if 'tagged' not in sent_sched.columns:
            sent_sched = sent_sched.assign(tagged=np.nan)
    else:
        sent_sched = pd.DataFrame()
    return sent_sched