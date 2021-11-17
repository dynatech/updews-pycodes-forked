from datetime import timedelta
import numpy as np
import pandas as pd


def check_sent(release, sent, queued):
    data_ts = pd.to_datetime(release['data_ts'].values[0])
    site_id = release['site_id'].values[0]
    end = data_ts + timedelta(minutes=30)
    onset = release['raising'].values[0]
    if onset != 1:
        ts_name = end.time().strftime('%I%p')
        end += timedelta(hours=4)
    release_queued = queued.loc[(queued.site_id == site_id) & (queued.timestamp >= data_ts) & (queued.timestamp <= end), :]
    if len(release_queued) != 0:
        release.loc[:, 'ts_written'] = min(release_queued)
    else:
        release.loc[:, 'ts_written'] = np.nan
    release_sent = sent.loc[(sent.site_id == site_id) & (sent.timestamp >= data_ts) & (sent.timestamp <= end), :]
    if onset == 1:
        release_sent = release_sent.loc[release_sent.narrative.str.contains(ts_name), :]
    release.loc[:, 'ts_sent'] = release.apply(lambda row: release_sent.loc[release_sent.narrative.str.contains(row.email), 'timestamp'], axis=1).min(axis=1)    
    return release
        
def ewi_sched(sched, recipients, sent_queued):
    sched = sched.loc[sched.event == 1, :]
    bulletin_sched = pd.merge(sched, recipients, how='inner', on='site_id')
    bulletin_sched = bulletin_sched.append(sched.assign(fullname='Arturo S. Daag', email='asdaag48@gmail.com'), ignore_index=True, sort=False)
    bulletin_sched = bulletin_sched.append(sched.assign(fullname='Renato U. Solidum, Jr.', email='RUS'), ignore_index=True, sort=False)
    bulletin_sched = bulletin_sched.append(sched.loc[sched.EQ == 1, :].assign(fullname='Jeffrey Perez', email='jeffrey.perez@phivolcs.dost.gov.ph'), ignore_index=True, sort=False)
    if len(bulletin_sched) != 0:
        sent = sent_queued.loc[sent_queued.narrative.str.contains('M EWI BULLETIN to '), :]
        queued = sent_queued.loc[sent_queued.narrative.str.contains('Added EWI bulletin to the sending queue'), :]
        per_release = bulletin_sched.groupby(['site_id', 'data_ts'], as_index=False)
        sent_sched = per_release.apply(check_sent, sent=sent, queued=queued).reset_index(drop=True)
        sent_sched.loc[sent_sched.ts_written.isnull(), 'ts_written'] = sent_sched.loc[sent_sched.ts_written.isnull(), 'ts_sent']
    else:
        sent_sched = pd.DataFrame()
    return sent_sched