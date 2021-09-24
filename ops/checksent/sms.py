from datetime import datetime, timedelta
import os
import pandas as pd
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import lib
import ops.ipr.ewisms_meal as ewisms
import ops.ipr.lib as iprlib


def unsent_ewisms(df):
    set_org_name = set(['lewc', 'blgu', 'mlgu', 'plgu'])
    if df.mon_type.values[0] != 'event':
        set_org_name -= set(['plgu'])
    unsent = sorted(set_org_name-set(df.org_name))
    unsent_df = pd.DataFrame()
    if len(unsent) != 0:
        unsent_df = pd.DataFrame({'site_code': [df.site_code.values[0]], 'ofc': [', '.join(unsent)]})
    return unsent_df


def main(time_now=datetime.now()):

    curr_release = iprlib.release_time(time_now) - timedelta(hours=4)
    
    start = curr_release - timedelta(3)
    end = curr_release + timedelta(hours=4)
    
    mysql = True
    
    ewi_sched = lib.get_monitored_sites(curr_release, start, end, mysql=mysql)
    
    if len(ewi_sched) != 0:
        ewisms_sent = ewisms.ewi_sent(start=curr_release, end=end, mysql=mysql)
        site_names = iprlib.get_site_names()
        ewisms_sent = pd.merge(ewisms_sent, site_names, on='site_code')
        ewisms_sent = ewisms_sent.loc[ewisms_sent.apply(lambda row: row['name'] in row['sms_msg'], axis=1), :]
        ewisms_sent = pd.merge(ewi_sched, ewisms_sent.reset_index(), how='left', on='site_code')
    
        site_ewisms = ewisms_sent.groupby('site_code', as_index=False)
        df = site_ewisms.apply(unsent_ewisms).reset_index(drop=True)
        if len(df) != 0:
            df.loc[df.site_code.isin(['lte']), 'ofc'] = df.loc[df.site_code.isin(['lte']), 'ofc'].apply(lambda x: ', '.join(sorted(set(x.split(', ')) - set(['lewc']))))
            df.loc[df.site_code.isin(['bar', 'msl', 'msu']), 'ofc'] = df.loc[df.site_code.isin(['bar', 'msl', 'msu']), 'ofc'].apply(lambda x: ', '.join(sorted(set(x.split(', ')) - set(['blgu']))))
            df = df.loc[df.ofc != '', :]
        
        lib.send_unsent_notif(df, 'EWI SMS', curr_release)


if __name__ == '__main__':
    main()